from __future__ import annotations

import math
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlencode, urlparse, urlunparse

from openpyxl import Workbook

from utils.json_utils import load_json, save_json
from utils.time_utils import elapsed_seconds, utc_now_iso


class TagExtractionManager:
    def __init__(self, root: Path, browser_manager, woo_api_manager):
        self.root = root
        self.browser_manager = browser_manager
        self.woo_api_manager = woo_api_manager
        self.index_path = root / 'data' / 'tag_extractions_index.json'
        self.extractions_root = root / 'data' / 'tag_extractions'
        self.files_root = root / 'data' / 'tag_extraction_files'
        self.extractions_root.mkdir(parents=True, exist_ok=True)
        self.files_root.mkdir(parents=True, exist_ok=True)
        save_json(self.index_path, load_json(self.index_path, []))

    def extraction_path(self, extraction_id: str) -> Path:
        return self.extractions_root / f'extraction_{extraction_id}.json'

    def list_extractions(self) -> List[Dict[str, Any]]:
        items = load_json(self.index_path, [])
        enriched: List[Dict[str, Any]] = []
        for item in items:
            extraction_id = item.get('extraction_id')
            if not extraction_id:
                continue
            full = load_json(self.extraction_path(extraction_id), {})
            if full:
                enriched.append(full)
            else:
                enriched.append(item)
        return enriched

    def load_extraction(self, extraction_id: str) -> Dict[str, Any]:
        return load_json(self.extraction_path(extraction_id), {})

    def save_extraction(self, extraction: Dict[str, Any]) -> None:
        save_json(self.extraction_path(extraction['extraction_id']), extraction)
        items = [x for x in load_json(self.index_path, []) if x.get('extraction_id') != extraction['extraction_id']]
        summary = {
            'extraction_id': extraction['extraction_id'],
            'status': extraction.get('status'),
            'website_label': extraction.get('website_label') or extraction.get('base_site_url') or '-',
            'category_url': extraction.get('category_url'),
            'mode': extraction.get('mode'),
            'tags_found': extraction.get('tags_found', 0),
            'created_at': extraction.get('created_at'),
            'started_at': extraction.get('started_at'),
            'ended_at': extraction.get('ended_at'),
            'file_name': extraction.get('file_name'),
            'download_path': extraction.get('download_path'),
            'progress_message': extraction.get('progress_message', ''),
        }
        items.insert(0, summary)
        save_json(self.index_path, items[:200])

    def new_extraction(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        extraction = {
            'extraction_id': uuid.uuid4().hex[:12],
            'status': 'queued',
            'created_at': utc_now_iso(),
            'started_at': None,
            'ended_at': None,
            'duration_seconds': 0,
            'website_label': str(payload.get('website_label') or payload.get('base_site_url') or '').strip(),
            'base_site_url': str(payload.get('base_site_url') or '').strip().rstrip('/'),
            'login_url': str(payload.get('login_url') or '').strip(),
            'username': str(payload.get('username') or '').strip(),
            'password': str(payload.get('password') or ''),
            'category_url': str(payload.get('category_url') or '').strip(),
            'use_api': bool(payload.get('use_api')),
            'api_consumer_key': str(payload.get('api_consumer_key') or '').strip(),
            'api_consumer_secret': str(payload.get('api_consumer_secret') or '').strip(),
            'api_base_url': str(payload.get('api_base_url') or '').strip(),
            'api_per_page': int(payload.get('api_per_page') or 100),
            'request_timeout_seconds': int(payload.get('request_timeout_seconds') or 30),
            'mode': 'api' if payload.get('use_api') else 'browser',
            'tags_found': 0,
            'processed_pages': 0,
            'estimated_pages': None,
            'progress_message': 'Queued',
            'logs': [],
            'rows': [],
            'file_name': None,
            'file_path': None,
            'download_path': None,
            'last_error': None,
        }
        self.save_extraction(extraction)
        return extraction

    def append_log(self, extraction: Dict[str, Any], level: str, message: str, **details: Any) -> None:
        extraction.setdefault('logs', []).append({
            'ts': utc_now_iso(),
            'level': level,
            'message': message,
            'details': details,
        })

    def _build_admin_tag_url(self, base_site_url: str, slug: str) -> str:
        return f"{base_site_url.rstrip('/')}/wp-admin/edit.php?product_tag={slug}&post_type=product"

    def _api_get(self, base_api_url: str, consumer_key: str, consumer_secret: str, path: str, params: Optional[Dict[str, Any]] = None, timeout: int = 30):
        import requests
        url = f"{base_api_url.rstrip('/')}/{path.lstrip('/')}"
        merged = {'consumer_key': consumer_key, 'consumer_secret': consumer_secret}
        if params:
            merged.update(params)
        resp = requests.get(url, params=merged, timeout=float(timeout or 30))
        resp.raise_for_status()
        return resp.json(), resp.headers

    def extract_via_api(self, extraction: Dict[str, Any]) -> List[Dict[str, Any]]:
        base_site_url = extraction['base_site_url']
        consumer_key = extraction['api_consumer_key']
        consumer_secret = extraction['api_consumer_secret']
        base_api_url = extraction['api_base_url'].rstrip('/') if extraction.get('api_base_url') else f"{base_site_url.rstrip('/')}/wp-json/wc/v3"
        per_page = max(1, min(100, int(extraction.get('api_per_page') or 100)))
        timeout = int(extraction.get('request_timeout_seconds') or 30)
        rows: List[Dict[str, Any]] = []
        page = 1
        total_pages = None
        while True:
            extraction['processed_pages'] = page - 1
            extraction['progress_message'] = f'Fetching tag page {page} through WooCommerce API...'
            self.save_extraction(extraction)
            payload, headers = self._api_get(base_api_url, consumer_key, consumer_secret, '/products/tags', {'page': page, 'per_page': per_page, 'orderby': 'name', 'order': 'asc'}, timeout=timeout)
            if total_pages is None:
                raw_total_pages = headers.get('X-WP-TotalPages') or headers.get('x-wp-totalpages')
                try:
                    total_pages = int(raw_total_pages) if raw_total_pages else None
                except Exception:
                    total_pages = None
                extraction['estimated_pages'] = total_pages
            if not payload:
                break
            for item in payload:
                title = str(item.get('name') or '').strip()
                slug = str(item.get('slug') or '').strip()
                if not title or not slug:
                    continue
                admin_url = self._build_admin_tag_url(base_site_url, slug)
                count_value = int(item.get('count') or 0)
                rows.append({
                    'title': title,
                    'url': admin_url,
                    'count': count_value,
                    'line': f'{title} | {admin_url}',
                    'slug': slug,
                })
            extraction['tags_found'] = len(rows)
            extraction['processed_pages'] = page
            extraction['progress_message'] = f'Fetched {len(rows)} tags from {page}{f"/{total_pages}" if total_pages else ""} API pages.'
            self.save_extraction(extraction)
            if total_pages and page >= total_pages:
                break
            if len(payload) < per_page:
                break
            page += 1
        return rows

    def _category_page_url(self, url: str, page_number: int) -> str:
        parsed = urlparse(url)
        qs = parse_qs(parsed.query)
        qs['paged'] = [str(page_number)]
        new_query = urlencode({k: v[0] if isinstance(v, list) and len(v) == 1 else v for k, v in qs.items()}, doseq=True)
        return urlunparse(parsed._replace(query=new_query))

    def _extract_rows_from_listing_page(self, page) -> List[Dict[str, Any]]:
        return page.evaluate(
            """
            () => {
              const rows = [];
              const tableRows = Array.from(document.querySelectorAll('#the-list tr'));
              for (const tr of tableRows) {
                const titleLink = tr.querySelector('td.name.column-name .row-title, td.name .row-title, .column-name .row-title, a.row-title');
                if (!titleLink) continue;
                const title = (titleLink.textContent || '').trim();
                const href = titleLink.href || titleLink.getAttribute('href') || '';
                const countCell = tr.querySelector('td.count, .column-count');
                let rawCount = '';
                if (countCell) rawCount = (countCell.textContent || '').trim();
                const count = parseInt((rawCount.match(/\d+/) || ['0'])[0], 10) || 0;
                if (!title || !href) continue;
                rows.push({ title, url: href, count });
              }
              return rows;
            }
            """
        ) or []

    def extract_via_browser(self, extraction: Dict[str, Any]) -> List[Dict[str, Any]]:
        self.browser_manager.ensure_browser()
        # Temporary login override for extraction-only browsing.
        original_site = dict(self.browser_manager.config.get('site', {}))
        original_creds = dict(self.browser_manager.config.get('credentials', {}))
        self.browser_manager.config.setdefault('site', {})['login_url'] = extraction['login_url']
        self.browser_manager.config.setdefault('site', {})['base_url'] = extraction['base_site_url']
        self.browser_manager.config.setdefault('credentials', {})['username'] = extraction['username']
        self.browser_manager.config.setdefault('credentials', {})['password'] = extraction['password']
        self.browser_manager.logged_in = False
        self.browser_manager.login_once()
        rows: List[Dict[str, Any]] = []
        seen = set()
        page_number = 1
        current_url = extraction['category_url']
        try:
            while current_url:
                extraction['progress_message'] = f'Opening tag category page {page_number} in browser...'
                self.save_extraction(extraction)
                page = self.browser_manager.new_page(current_url)
                try:
                    timeout = int(extraction.get('request_timeout_seconds') or 30) * 1000
                    page.wait_for_load_state('domcontentloaded', timeout=timeout)
                    page.wait_for_selector('#the-list', timeout=timeout)
                    page_rows = self._extract_rows_from_listing_page(page)
                    for item in page_rows:
                        key = (item.get('title'), item.get('url'))
                        if key in seen:
                            continue
                        seen.add(key)
                        rows.append({
                            'title': item['title'],
                            'url': item['url'],
                            'count': int(item.get('count') or 0),
                            'line': f"{item['title']} | {item['url']}",
                        })
                    extraction['tags_found'] = len(rows)
                    extraction['processed_pages'] = page_number
                    extraction['progress_message'] = f'Scraped {len(rows)} tags from {page_number} admin pages.'
                    self.save_extraction(extraction)
                    next_locator = page.locator('.tablenav-pages a.next-page, .tablenav-pages a.next').first
                    next_href = next_locator.evaluate('(el) => el.href') if next_locator.count() else None
                    current_url = next_href
                finally:
                    try:
                        page.close()
                    except Exception:
                        pass
                if not current_url:
                    break
                page_number += 1
        finally:
            self.browser_manager.config['site'] = original_site
            self.browser_manager.config['credentials'] = original_creds
            self.browser_manager.logged_in = False
        return rows

    def build_spreadsheet(self, extraction: Dict[str, Any], rows: List[Dict[str, Any]]) -> str:
        wb = Workbook()
        ws = wb.active
        ws.title = 'Tags'
        ws.append(['Tag Title | Tag URL', 'Count'])
        for item in rows:
            ws.append([item['line'], int(item.get('count') or 0)])
        ws.column_dimensions['A'].width = 90
        ws.column_dimensions['B'].width = 14
        file_name = f"tag_data_{extraction['extraction_id']}.xlsx"
        file_path = self.files_root / file_name
        wb.save(file_path)
        return file_name

    def execute_extraction(self, extraction: Dict[str, Any]) -> Dict[str, Any]:
        extraction = dict(extraction)
        extraction['status'] = 'running'
        extraction['started_at'] = utc_now_iso()
        extraction['progress_message'] = 'Starting extraction...'
        self.append_log(extraction, 'info', 'Extraction started', mode='api' if extraction.get('use_api') else 'browser')
        self.save_extraction(extraction)
        try:
            if extraction.get('use_api'):
                rows = self.extract_via_api(extraction)
                extraction['mode'] = 'api'
            else:
                rows = self.extract_via_browser(extraction)
                extraction['mode'] = 'browser'
            rows = sorted(rows, key=lambda item: (str(item.get('title') or '').lower(), str(item.get('url') or '').lower()))
            extraction['rows'] = rows
            extraction['tags_found'] = len(rows)
            extraction['progress_message'] = f'Building spreadsheet for {len(rows)} extracted tags...'
            self.save_extraction(extraction)
            file_name = self.build_spreadsheet(extraction, rows)
            extraction['file_name'] = file_name
            extraction['file_path'] = str(self.files_root / file_name)
            extraction['download_path'] = f"/api/extractions/{extraction['extraction_id']}/download"
            extraction['status'] = 'completed'
            extraction['ended_at'] = utc_now_iso()
            extraction['duration_seconds'] = elapsed_seconds(extraction.get('started_at'), extraction.get('ended_at'))
            extraction['progress_message'] = f'Completed. {len(rows)} tags extracted.'
            self.append_log(extraction, 'info', 'Extraction completed', tags_found=len(rows), file_name=file_name)
            self.save_extraction(extraction)
            return extraction
        except Exception as exc:
            extraction['status'] = 'failed'
            extraction['ended_at'] = utc_now_iso()
            extraction['duration_seconds'] = elapsed_seconds(extraction.get('started_at'), extraction.get('ended_at'))
            extraction['last_error'] = str(exc)
            extraction['progress_message'] = f'Extraction failed: {exc}'
            self.append_log(extraction, 'error', 'Extraction failed', error=str(exc))
            self.save_extraction(extraction)
            return extraction
