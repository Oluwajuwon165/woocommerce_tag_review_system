from __future__ import annotations

from typing import Any, Dict, List, Optional
from urllib.parse import parse_qs, urlparse

import requests


class WooCommerceAPIManager:
    def __init__(self, config: Dict[str, Any]):
        self.config = config

    def refresh_config(self, config: Dict[str, Any]) -> None:
        self.config = config

    @property
    def settings(self) -> Dict[str, Any]:
        return self.config.setdefault('woo_api', {})

    def is_enabled(self) -> bool:
        s = self.settings
        return bool(s.get('enabled') and s.get('consumer_key') and s.get('consumer_secret'))

    def _auth_params(self) -> Dict[str, str]:
        s = self.settings
        return {
            'consumer_key': s.get('consumer_key', ''),
            'consumer_secret': s.get('consumer_secret', ''),
        }

    def _base_api_url(self) -> str:
        base = (self.config.get('site', {}).get('base_url') or '').rstrip('/')
        custom = (self.settings.get('base_api_url') or '').strip().rstrip('/')
        return custom or f"{base}/wp-json/wc/v3"

    def _get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self._base_api_url()}/{path.lstrip('/')}"
        merged = dict(self._auth_params())
        if params:
            merged.update(params)
        timeout = float(self.settings.get('request_timeout_seconds') or 30)
        resp = requests.get(url, params=merged, timeout=timeout)
        resp.raise_for_status()
        return resp.json()

    def extract_tag_slug(self, tag_url: str) -> str:
        parsed = urlparse(tag_url)
        qs = parse_qs(parsed.query)
        slug = (qs.get('product_tag') or qs.get('tag') or [''])[0]
        return slug.strip()

    def resolve_tag(self, tag_url: str, tag_title: str) -> Dict[str, Any]:
        slug = self.extract_tag_slug(tag_url)
        if not slug and tag_title:
            slug = tag_title.strip().lower().replace(' ', '-')
        if not slug:
            raise RuntimeError('Could not determine product tag slug from tag URL for Woo API mode.')
        payload = self._get('/products/tags', {'search': slug, 'per_page': 100})
        payload = payload or []
        exact = None
        for item in payload:
            if str(item.get('slug') or '').strip().lower() == slug.lower():
                exact = item
                break
        if not exact:
            title_lower = tag_title.strip().lower()
            for item in payload:
                if str(item.get('name') or '').strip().lower() == title_lower:
                    exact = item
                    break
        if not exact:
            raise RuntimeError(f"Woo API could not resolve tag '{tag_title}' (slug '{slug}').")
        return {'id': exact.get('id'), 'slug': exact.get('slug') or slug, 'name': exact.get('name') or tag_title}


    def fetch_products_for_tag_pages(self, tag_id: int, start_page: int = 1, max_pages: Optional[int] = None, per_page: Optional[int] = None) -> List[Dict[str, Any]]:
        page_size = min(100, int(per_page or self.settings.get('per_page') or 100))
        page = max(1, int(start_page or 1))
        pages: List[Dict[str, Any]] = []
        collected = 0
        while True:
            payload = self._get('/products', {
                'tag': int(tag_id),
                'page': page,
                'per_page': page_size,
                '_fields': 'id,name',
            })
            if not payload:
                break
            products = []
            for item in payload:
                products.append({
                    'productId': str(item.get('id')),
                    'productTitle': str(item.get('name') or '').strip(),
                })
            pages.append({
                'page_number': page,
                'products': products,
                'page_size': page_size,
            })
            collected += 1
            if len(payload) < page_size:
                break
            if max_pages is not None and collected >= int(max_pages):
                break
            page += 1
        return pages

    def fetch_products_for_tag(self, tag_id: int) -> List[Dict[str, Any]]:
        per_page = min(100, int(self.settings.get('per_page') or 100))
        page = 1
        items: List[Dict[str, Any]] = []
        while True:
            payload = self._get('/products', {
                'tag': int(tag_id),
                'page': page,
                'per_page': per_page,
                '_fields': 'id,name',
            })
            if not payload:
                break
            for item in payload:
                items.append({
                    'productId': str(item.get('id')),
                    'productTitle': str(item.get('name') or '').strip(),
                })
            if len(payload) < per_page:
                break
            page += 1
        return items
