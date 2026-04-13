from typing import Any, Dict, Optional
from urllib.parse import parse_qs, urlparse

from classifier.models import TagSession
from managers.error_summary import classify_error
from utils.logging_utils import append_log
from utils.time_utils import elapsed_seconds, utc_now_iso


class TagManager:
    def __init__(self, config: Dict[str, Any], browser_manager, page_manager, persistence_manager, woo_api_manager=None):
        self.config = config
        self.browser_manager = browser_manager
        self.page_manager = page_manager
        self.persistence_manager = persistence_manager
        self.woo_api_manager = woo_api_manager

    def _next_page_url(self, page, selectors: Dict[str, str]) -> Optional[str]:
        next_loc = page.locator(selectors['next_page']).first
        if next_loc.count() == 0:
            return None
        href = next_loc.get_attribute('href')
        return href or None

    def _run_mode(self, tag_or_payload) -> str:
        value = ''
        if isinstance(tag_or_payload, dict):
            value = str(tag_or_payload.get('run_mode') or '')
        else:
            value = str(getattr(tag_or_payload, 'run_mode', '') or '')
        value = value.strip().lower()
        return value if value in {'full_tag', 'per_page'} else 'full_tag'


    def _execution_mode(self, tag_or_payload) -> str:
        if isinstance(tag_or_payload, dict):
            value = str(tag_or_payload.get('execution_mode') or '')
        else:
            value = str(getattr(tag_or_payload, 'execution_mode', '') or '')
        value = value.strip().lower()
        return value if value in {'full_review', 'actionable_review', 'assessment_only'} else 'full_review'

    def _page_number_from_url(self, url: str) -> int:
        try:
            parsed = urlparse(url or '')
            paged = parse_qs(parsed.query).get('paged', ['1'])[0]
            value = int(str(paged).strip() or '1')
            return value if value > 0 else 1
        except Exception:
            return 1

    def _build_carry_forward_index(self, source_meta: Dict[str, str]) -> Dict[str, Dict[str, Any]]:
        if not source_meta or not source_meta.get('run_id') or not source_meta.get('tag_id'):
            return {}
        old_tag = self.persistence_manager.load_tag(source_meta['run_id'], source_meta['tag_id'])
        if not old_tag:
            return {}
        carried = {}
        for page in old_tag.get('pages', []) or []:
            assessments = {a.get('product_id'): a for a in page.get('assessments', []) if a.get('product_id')}
            for diag in page.get('row_diagnostics', []) or []:
                product_id = diag.get('product_id')
                if not product_id or product_id in carried:
                    continue
                assessment = assessments.get(product_id)
                if not assessment:
                    continue
                if diag.get('skip_reason'):
                    continue
                if not diag.get('title_found'):
                    continue
                if diag.get('parse_status') in {'api_failure', 'parse_failure'}:
                    continue
                if diag.get('api_error'):
                    continue
                if not assessment.get('decision'):
                    continue
                carried[product_id] = {'assessment': assessment, 'diagnostic': diag}
        return carried


    def _api_mode_enabled(self) -> bool:
        return bool(self.woo_api_manager and self.woo_api_manager.is_enabled())

    def _process_tag_via_api(self, run_id: str, tag) -> Dict[str, Any]:
        selectors = self.config['selectors']
        carry_forward_index = self._build_carry_forward_index(getattr(tag, 'carry_forward_source', None) or {})
        append_log(tag.logs, 'info', 'Woo API mode enabled for tag', tag_title=tag.tag_title, carry_forward_rows=len(carry_forward_index))
        resolved = self.woo_api_manager.resolve_tag(tag.tag_url, tag.tag_title)
        products = self.woo_api_manager.fetch_products_for_tag(int(resolved['id']))
        append_log(tag.logs, 'info', 'Woo API products fetched', tag_id=resolved['id'], products=len(products), slug=resolved.get('slug'))
        result_map = self.page_manager.build_precomputed_result_map(tag.tag_title, tag.strictness, products, carry_forward_index=carry_forward_index)
        append_log(tag.logs, 'info', 'Woo API classification map built', results=len(result_map))
        current_url = tag.tag_url
        page_number = self._page_number_from_url(current_url)
        run_mode = self._run_mode(tag)
        while current_url and page_number <= tag.max_pages:
            page_data = self.page_manager.process_page_with_result_map(run_id, tag.to_dict(), page_number, current_url, result_map)
            tag.pages.append(page_data)
            tag.keep_count += page_data.get('keep_count', 0)
            tag.mark_count += page_data.get('mark_count', 0)
            tag.review_count += page_data.get('review_count', 0)
            tag.carried_forward_count += page_data.get('carried_forward_count', 0)
            if page_data['status'] == 'actionable':
                tag.actionable_count += 1
            elif page_data['status'] == 'review_only':
                tag.review_only_count += 1
            elif page_data['status'] == 'clean':
                tag.clean_count += 1
            elif page_data['status'] == 'failed':
                tag.failed_count += 1
            if page_data.get('preserved_tab'):
                tag.open_page_tabs.append(page_number)
            tag.duration_seconds = elapsed_seconds(tag.started_at, None)
            self.persistence_manager.save_tag(run_id, tag.to_dict())
            if run_mode == 'per_page':
                break
            token = page_data.get('tab_focus_token')
            page_obj = self.browser_manager.tab_registry.get(token) if token else None
            if page_obj and not page_obj.is_closed():
                current_url = self._next_page_url(page_obj, selectors)
            else:
                temp_page = self.browser_manager.new_page(current_url)
                try:
                    temp_page.wait_for_selector(selectors['products_table'], timeout=self.config.get('runtime', {}).get('row_wait_timeout_ms', 60000))
                    current_url = self._next_page_url(temp_page, selectors)
                finally:
                    temp_page.close()
            if not current_url:
                break
            page_number += 1
        tag.status = 'failed' if tag.failed_count else ('completed_waiting_for_human' if tag.open_page_tabs else 'completed')
        tag.ended_at = utc_now_iso()
        tag.duration_seconds = elapsed_seconds(tag.started_at, tag.ended_at)
        append_log(tag.logs, 'info', 'Tag completed', status=tag.status, duration_seconds=tag.duration_seconds, carried_forward_count=tag.carried_forward_count, mode='woo_api')
        data = tag.to_dict()
        self.persistence_manager.save_tag(run_id, data)
        return data

    def process_tag(self, run_id: str, tag_payload: Dict[str, Any]) -> Dict[str, Any]:
        tag = TagSession(
            tag_id=tag_payload['tag_id'],
            run_id=run_id,
            tag_title=tag_payload['tag_title'],
            tag_url=tag_payload['tag_url'],
            strictness=tag_payload['strictness'],
            max_pages=int(tag_payload['max_pages']),
            dry_run=bool(tag_payload['dry_run']),
            status='running',
            started_at=utc_now_iso(),
        )
        selectors = self.config['selectors']
        current_url = tag.tag_url
        setattr(tag, 'carry_forward_source', tag_payload.get('carry_forward_source') or {})
        setattr(tag, 'run_mode', self._run_mode(tag_payload))
        setattr(tag, 'execution_mode', self._execution_mode(tag_payload))
        page_number = self._page_number_from_url(current_url)
        carry_forward_index = self._build_carry_forward_index(tag_payload.get('carry_forward_source') or {})
        self.persistence_manager.save_tag(run_id, tag.to_dict())
        try:
            if self._api_mode_enabled():
                return self._process_tag_via_api(run_id, tag)
            append_log(tag.logs, 'info', 'Started tag', tag_title=tag.tag_title, tag_url=tag.tag_url, carry_forward_rows=len(carry_forward_index))
            self.persistence_manager.save_tag(run_id, tag.to_dict())
            while current_url and page_number <= tag.max_pages:
                page_data = self.page_manager.process_page(run_id, tag.to_dict(), page_number, current_url, carry_forward_index=carry_forward_index)
                tag.pages.append(page_data)
                tag.keep_count += page_data.get('keep_count', 0)
                tag.mark_count += page_data.get('mark_count', 0)
                tag.review_count += page_data.get('review_count', 0)
                tag.carried_forward_count += page_data.get('carried_forward_count', 0)
                if page_data['status'] == 'actionable':
                    tag.actionable_count += 1
                elif page_data['status'] == 'review_only':
                    tag.review_only_count += 1
                elif page_data['status'] == 'clean':
                    tag.clean_count += 1
                elif page_data['status'] == 'failed':
                    tag.failed_count += 1
                if page_data.get('preserved_tab'):
                    tag.open_page_tabs.append(page_number)

                tag.duration_seconds = elapsed_seconds(tag.started_at, None)
                self.persistence_manager.save_tag(run_id, tag.to_dict())

                if self._run_mode(tag_payload) == 'per_page':
                    break
                if page_number >= tag.max_pages:
                    break
                token = page_data.get('tab_focus_token')
                page_obj = self.browser_manager.tab_registry.get(token) if token else None
                if page_obj and not page_obj.is_closed():
                    current_url = self._next_page_url(page_obj, selectors)
                else:
                    temp_page = self.browser_manager.new_page(current_url)
                    try:
                        temp_page.wait_for_selector(selectors['products_table'], timeout=self.config.get('runtime', {}).get('row_wait_timeout_ms', 60000))
                        current_url = self._next_page_url(temp_page, selectors)
                    finally:
                        temp_page.close()
                page_number += 1

            tag.status = 'failed' if tag.failed_count else ('completed_waiting_for_human' if tag.open_page_tabs else 'completed')
            tag.ended_at = utc_now_iso()
            tag.duration_seconds = elapsed_seconds(tag.started_at, tag.ended_at)
            append_log(tag.logs, 'info', 'Tag completed', status=tag.status, duration_seconds=tag.duration_seconds, carried_forward_count=tag.carried_forward_count)
        except Exception as exc:
            error_type, error_summary = classify_error(exc)
            tag.status = 'failed'
            tag.error_type = error_type
            tag.error_summary = error_summary
            tag.last_error = str(exc)
            tag.ended_at = utc_now_iso()
            tag.duration_seconds = elapsed_seconds(tag.started_at, tag.ended_at)
            append_log(tag.logs, 'error', 'Tag failed', error=str(exc), error_type=error_type)

        data = tag.to_dict()
        self.persistence_manager.save_tag(run_id, data)
        return data
