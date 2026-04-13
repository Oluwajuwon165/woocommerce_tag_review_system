from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional, Tuple
from classifier.models import AssessmentRecord, PageSession
from utils.logging_utils import append_log
from utils.time_utils import elapsed_seconds, utc_now_iso
from managers.error_summary import classify_error
import uuid


class PageManager:
    def __init__(self, config: Dict[str, Any], classifier, browser_manager, persistence_manager):
        self.config = config
        self.classifier = classifier
        self.browser_manager = browser_manager
        self.persistence_manager = persistence_manager

    def _strict_threshold(self, strictness: str) -> float:
        base = float(self.config['classifier'].get('auto_mark_min_confidence', 0.88))
        if strictness == 'loose':
            return min(0.98, base + 0.05)
        if strictness == 'strict':
            return max(0.50, base - 0.05)
        return base

    @staticmethod
    def _normalize_preview(text: str) -> str:
        text = (text or '').strip()
        text = ' '.join(text.split())
        return text[:220]

    def _snapshot_rows(self, page, selectors: Dict[str, str], bulk: Dict[str, str]) -> Tuple[List[Dict[str, Any]], int, int]:
        product_rows_selector = selectors.get('stable_product_rows', "#the-list tr[id^='post-']")
        helper_rows_selector = selectors.get('helper_rows', f"{bulk['acp_bulk_row']}, #the-list tr.inline-editor, #the-list tr.hidden")
        script = """
        ({ productRowsSelector, helperRowsSelector, titleSelector, checkboxSelector }) => {
          const root = document.querySelector('#the-list');
          const allRows = root ? Array.from(root.querySelectorAll('tr')) : [];
          const productRows = Array.from(document.querySelectorAll(productRowsSelector));
          const helperRows = helperRowsSelector ? Array.from(document.querySelectorAll(helperRowsSelector)) : [];
          const helperIds = new Set(helperRows.map(r => r.id || ''));
          return {
            totalRows: allRows.length,
            helperRowsCount: helperRows.length,
            items: productRows.map((row, index) => {
              const titleNode = row.querySelector(titleSelector);
              const checkboxNode = row.querySelector(checkboxSelector);
              const rowText = (row.innerText || '').replace(/\s+/g, ' ').trim();
              const editLink = titleNode ? titleNode.getAttribute('href') || '' : '';
              const checkboxValue = checkboxNode ? checkboxNode.value || '' : '';
              const rowId = row.id || '';
              const productId = checkboxValue || (rowId.startsWith('post-') ? rowId.replace('post-', '') : '') || editLink || rowText;
              return {
                snapshotIndex: index + 1,
                rowDomId: rowId,
                productId,
                productTitle: titleNode ? (titleNode.textContent || '').trim() : '',
                titleFound: !!titleNode,
                checkboxExists: !!checkboxNode,
                checkboxValue,
                editLink,
                rowPreview: rowText.slice(0, 220),
                rowType: helperIds.has(rowId) ? 'helper_row' : 'product_row'
              };
            })
          };
        }
        """
        snapshot = page.evaluate(script, {
            'productRowsSelector': product_rows_selector,
            'helperRowsSelector': helper_rows_selector,
            'titleSelector': selectors['title_link'],
            'checkboxSelector': selectors['checkbox'],
        })
        return snapshot.get('items', []), int(snapshot.get('totalRows', 0) or 0), int(snapshot.get('helperRowsCount', 0) or 0)

    def _build_diag(self, item: Dict[str, Any], threshold: float) -> Dict[str, Any]:
        return {
            'row_index': item.get('snapshotIndex'),
            'row_dom_id': item.get('rowDomId', ''),
            'product_id': item.get('productId', ''),
            'row_type': item.get('rowType', 'product_row'),
            'title_found': bool(item.get('titleFound')),
            'product_title': item.get('productTitle', ''),
            'decision': '',
            'confidence': None,
            'classification_started': False,
            'classification_source': '',
            'provider_used': '',
            'model_used': '',
            'tier_used': '',
            'attempt_log': [],
            'threshold_required': threshold,
            'checkbox_exists': bool(item.get('checkboxExists')),
            'checkbox_already_checked': False,
            'checkbox_attempted': False,
            'checkbox_result': 'not_attempted',
            'skip_reason': '',
            'cache_hit': False,
            'llm_called': False,
            'parse_status': '',
            'api_error': '',
            'reason': '',
            'raw_response_preview': '',
            'row_text_preview': self._normalize_preview(item.get('rowPreview', '')),
            'carried_forward': False,
            'result_status': '',
        }

    def _make_record(self, tag_session: Dict[str, Any], page_number: int, product_id: str, row_dom_id: str, product_title: str, result: Dict[str, Any], carried_forward: bool) -> Dict[str, Any]:
        status = 'success'
        if result.get('parse_status') == 'api_failure' or result.get('api_error'):
            status = 'api_failed'
        elif result.get('parse_status') == 'parse_failure':
            status = 'parse_failed'
        record = AssessmentRecord(
            assessment_id=uuid.uuid4().hex,
            tag_id=tag_session['tag_id'],
            page_number=page_number,
            product_title=product_title,
            decision=result.get('decision', 'review'),
            confidence=float(result.get('confidence', 0.0) or 0.0),
            source=result.get('source', 'unknown'),
            checkbox_ticked=False,
            timestamp=utc_now_iso(),
            reason=result.get('reason', ''),
            provider=result.get('provider_used', ''),
            model=result.get('model_used', ''),
            tier_label=result.get('tier_used', ''),
            product_id=product_id,
            row_dom_id=row_dom_id,
            result_status=status,
            reused_from_previous_run=carried_forward,
        )
        return record.to_dict()

    def _classify_worker(self, tag_title: str, strictness: str, item: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        return item.get('productId', ''), self.classifier.classify(tag_title, item.get('productTitle', ''), strictness)


    def build_precomputed_result_map(self, tag_title: str, strictness: str, products: List[Dict[str, Any]], carry_forward_index: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Dict[str, Any]]:
        carry_forward_index = carry_forward_index or {}
        result_map: Dict[str, Dict[str, Any]] = {}
        workers = max(1, int(self.config.get('runtime', {}).get('classification_workers', 8) or 8))
        pending: List[Dict[str, Any]] = []
        for item in products:
            product_id = str(item.get('productId') or '').strip()
            title = str(item.get('productTitle') or '').strip()
            if not product_id or not title:
                continue
            prior = carry_forward_index.get(product_id)
            if prior:
                old_assessment = prior.get('assessment', {})
                old_diag = prior.get('diagnostic', {})
                result_map[product_id] = {
                    'decision': old_assessment.get('decision', 'review'),
                    'confidence': float(old_assessment.get('confidence', 0.0) or 0.0),
                    'reason': old_assessment.get('reason', old_diag.get('reason', '')),
                    'source': 'carry_forward',
                    'cache_hit': False,
                    'llm_called': False,
                    'parse_status': old_diag.get('parse_status', 'ok'),
                    'provider_used': old_assessment.get('provider') or old_diag.get('provider_used', ''),
                    'model_used': old_assessment.get('model') or old_diag.get('model_used', ''),
                    'tier_used': old_assessment.get('tier_label') or old_diag.get('tier_used', 'carry_forward'),
                    'api_error': '',
                    'raw_response': '',
                    'attempt_log': old_diag.get('attempt_log', []),
                    'carried_forward': True,
                }
            else:
                pending.append({'productId': product_id, 'productTitle': title})

        if pending:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                future_map = {executor.submit(self._classify_worker, tag_title, strictness, item): item for item in pending}
                for future in as_completed(future_map):
                    item = future_map[future]
                    product_id = item['productId']
                    try:
                        _, result = future.result()
                    except Exception as exc:
                        result = {
                            'decision': 'review', 'confidence': 0.0, 'reason': 'LLM/API failure', 'source': 'fallback', 'cache_hit': False,
                            'llm_called': True, 'parse_status': 'api_failure', 'raw_response': '', 'api_error': str(exc),
                            'provider_used': 'fallback', 'model_used': '', 'tier_used': 'failed', 'attempt_log': [], 'carried_forward': False,
                        }
                    result.setdefault('carried_forward', False)
                    result_map[product_id] = result
        return result_map

    def process_page_with_result_map(self, run_id: str, tag_session: Dict[str, Any], page_number: int, page_url: str, result_map: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        page_session = PageSession(page_number=page_number, page_url=page_url, status='running', started_at=utc_now_iso())
        selectors = self.config['selectors']
        bulk = self.config['bulk_edit']
        runtime = self.config.get('runtime', {})
        threshold = self._strict_threshold(tag_session['strictness'])
        page = None
        token = None
        try:
            append_log(page_session.logs, 'info', 'Opening tag page (API replay mode)', page_number=page_number, page_url=page_url)
            page = self.browser_manager.new_page(page_url)
            token = self.browser_manager.register_tab(page, note=f"{tag_session['tag_title']} page {page_number}")
            page_session.tab_focus_token = token
            page_session.browser_tab_id = token
            page.wait_for_selector(selectors['products_table'], timeout=runtime.get('row_wait_timeout_ms', 60000))
            snapshot_rows, total_rows, helper_rows_count = self._snapshot_rows(page, selectors, bulk)
            page_session.total_rows = total_rows
            page_session.helper_rows_count = helper_rows_count
            append_log(page_session.logs, 'info', 'Stable row snapshot captured in API replay mode', total_rows=total_rows, snapshot_product_rows=len(snapshot_rows), helper_rows=helper_rows_count, threshold=threshold)

            seen_product_ids = set()
            rows_to_checkbox: List[Dict[str, Any]] = []
            page_ids_to_check: List[str] = []
            row_dom_ids_to_check: List[str] = []

            for item in snapshot_rows:
                diagnostic = self._build_diag(item, threshold)
                product_id = diagnostic['product_id']
                if not product_id:
                    diagnostic['skip_reason'] = 'missing stable product identifier'
                    page_session.skipped_rows_count += 1
                    page_session.row_diagnostics.append(diagnostic)
                    continue
                if product_id in seen_product_ids:
                    diagnostic['skip_reason'] = 'duplicate product row in stable snapshot'
                    diagnostic['checkbox_result'] = 'duplicate_ignored'
                    page_session.duplicate_rows_count += 1
                    page_session.skipped_rows_count += 1
                    page_session.row_diagnostics.append(diagnostic)
                    continue
                seen_product_ids.add(product_id)
                page_session.candidate_rows += 1
                if not diagnostic['title_found']:
                    diagnostic['skip_reason'] = 'title selector missing on stable product row'
                    page_session.skipped_rows_count += 1
                    page_session.row_diagnostics.append(diagnostic)
                    continue

                result = dict(result_map.get(product_id) or {})
                if not result:
                    result = self.classifier.classify(tag_session['tag_title'], diagnostic['product_title'], tag_session['strictness'])
                    result.setdefault('carried_forward', False)
                decision = result.get('decision', 'review')
                confidence = float(result.get('confidence', 0.0) or 0.0)
                diagnostic['classification_started'] = True
                diagnostic['classification_source'] = result.get('source', 'unknown')
                diagnostic['decision'] = decision
                diagnostic['confidence'] = confidence
                diagnostic['cache_hit'] = bool(result.get('cache_hit', False))
                diagnostic['llm_called'] = bool(result.get('llm_called', False))
                diagnostic['parse_status'] = result.get('parse_status', '')
                diagnostic['provider_used'] = result.get('provider_used', '')
                diagnostic['model_used'] = result.get('model_used', '')
                diagnostic['tier_used'] = result.get('tier_used', '')
                diagnostic['attempt_log'] = result.get('attempt_log', [])
                diagnostic['api_error'] = result.get('api_error', '')
                diagnostic['reason'] = result.get('reason', '')
                diagnostic['raw_response_preview'] = (result.get('raw_response', '') or '')[:280]
                diagnostic['carried_forward'] = bool(result.get('carried_forward', False))
                diagnostic['result_status'] = 'success'
                if result.get('parse_status') == 'api_failure' or result.get('api_error'):
                    diagnostic['result_status'] = 'api_failed'
                elif result.get('parse_status') == 'parse_failure':
                    diagnostic['result_status'] = 'parse_failed'

                if diagnostic['cache_hit']:
                    page_session.cache_hit_count += 1
                if diagnostic['llm_called']:
                    page_session.llm_call_count += 1
                if diagnostic['classification_source'] == 'fallback':
                    page_session.fallback_count += 1
                if diagnostic['parse_status'] == 'parse_failure':
                    page_session.parse_failure_count += 1
                if diagnostic['parse_status'] == 'api_failure':
                    page_session.api_failure_count += 1
                if diagnostic['carried_forward']:
                    page_session.carried_forward_count += 1

                if decision == 'keep':
                    page_session.keep_count += 1
                elif decision == 'mark':
                    page_session.mark_count += 1
                else:
                    page_session.review_count += 1

                should_tick = (
                    not tag_session['dry_run'] and
                    decision == 'mark' and
                    diagnostic['checkbox_exists'] and
                    (
                        confidence >= threshold or
                        diagnostic['classification_source'] == 'cache'
                    )
                )
                if should_tick:
                    diagnostic['checkbox_attempted'] = True
                    diagnostic['checkbox_result'] = 'queued_for_checkbox'
                    rows_to_checkbox.append({'product_id': product_id, 'row_dom_id': diagnostic['row_dom_id'], 'product_title': diagnostic['product_title']})
                    page_ids_to_check.append(product_id)
                    if diagnostic['row_dom_id']:
                        row_dom_ids_to_check.append(diagnostic['row_dom_id'])
                else:
                    if tag_session['dry_run']:
                        diagnostic['checkbox_result'] = 'dry_run'
                    elif decision != 'mark':
                        diagnostic['checkbox_result'] = f'decision_{decision}'
                    elif confidence < threshold and diagnostic['classification_source'] != 'cache':
                        diagnostic['checkbox_result'] = 'below_threshold'
                    elif not diagnostic['checkbox_exists']:
                        diagnostic['checkbox_result'] = 'checkbox_missing'

                page_session.assessments.append(self._make_record(tag_session, page_number, product_id, diagnostic['row_dom_id'], diagnostic['product_title'], result, diagnostic['carried_forward']))
                page_session.assessed_rows += 1
                page_session.row_diagnostics.append(diagnostic)

            if page_ids_to_check:
                apply_script = """
                ({productIds, rowDomIds, checkboxSelector}) => {
                  const ids = new Set((productIds || []).map(String));
                  const domIds = new Set((rowDomIds || []).map(String));
                  const touched = [];
                  const rows = Array.from(document.querySelectorAll('#the-list tr[id^="post-"]'));
                  rows.forEach(row => {
                    const rowId = row.id || '';
                    const productId = rowId.startsWith('post-') ? rowId.replace('post-', '') : '';
                    if (!ids.has(productId) && !domIds.has(rowId)) return;
                    const cb = row.querySelector(checkboxSelector);
                    if (!cb) {
                      touched.push({productId, rowDomId: rowId, result: 'checkbox_missing'});
                      return;
                    }
                    if (cb.checked) {
                      touched.push({productId, rowDomId: rowId, result: 'already_checked'});
                    } else {
                      cb.checked = true;
                      cb.dispatchEvent(new Event('change', {bubbles: true}));
                      cb.dispatchEvent(new Event('click', {bubbles: true}));
                      touched.push({productId, rowDomId: rowId, result: 'checked_now'});
                    }
                  });
                  return touched;
                }
                """
                touched = page.evaluate(apply_script, {'productIds': page_ids_to_check, 'rowDomIds': row_dom_ids_to_check, 'checkboxSelector': selectors['checkbox']}) or []
                touched_map = {(str(x.get('productId') or ''), str(x.get('rowDomId') or '')): x.get('result') for x in touched}
                for target in rows_to_checkbox:
                    key = (str(target['product_id']), str(target['row_dom_id'] or ''))
                    result = touched_map.get(key)
                    row_diag = next((d for d in page_session.row_diagnostics if d.get('product_id') == target['product_id'] and d.get('row_dom_id') == target['row_dom_id']), None)
                    if result == 'checked_now':
                        page_session.checkbox_count += 1
                        if row_diag:
                            row_diag['checkbox_result'] = 'checked_now'
                        for assessment in page_session.assessments:
                            if assessment.get('product_id') == target['product_id'] and assessment.get('checkbox_ticked') is False:
                                assessment['checkbox_ticked'] = True
                                break
                    elif result == 'already_checked':
                        if row_diag:
                            row_diag['checkbox_result'] = 'already_checked'
                    elif result == 'checkbox_missing':
                        if row_diag:
                            row_diag['checkbox_result'] = 'checkbox_missing'
                    else:
                        if row_diag:
                            row_diag['checkbox_result'] = 'row_not_found_for_checkbox'

            page_session.acp_bulk_ready = page.locator(bulk['acp_bulk_row']).count() > 0 or page.locator(bulk['tag_bulk_edit_button']).count() > 0
            page_session.ready_for_human = page_session.checkbox_count > 0 and (page_session.acp_bulk_ready or True)
            page_session.ended_at = utc_now_iso()
            page_session.duration_seconds = elapsed_seconds(page_session.started_at, page_session.ended_at)
            if page_session.checkbox_count > 0:
                page_session.status = 'actionable'
            elif page_session.review_count > 0:
                page_session.status = 'review_only'
            else:
                page_session.status = 'clean'
            preserve_all_pages = bool(runtime.get('preserve_all_pages_open', True))
            preserve_review_only = bool(runtime.get('preserve_review_only_pages', True))
            page_session.preserved_tab = preserve_all_pages or page_session.status == 'actionable' or (page_session.status == 'review_only' and preserve_review_only)
            if not page_session.preserved_tab and page:
                page.close()
                self.browser_manager.unregister_tab(token)
                page_session.browser_tab_id = None
                page_session.tab_focus_token = None
            else:
                try:
                    page.bring_to_front()
                except Exception:
                    pass
                append_log(page_session.logs, 'info', 'Page preserved', page_status=page_session.status)
        except Exception as exc:
            page_session.ended_at = utc_now_iso()
            page_session.duration_seconds = elapsed_seconds(page_session.started_at, page_session.ended_at)
            error_type, error_summary = classify_error(exc)
            page_session.status = 'failed'
            page_session.error_type = error_type
            page_session.error_summary = error_summary
            page_session.last_error = str(exc)
            append_log(page_session.logs, 'error', 'Page failed', error=str(exc), error_type=error_type)
            if page and not page.is_closed():
                page.close()
            if token:
                self.browser_manager.unregister_tab(token)
            page_session.browser_tab_id = None
            page_session.tab_focus_token = None
        data = page_session.to_dict()
        self.persistence_manager.save_page(run_id, tag_session['tag_id'], data)
        return data

    def replay_selected_assessments(self, run_id: str, tag_session: Dict[str, Any], page_session: Dict[str, Any], selected_products: List[Dict[str, Any]]) -> Dict[str, Any]:
        selectors = self.config['selectors']
        runtime = self.config.get('runtime', {})
        page = None
        token = None
        page_url = str(page_session.get('page_url') or tag_session.get('tag_url') or '').strip()
        if not page_url:
            raise RuntimeError('No page URL is available for replay.')

        normalized = []
        seen = set()
        for item in selected_products or []:
            product_id = str(item.get('product_id') or '').strip()
            row_dom_id = str(item.get('row_dom_id') or '').strip()
            product_title = str(item.get('product_title') or '').strip()
            if not (product_id or row_dom_id or product_title):
                continue
            key = (product_id, row_dom_id, product_title.casefold())
            if key in seen:
                continue
            seen.add(key)
            normalized.append({
                'product_id': product_id,
                'row_dom_id': row_dom_id,
                'product_title': product_title,
            })
        if not normalized:
            raise RuntimeError('No selected products were provided for replay.')

        self.browser_manager.login_once()
        try:
            page = self.browser_manager.new_page(page_url)
        except Exception:
            self.browser_manager.close_browser()
            self.browser_manager.login_once()
            page = self.browser_manager.new_page(page_url)

        token = self.browser_manager.register_tab(page, note=f"manual replay {tag_session.get('tag_title', '')} page {page_session.get('page_number', '')}")
        page.wait_for_selector(selectors['products_table'], timeout=runtime.get('row_wait_timeout_ms', 60000))

        apply_script = """
        ({ selected, checkboxSelector, titleSelector }) => {
          const normalize = (value) => String(value || '').replace(/\s+/g, ' ').trim().toLowerCase();
          const byId = new Map();
          const byRowId = new Map();
          const byTitle = new Map();
          (selected || []).forEach(item => {
            const payload = {
              product_id: String(item.product_id || ''),
              row_dom_id: String(item.row_dom_id || ''),
              product_title: String(item.product_title || ''),
            };
            if (payload.product_id) byId.set(payload.product_id, payload);
            if (payload.row_dom_id) byRowId.set(payload.row_dom_id, payload);
            const titleKey = normalize(payload.product_title);
            if (titleKey && !payload.product_id && !payload.row_dom_id) byTitle.set(titleKey, payload);
          });
          const rows = Array.from(document.querySelectorAll('#the-list tr[id^="post-"]'));
          const touched = [];
          rows.forEach((row) => {
            const rowId = String(row.id || '');
            const productId = rowId.startsWith('post-') ? rowId.replace('post-', '') : '';
            const titleNode = row.querySelector(titleSelector);
            const titleKey = normalize(titleNode ? titleNode.textContent : '');
            const selectedItem = byId.get(productId) || byRowId.get(rowId) || byTitle.get(titleKey);
            if (!selectedItem) return;
            const checkbox = row.querySelector(checkboxSelector);
            if (!checkbox) {
              touched.push({product_id: productId, row_dom_id: rowId, product_title: titleKey, result: 'checkbox_missing'});
              return;
            }
            if (!checkbox.checked) {
              checkbox.checked = true;
              checkbox.dispatchEvent(new Event('change', { bubbles: true }));
              checkbox.dispatchEvent(new Event('click', { bubbles: true }));
              touched.push({product_id: productId, row_dom_id: rowId, product_title: titleKey, result: 'checked_now'});
            } else {
              touched.push({product_id: productId, row_dom_id: rowId, product_title: titleKey, result: 'already_checked'});
            }
          });
          return touched;
        }
        """

        touched = page.evaluate(apply_script, {
            'selected': normalized,
            'checkboxSelector': selectors['checkbox'],
            'titleSelector': selectors['title_link'],
        }) or []

        matched_keys = set()
        checked_now = 0
        already_checked = 0
        checkbox_missing = 0
        for item in touched:
            matched_keys.add((str(item.get('product_id') or '').strip(), str(item.get('row_dom_id') or '').strip(), str(item.get('product_title') or '').strip()))
            result = item.get('result')
            if result == 'checked_now':
                checked_now += 1
            elif result == 'already_checked':
                already_checked += 1
            elif result == 'checkbox_missing':
                checkbox_missing += 1

        def item_key(obj: Dict[str, Any]):
            return (str(obj.get('product_id') or '').strip(), str(obj.get('row_dom_id') or '').strip(), str(obj.get('product_title') or '').strip().casefold())

        touched_by_selection = set()
        for item in touched:
            touched_by_selection.add(item_key(item))
        not_found_items = []
        for item in normalized:
            if item_key(item) not in touched_by_selection:
                not_found_items.append(item)

        try:
            page.bring_to_front()
        except Exception:
            pass

        append_log(page_session.setdefault('logs', []), 'info', 'Manual replay opened in browser', selected_count=len(normalized), checked_now=checked_now, already_checked=already_checked, not_found=len(not_found_items), page_url=page_url)
        self.persistence_manager.save_page(run_id, tag_session['tag_id'], page_session)
        return {
            'ok': True,
            'opened_page_url': page.url,
            'tab_focus_token': token,
            'selected_count': len(normalized),
            'matched_count': len(touched),
            'checked_now': checked_now,
            'already_checked': already_checked,
            'checkbox_missing': checkbox_missing,
            'not_found_count': len(not_found_items),
            'not_found_products': not_found_items[:25],
        }

    def process_page(self, run_id: str, tag_session: Dict[str, Any], page_number: int, page_url: str, carry_forward_index: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
        carry_forward_index = carry_forward_index or {}
        page_session = PageSession(page_number=page_number, page_url=page_url, status='running', started_at=utc_now_iso())
        selectors = self.config['selectors']
        bulk = self.config['bulk_edit']
        runtime = self.config.get('runtime', {})
        threshold = self._strict_threshold(tag_session['strictness'])
        page = None
        token = None
        try:
            append_log(page_session.logs, 'info', 'Opening tag page', page_number=page_number, page_url=page_url)
            page = self.browser_manager.new_page(page_url)
            token = self.browser_manager.register_tab(page, note=f"{tag_session['tag_title']} page {page_number}")
            page_session.tab_focus_token = token
            page_session.browser_tab_id = token
            page.wait_for_selector(selectors['products_table'], timeout=runtime.get('row_wait_timeout_ms', 60000))

            snapshot_rows, total_rows, helper_rows_count = self._snapshot_rows(page, selectors, bulk)
            page_session.total_rows = total_rows
            page_session.helper_rows_count = helper_rows_count
            append_log(page_session.logs, 'info', 'Stable row snapshot captured', total_rows=total_rows, snapshot_product_rows=len(snapshot_rows), helper_rows=helper_rows_count, threshold=threshold)

            seen_product_ids = set()
            rows_to_checkbox: List[Dict[str, Any]] = []
            classification_items: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
            diag_by_product: Dict[str, Dict[str, Any]] = {}
            item_by_product: Dict[str, Dict[str, Any]] = {}

            for item in snapshot_rows:
                diagnostic = self._build_diag(item, threshold)
                product_id = diagnostic['product_id']
                try:
                    if not product_id:
                        diagnostic['skip_reason'] = 'missing stable product identifier'
                        page_session.skipped_rows_count += 1
                        page_session.row_diagnostics.append(diagnostic)
                        continue
                    if product_id in seen_product_ids:
                        diagnostic['skip_reason'] = 'duplicate product row in stable snapshot'
                        diagnostic['checkbox_result'] = 'duplicate_ignored'
                        page_session.duplicate_rows_count += 1
                        page_session.skipped_rows_count += 1
                        page_session.row_diagnostics.append(diagnostic)
                        append_log(page_session.logs, 'warning', 'Duplicate product row ignored', row_index=diagnostic['row_index'], product_id=product_id, product_title=diagnostic['product_title'])
                        continue
                    seen_product_ids.add(product_id)
                    page_session.candidate_rows += 1
                    if not diagnostic['title_found']:
                        diagnostic['skip_reason'] = 'title selector missing on stable product row'
                        page_session.skipped_rows_count += 1
                        page_session.row_diagnostics.append(diagnostic)
                        append_log(page_session.logs, 'warning', 'Row skipped', row_index=diagnostic['row_index'], product_id=product_id, skip_reason=diagnostic['skip_reason'])
                        continue
                    diag_by_product[product_id] = diagnostic
                    item_by_product[product_id] = item
                    classification_items.append((item, diagnostic))
                except Exception as row_exc:
                    diagnostic['skip_reason'] = f'row exception: {str(row_exc)}'
                    page_session.skipped_rows_count += 1
                    page_session.row_diagnostics.append(diagnostic)
                    append_log(page_session.logs, 'warning', 'Malformed row skipped', row_index=diagnostic['row_index'], error=str(row_exc), product_id=diagnostic['product_id'])

            # classify with carry-forward + concurrency
            checkpoint_every = int(runtime.get('page_checkpoint_every', 20) or 20)
            workers = max(1, int(runtime.get('classification_workers', 8) or 8))
            pending_items: List[Dict[str, Any]] = []
            completed_since_checkpoint = 0

            def consume_result(item: Dict[str, Any], diagnostic: Dict[str, Any], result: Dict[str, Any], carried_forward: bool = False):
                nonlocal completed_since_checkpoint
                decision = result.get('decision', 'review')
                confidence = float(result.get('confidence', 0.0) or 0.0)
                diagnostic['classification_started'] = True
                diagnostic['classification_source'] = result.get('source', 'unknown')
                diagnostic['decision'] = decision
                diagnostic['confidence'] = confidence
                diagnostic['cache_hit'] = bool(result.get('cache_hit', False))
                diagnostic['llm_called'] = bool(result.get('llm_called', False))
                diagnostic['parse_status'] = result.get('parse_status', '')
                diagnostic['provider_used'] = result.get('provider_used', '')
                diagnostic['model_used'] = result.get('model_used', '')
                diagnostic['tier_used'] = result.get('tier_used', '')
                diagnostic['attempt_log'] = result.get('attempt_log', [])
                diagnostic['api_error'] = result.get('api_error', '')
                diagnostic['reason'] = result.get('reason', '')
                diagnostic['raw_response_preview'] = (result.get('raw_response', '') or '')[:280]
                diagnostic['carried_forward'] = carried_forward
                diagnostic['result_status'] = 'success'
                if result.get('parse_status') == 'api_failure' or result.get('api_error'):
                    diagnostic['result_status'] = 'api_failed'
                elif result.get('parse_status') == 'parse_failure':
                    diagnostic['result_status'] = 'parse_failed'
                elif result.get('source') == 'carry_forward':
                    diagnostic['result_status'] = 'success'

                if diagnostic['cache_hit']:
                    page_session.cache_hit_count += 1
                if diagnostic['llm_called']:
                    page_session.llm_call_count += 1
                if diagnostic['classification_source'] == 'fallback':
                    page_session.fallback_count += 1
                if diagnostic['parse_status'] == 'parse_failure':
                    page_session.parse_failure_count += 1
                if diagnostic['parse_status'] == 'api_failure':
                    page_session.api_failure_count += 1
                if carried_forward:
                    page_session.carried_forward_count += 1

                if decision == 'keep':
                    page_session.keep_count += 1
                elif decision == 'mark':
                    page_session.mark_count += 1
                else:
                    page_session.review_count += 1

                should_tick = (
                    not tag_session['dry_run'] and
                    decision == 'mark' and
                    diagnostic['checkbox_exists'] and
                    (
                        confidence >= threshold or
                        diagnostic['classification_source'] == 'cache'
                    )
                )
                if should_tick:
                    diagnostic['checkbox_attempted'] = True
                    diagnostic['checkbox_result'] = 'queued_for_checkbox'
                    rows_to_checkbox.append({
                        'product_id': diagnostic['product_id'],
                        'row_dom_id': diagnostic['row_dom_id'],
                        'row_index': diagnostic['row_index'],
                        'product_title': diagnostic['product_title'],
                    })
                else:
                    if tag_session['dry_run']:
                        diagnostic['checkbox_result'] = 'dry_run'
                    elif decision != 'mark':
                        diagnostic['checkbox_result'] = f'decision_{decision}'
                    elif confidence < threshold and diagnostic['classification_source'] != 'cache':
                        diagnostic['checkbox_result'] = 'below_threshold'
                    elif not diagnostic['checkbox_exists']:
                        diagnostic['checkbox_result'] = 'checkbox_missing'

                page_session.assessments.append(self._make_record(tag_session, page_number, diagnostic['product_id'], diagnostic['row_dom_id'], diagnostic['product_title'], result, carried_forward))
                page_session.assessed_rows += 1
                page_session.row_diagnostics.append(diagnostic)
                completed_since_checkpoint += 1
                append_log(page_session.logs, 'info', 'Classification completed', row_index=diagnostic['row_index'], product_id=diagnostic['product_id'], decision=decision, confidence=confidence, source=diagnostic['classification_source'], provider=diagnostic['provider_used'], model=diagnostic['model_used'], tier=diagnostic['tier_used'], carried_forward=carried_forward, checkbox_result=diagnostic['checkbox_result'])
                if completed_since_checkpoint >= checkpoint_every:
                    page_session.duration_seconds = elapsed_seconds(page_session.started_at, None)
                    self.persistence_manager.save_page(run_id, tag_session['tag_id'], page_session.to_dict())
                    completed_since_checkpoint = 0

            for item, diagnostic in classification_items:
                prior = carry_forward_index.get(diagnostic['product_id'])
                if prior:
                    old_assessment = prior.get('assessment', {})
                    old_diag = prior.get('diagnostic', {})
                    result = {
                        'decision': old_assessment.get('decision', 'review'),
                        'confidence': float(old_assessment.get('confidence', 0.0) or 0.0),
                        'reason': old_assessment.get('reason', old_diag.get('reason', '')),
                        'source': 'carry_forward',
                        'cache_hit': False,
                        'llm_called': False,
                        'parse_status': old_diag.get('parse_status', 'ok'),
                        'provider_used': old_assessment.get('provider') or old_diag.get('provider_used', ''),
                        'model_used': old_assessment.get('model') or old_diag.get('model_used', ''),
                        'tier_used': old_assessment.get('tier_label') or old_diag.get('tier_used', 'carry_forward'),
                        'api_error': '',
                        'raw_response': '',
                        'attempt_log': old_diag.get('attempt_log', []),
                    }
                    consume_result(item, diagnostic, result, carried_forward=True)
                else:
                    pending_items.append(item)

            if pending_items:
                append_log(page_session.logs, 'info', 'Starting concurrent classification', rows=len(pending_items), workers=workers)
                with ThreadPoolExecutor(max_workers=workers) as executor:
                    future_map = {executor.submit(self._classify_worker, tag_session['tag_title'], tag_session['strictness'], item): item for item in pending_items}
                    for future in as_completed(future_map):
                        item = future_map[future]
                        diagnostic = diag_by_product[item.get('productId', '')]
                        try:
                            _, result = future.result()
                        except Exception as exc:
                            result = {
                                'decision': 'review', 'confidence': 0.0, 'reason': 'LLM/API failure', 'source': 'fallback', 'cache_hit': False,
                                'llm_called': True, 'parse_status': 'api_failure', 'raw_response': '', 'api_error': str(exc),
                                'provider_used': 'fallback', 'model_used': '', 'tier_used': 'failed', 'attempt_log': []
                            }
                        consume_result(item, diagnostic, result, carried_forward=False)

            # checkbox phase
            for target in rows_to_checkbox:
                row_dom_id = target['row_dom_id']
                row_index = target['row_index']
                product_id = target['product_id']
                row_diag = next((d for d in page_session.row_diagnostics if d.get('product_id') == product_id and d.get('row_dom_id') == row_dom_id), None)
                try:
                    if row_dom_id:
                        row = page.locator(f"#{row_dom_id}").first
                    else:
                        row = page.locator(f"#the-list tr[id='post-{product_id}']").first
                    if row.count() == 0:
                        if row_diag:
                            row_diag['checkbox_result'] = 'row_not_found_for_checkbox'
                            row_diag['skip_reason'] = 'stable row not found during checkbox phase'
                        append_log(page_session.logs, 'warning', 'Checkbox skipped: row not found', row_index=row_index, product_id=product_id, row_dom_id=row_dom_id)
                        continue
                    checkbox_locator = row.locator(selectors['checkbox']).first
                    if checkbox_locator.count() == 0:
                        if row_diag:
                            row_diag['checkbox_result'] = 'checkbox_missing'
                        append_log(page_session.logs, 'warning', 'Checkbox missing during checkbox phase', row_index=row_index, product_id=product_id, row_dom_id=row_dom_id)
                        continue
                    checked_now = checkbox_locator.is_checked()
                    if row_diag:
                        row_diag['checkbox_already_checked'] = checked_now
                    if not checked_now:
                        checkbox_locator.check(force=True)
                        page_session.checkbox_count += 1
                        if row_diag:
                            row_diag['checkbox_result'] = 'checked_now'
                        for assessment in page_session.assessments:
                            if assessment.get('product_id') == product_id and assessment.get('checkbox_ticked') is False:
                                assessment['checkbox_ticked'] = True
                                break
                        append_log(page_session.logs, 'info', 'Checkbox ticked', row_index=row_index, product_id=product_id, product_title=target['product_title'])
                    else:
                        if row_diag:
                            row_diag['checkbox_result'] = 'already_checked'
                        append_log(page_session.logs, 'info', 'Checkbox already checked', row_index=row_index, product_id=product_id, product_title=target['product_title'])
                except Exception as checkbox_exc:
                    if row_diag:
                        row_diag['checkbox_result'] = f'checkbox_error: {str(checkbox_exc)}'[:180]
                    append_log(page_session.logs, 'warning', 'Checkbox action failed', row_index=row_index, product_id=product_id, product_title=target['product_title'], error=str(checkbox_exc))

            page_session.acp_bulk_ready = page.locator(bulk['acp_bulk_row']).count() > 0 or page.locator(bulk['tag_bulk_edit_button']).count() > 0
            page_session.ready_for_human = page_session.checkbox_count > 0 and (page_session.acp_bulk_ready or True)
            page_session.ended_at = utc_now_iso()
            page_session.duration_seconds = elapsed_seconds(page_session.started_at, page_session.ended_at)
            append_log(page_session.logs, 'info', 'Page diagnostics summary', total_rows=page_session.total_rows, helper_rows=page_session.helper_rows_count, candidate_rows=page_session.candidate_rows, assessed_rows=page_session.assessed_rows, skipped_rows=page_session.skipped_rows_count, duplicate_rows=page_session.duplicate_rows_count, llm_calls=page_session.llm_call_count, cache_hits=page_session.cache_hit_count, carried_forward=page_session.carried_forward_count, fallbacks=page_session.fallback_count, parse_failures=page_session.parse_failure_count, api_failures=page_session.api_failure_count)

            if page_session.checkbox_count > 0:
                page_session.status = 'actionable'
            elif page_session.review_count > 0:
                page_session.status = 'review_only'
            else:
                page_session.status = 'clean'

            preserve_all_pages = bool(runtime.get('preserve_all_pages_open', True))
            preserve_review_only = bool(runtime.get('preserve_review_only_pages', True))
            page_session.preserved_tab = preserve_all_pages or page_session.status == 'actionable' or (page_session.status == 'review_only' and preserve_review_only)

            if not page_session.preserved_tab and page:
                page.close()
                self.browser_manager.unregister_tab(token)
                page_session.browser_tab_id = None
                page_session.tab_focus_token = None
            else:
                try:
                    page.bring_to_front()
                except Exception:
                    pass
                append_log(page_session.logs, 'info', 'Page preserved', page_status=page_session.status)

        except Exception as exc:
            page_session.ended_at = utc_now_iso()
            page_session.duration_seconds = elapsed_seconds(page_session.started_at, page_session.ended_at)
            error_type, error_summary = classify_error(exc)
            page_session.status = 'failed'
            page_session.error_type = error_type
            page_session.error_summary = error_summary
            page_session.last_error = str(exc)
            append_log(page_session.logs, 'error', 'Page failed', error=str(exc), error_type=error_type)
            if page and not page.is_closed():
                page.close()
            if token:
                self.browser_manager.unregister_tab(token)
            page_session.browser_tab_id = None
            page_session.tab_focus_token = None

        data = page_session.to_dict()
        self.persistence_manager.save_page(run_id, tag_session['tag_id'], data)
        return data
