import queue
import threading
import uuid
from typing import Any, Dict, List

from classifier.models import RunSession
from utils.logging_utils import append_log
from utils.time_utils import elapsed_seconds, utc_now_iso


class RunManager:
    def __init__(self, config: Dict[str, Any], browser_manager, tag_manager, persistence_manager, tag_extraction_manager=None, replay_browser_manager=None):
        self.config = config
        self.browser_manager = browser_manager
        self.tag_manager = tag_manager
        self.persistence_manager = persistence_manager
        self.tag_extraction_manager = tag_extraction_manager
        self.replay_browser_manager = replay_browser_manager
        self._queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self._browser_queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self._worker_started = False
        self._browser_worker_started = False
        self._worker_lock = threading.Lock()
        self._browser_worker_lock = threading.Lock()
        self._ensure_worker()
        self._ensure_browser_worker()

    def _ensure_worker(self) -> None:
        with self._worker_lock:
            if self._worker_started:
                return
            thread = threading.Thread(target=self._worker_loop, daemon=True, name='run-manager-worker')
            thread.start()
            self._worker_started = True

    def _ensure_browser_worker(self) -> None:
        with self._browser_worker_lock:
            if self._browser_worker_started:
                return
            thread = threading.Thread(target=self._browser_worker_loop, daemon=True, name='browser-manager-worker')
            thread.start()
            self._browser_worker_started = True

    def _worker_loop(self) -> None:
        while True:
            job = self._queue.get()
            try:
                kind = job.get('kind', 'run') if isinstance(job, dict) else 'run'
                if kind == 'run':
                    self._execute_run(job['payload'])
                elif kind == 'extract':
                    if not self.tag_extraction_manager:
                        raise RuntimeError('Tag extraction manager is not configured.')
                    self.tag_extraction_manager.execute_extraction(job['payload'])
                elif kind == 'call':
                    callback = job['callback']
                    args = job.get('args', ())
                    kwargs = job.get('kwargs', {})
                    result_queue = job['result_queue']
                    try:
                        result_queue.put((True, callback(*args, **kwargs)))
                    except Exception as exc:
                        result_queue.put((False, exc))
                else:
                    raise ValueError(f"Unknown worker job kind: {kind}")
            finally:
                self._queue.task_done()

    def _browser_worker_loop(self) -> None:
        while True:
            job = self._browser_queue.get()
            try:
                kind = job.get('kind', 'call') if isinstance(job, dict) else 'call'
                if kind == 'call':
                    callback = job['callback']
                    args = job.get('args', ())
                    kwargs = job.get('kwargs', {})
                    result_queue = job.get('result_queue')
                    try:
                        result = callback(*args, **kwargs)
                        if result_queue is not None:
                            result_queue.put((True, result))
                    except Exception as exc:
                        if result_queue is not None:
                            result_queue.put((False, exc))
                else:
                    raise ValueError(f"Unknown browser worker job kind: {kind}")
            finally:
                self._browser_queue.task_done()

    def parse_tag_lines(self, text: str, strictness: str, max_pages: int, dry_run: bool, run_mode: str = 'full_tag', execution_mode: str = 'full_review') -> List[Dict[str, Any]]:
        run_mode = (run_mode or 'full_tag').strip().lower()
        if run_mode not in {'full_tag', 'per_page'}:
            raise ValueError('run_mode is invalid')
        execution_mode = (execution_mode or 'full_review').strip().lower()
        if execution_mode not in {'full_review', 'actionable_review', 'assessment_only'}:
            raise ValueError('execution_mode is invalid')
        tags = []
        for line in [ln.strip() for ln in text.splitlines() if ln.strip()]:
            if '|' not in line:
                raise ValueError(f"Invalid tag line (expected 'Title | URL'): {line}")
            title, url = [part.strip() for part in line.split('|', 1)]
            if not title:
                raise ValueError('Tag title exists validation failed.')
            if not url:
                raise ValueError('Tag URL exists validation failed.')
            tags.append({
                'tag_id': uuid.uuid4().hex[:10],
                'tag_title': title,
                'tag_url': url,
                'strictness': strictness,
                'max_pages': int(max_pages),
                'dry_run': bool(dry_run),
                'run_mode': run_mode,
                'execution_mode': execution_mode,
            })
        if not tags:
            raise ValueError('At least one tag is required.')
        return tags

    def start_extraction(self, extraction: Dict[str, Any]) -> Dict[str, Any]:
        self._ensure_worker()
        self._queue.put({'kind': 'extract', 'payload': extraction})
        return extraction

    def start_run(self, tags: List[Dict[str, Any]]) -> Dict[str, Any]:
        self._ensure_worker()
        run = RunSession(
            run_id=uuid.uuid4().hex[:12],
            status='queued',
            created_at=utc_now_iso(),
            total_tags=len(tags),
            tags=tags,
            execution_mode=(tags[0].get('execution_mode') if tags else 'full_review'),
        )
        run_data = run.to_dict()
        append_log(run_data['logs'], 'info', 'Run queued')
        self.persistence_manager.save_run(run_data)
        self._queue.put({'kind': 'run', 'payload': run_data})
        return run_data

    def call_in_worker(self, callback, *args, **kwargs):
        self._ensure_worker()
        result_queue: "queue.Queue[Any]" = queue.Queue(maxsize=1)
        self._queue.put({
            'kind': 'call',
            'callback': callback,
            'args': args,
            'kwargs': kwargs,
            'result_queue': result_queue,
        })
        ok, payload = result_queue.get()
        if ok:
            return payload
        raise payload

    def call_in_replay_worker(self, callback, *args, wait: bool = True, **kwargs):
        self._ensure_browser_worker()
        result_queue = queue.Queue(maxsize=1) if wait else None
        self._browser_queue.put({
            'kind': 'call',
            'callback': callback,
            'args': args,
            'kwargs': kwargs,
            'result_queue': result_queue,
        })
        if not wait:
            return None
        ok, payload = result_queue.get()
        if ok:
            return payload
        raise payload

    def warmup_replay_browser_async(self) -> None:
        if not self.replay_browser_manager:
            return
        self.call_in_replay_worker(self.replay_browser_manager.login_once, wait=False)

    def _execute_run(self, run_data: Dict[str, Any]) -> None:
        run_data['status'] = 'running'
        run_data['started_at'] = utc_now_iso()
        append_log(run_data['logs'], 'info', 'Run started')
        self.persistence_manager.save_run(run_data)
        try:
            if self._run_requires_browser(run_data):
                self.browser_manager.login_once()
                run_data['browser_state'] = self.browser_manager.browser_state()
                append_log(run_data['logs'], 'info', 'Browser launched and login successful')
            else:
                run_data['browser_state'] = self.browser_manager.browser_state()
                append_log(run_data['logs'], 'info', 'Browser deferred for assessment-only API mode')
                self.warmup_replay_browser_async()
            completed = 0
            failed = 0
            actionable_pages = 0
            processed_tags = []
            for tag in run_data['tags']:
                result = self.tag_manager.process_tag(run_data['run_id'], tag)
                processed_tags.append(result)
                if result['status'] == 'failed':
                    failed += 1
                else:
                    completed += 1
                actionable_pages += result.get('actionable_count', 0)
                run_data['completed_tags'] = completed
                run_data['failed_tags'] = failed
                run_data['actionable_pages_count'] = actionable_pages
                run_data['tags'] = processed_tags + [t for t in run_data['tags'] if t.get('tag_id') not in {r.get('tag_id') for r in processed_tags}]
                run_data['duration_seconds'] = elapsed_seconds(run_data.get('started_at'), None)
                self.persistence_manager.save_run(run_data)

            run_data['tags'] = processed_tags
            run_data['ended_at'] = utc_now_iso()
            run_data['duration_seconds'] = elapsed_seconds(run_data.get('started_at'), run_data.get('ended_at'))
            run_data['browser_state'] = self.browser_manager.browser_state()
            run_data['status'] = 'completed_waiting_for_human' if any(t.get('open_page_tabs') for t in processed_tags) else ('failed' if failed == len(processed_tags) else 'completed')
            append_log(run_data['logs'], 'info', 'Run completed', status=run_data['status'], duration_seconds=run_data['duration_seconds'])
            self.persistence_manager.save_run(run_data)
        except Exception as exc:
            run_data['ended_at'] = utc_now_iso()
            run_data['duration_seconds'] = elapsed_seconds(run_data.get('started_at'), run_data.get('ended_at'))
            run_data['status'] = 'failed'
            append_log(run_data['logs'], 'error', 'Run failed', error=str(exc))
            self.persistence_manager.save_run(run_data)

    def _run_requires_browser(self, run_data: Dict[str, Any]) -> bool:
        tags = run_data.get('tags') or []
        if not tags:
            return False
        all_assessment_only = all(str((tag or {}).get('execution_mode') or '').strip().lower() == 'assessment_only' for tag in tags)
        if all_assessment_only and getattr(self.tag_manager, '_api_mode_enabled', lambda: False)():
            return False
        return True

    def _carry_payload(self, tag: Dict[str, Any]) -> Dict[str, Any]:
        return {
            'tag_id': uuid.uuid4().hex[:10],
            'tag_title': tag['tag_title'],
            'tag_url': tag['tag_url'],
            'strictness': tag['strictness'],
            'max_pages': tag['max_pages'],
            'dry_run': tag['dry_run'],
            'run_mode': tag.get('run_mode', 'full_tag'),
            'execution_mode': tag.get('execution_mode', 'full_review'),
            'carry_forward_source': {
                'run_id': tag.get('run_id'),
                'tag_id': tag.get('tag_id'),
            },
        }

    def rerun_failed_tags(self, run_data: Dict[str, Any]) -> Dict[str, Any]:
        failed_tags = [self._carry_payload(tag) for tag in run_data.get('tags', []) if tag.get('status') == 'failed']
        if not failed_tags:
            raise ValueError('No failed tags found to rerun.')
        return self.start_run(failed_tags)

    def rerun_single_tag(self, tag: Dict[str, Any]) -> Dict[str, Any]:
        payload = [self._carry_payload(tag)]
        return self.start_run(payload)
