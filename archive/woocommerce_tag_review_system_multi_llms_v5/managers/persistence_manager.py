from pathlib import Path
from typing import Any, Dict, List
from utils.json_utils import load_json, save_json


class PersistenceManager:
    def __init__(self, root: Path):
        self.root = root
        self.active_runs_path = root / 'data' / 'active_runs.json'
        self.runs_root = root / 'runs'
        self.runs_root.mkdir(parents=True, exist_ok=True)
        save_json(self.active_runs_path, load_json(self.active_runs_path, []))

    def run_dir(self, run_id: str) -> Path:
        d = self.runs_root / f'run_{run_id}'
        d.mkdir(parents=True, exist_ok=True)
        return d

    def tag_dir(self, run_id: str, tag_id: str) -> Path:
        d = self.run_dir(run_id) / 'tags'
        d.mkdir(parents=True, exist_ok=True)
        return d

    def page_dir(self, run_id: str, tag_id: str) -> Path:
        d = self.tag_dir(run_id, tag_id) / f'tag_{tag_id}_pages'
        d.mkdir(parents=True, exist_ok=True)
        return d

    def save_run(self, run_data: Dict[str, Any]) -> None:
        run_dir = self.run_dir(run_data['run_id'])
        save_json(run_dir / 'run_summary.json', run_data)
        save_json(run_dir / 'logs.json', run_data.get('logs', []))
        runs = load_json(self.active_runs_path, [])
        runs = [r for r in runs if r.get('run_id') != run_data['run_id']]
        runs.insert(0, {
            'run_id': run_data['run_id'],
            'status': run_data.get('status'),
            'total_tags': run_data.get('total_tags', 0),
            'actionable_pages_count': run_data.get('actionable_pages_count', 0),
            'started_at': run_data.get('started_at'),
            'created_at': run_data.get('created_at'),
            'ended_at': run_data.get('ended_at'),
            'duration_seconds': run_data.get('duration_seconds', 0),
        })
        save_json(self.active_runs_path, runs[:50])

    def save_tag(self, run_id: str, tag_data: Dict[str, Any]) -> None:
        save_json(self.tag_dir(run_id, tag_data['tag_id']) / f"tag_{tag_data['tag_id']}.json", tag_data)

    def save_page(self, run_id: str, tag_id: str, page_data: Dict[str, Any]) -> None:
        save_json(self.page_dir(run_id, tag_id) / f"page_{page_data['page_number']}.json", page_data)

    def list_runs(self) -> List[Dict[str, Any]]:
        return load_json(self.active_runs_path, [])

    def load_all_tags(self, run_id: str) -> List[Dict[str, Any]]:
        tags_dir = self.run_dir(run_id) / 'tags'
        if not tags_dir.exists():
            return []
        tag_files = sorted([p for p in tags_dir.glob('tag_*.json') if p.is_file()])
        return [load_json(p, {}) for p in tag_files]

    def load_all_pages(self, run_id: str, tag_id: str) -> List[Dict[str, Any]]:
        pages_dir = self.page_dir(run_id, tag_id)
        if not pages_dir.exists():
            return []
        page_files = sorted([p for p in pages_dir.glob('page_*.json') if p.is_file()])
        return [load_json(p, {}) for p in page_files]

    def load_run(self, run_id: str) -> Dict[str, Any]:
        run = load_json(self.run_dir(run_id) / 'run_summary.json', {})
        if not run:
            return {}
        stored_tags = self.load_all_tags(run_id)
        if stored_tags:
            by_id = {t.get('tag_id'): t for t in stored_tags if t.get('tag_id')}
            merged = []
            for tag in run.get('tags', []):
                merged.append(by_id.get(tag.get('tag_id'), tag))
            extras = [t for tid, t in by_id.items() if tid not in {x.get('tag_id') for x in run.get('tags', [])}]
            run['tags'] = merged + extras
            run['completed_tags'] = sum(1 for t in run['tags'] if t.get('status') not in {'failed', 'queued', 'running'})
            run['failed_tags'] = sum(1 for t in run['tags'] if t.get('status') == 'failed')
            run['actionable_pages_count'] = sum(int(t.get('actionable_count', 0) or 0) for t in run['tags'])
        return run

    def load_tag(self, run_id: str, tag_id: str) -> Dict[str, Any]:
        tag = load_json(self.tag_dir(run_id, tag_id) / f'tag_{tag_id}.json', {})
        if tag:
            tag['pages'] = self.load_all_pages(run_id, tag_id) or tag.get('pages', [])
        return tag
