import hashlib
import threading
from pathlib import Path
from typing import Any, Dict
from utils.json_utils import load_json, save_json


class DecisionCache:
    def __init__(self, path: Path):
        self.path = path
        self._lock = threading.RLock()
        self.data = load_json(path, {})

    @staticmethod
    def make_key(mode: str, strictness: str, tag_title: str, product_title: str) -> str:
        raw = f"{mode}|{strictness}|{tag_title.strip().lower()}|{product_title.strip().lower()}"
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def get(self, key: str) -> Dict[str, Any] | None:
        with self._lock:
            return self.data.get(key)

    def set(self, key: str, value: Dict[str, Any]) -> None:
        with self._lock:
            self.data[key] = value
            save_json(self.path, self.data)
