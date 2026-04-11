from typing import Any, Dict, List
from .time_utils import utc_now_iso


def make_log(level: str, message: str, **extra: Any) -> Dict[str, Any]:
    payload = {"timestamp": utc_now_iso(), "level": level.upper(), "message": message}
    if extra:
        payload["extra"] = extra
    return payload


def append_log(bucket: List[Dict[str, Any]], level: str, message: str, **extra: Any) -> Dict[str, Any]:
    entry = make_log(level, message, **extra)
    bucket.append(entry)
    return entry
