from datetime import datetime, timezone


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(value: str | None):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except Exception:
        return None


def elapsed_seconds(started_at: str | None, ended_at: str | None = None) -> int:
    start = parse_iso(started_at)
    end = parse_iso(ended_at) if ended_at else datetime.now(timezone.utc)
    if not start:
        return 0
    return max(0, int((end - start).total_seconds()))


def format_duration(total_seconds: int) -> str:
    total_seconds = max(0, int(total_seconds or 0))
    hours, rem = divmod(total_seconds, 3600)
    minutes, seconds = divmod(rem, 60)
    if hours:
        return f"{hours}h {minutes}m {seconds}s"
    if minutes:
        return f"{minutes}m {seconds}s"
    return f"{seconds}s"
