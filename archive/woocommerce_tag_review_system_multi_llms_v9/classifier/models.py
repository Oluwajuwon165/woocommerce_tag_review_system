from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


@dataclass
class AssessmentRecord:
    assessment_id: str
    tag_id: str
    page_number: int
    product_title: str
    decision: str
    confidence: float
    source: str
    checkbox_ticked: bool
    timestamp: str
    reason: str = ""
    provider: str = ""
    model: str = ""
    tier_label: str = ""
    product_id: str = ""
    row_dom_id: str = ""
    result_status: str = "success"
    reused_from_previous_run: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PageSession:
    page_number: int
    page_url: str
    status: str = "queued"
    total_rows: int = 0
    candidate_rows: int = 0
    assessed_rows: int = 0
    skipped_rows_count: int = 0
    helper_rows_count: int = 0
    duplicate_rows_count: int = 0
    carried_forward_count: int = 0
    keep_count: int = 0
    mark_count: int = 0
    review_count: int = 0
    checkbox_count: int = 0
    cache_hit_count: int = 0
    llm_call_count: int = 0
    fallback_count: int = 0
    parse_failure_count: int = 0
    api_failure_count: int = 0
    preserved_tab: bool = False
    browser_tab_id: Optional[str] = None
    tab_focus_token: Optional[str] = None
    acp_bulk_ready: bool = False
    ready_for_human: bool = False
    assessments: List[Dict[str, Any]] = field(default_factory=list)
    row_diagnostics: List[Dict[str, Any]] = field(default_factory=list)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    error_summary: str = ""
    error_type: str = ""
    last_error: str = ""
    started_at: str = ""
    ended_at: str = ""
    duration_seconds: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class TagSession:
    tag_id: str
    run_id: str
    tag_title: str
    tag_url: str
    strictness: str
    max_pages: int
    dry_run: bool
    status: str = "queued"
    started_at: str = ""
    ended_at: str = ""
    duration_seconds: int = 0
    keep_count: int = 0
    mark_count: int = 0
    review_count: int = 0
    actionable_count: int = 0
    review_only_count: int = 0
    clean_count: int = 0
    failed_count: int = 0
    carried_forward_count: int = 0
    open_page_tabs: List[int] = field(default_factory=list)
    pages: List[Dict[str, Any]] = field(default_factory=list)
    logs: List[Dict[str, Any]] = field(default_factory=list)
    error_summary: str = ""
    error_type: str = ""
    last_error: str = ""

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RunSession:
    run_id: str
    status: str = "queued"
    created_at: str = ""
    started_at: str = ""
    ended_at: str = ""
    duration_seconds: int = 0
    total_tags: int = 0
    completed_tags: int = 0
    failed_tags: int = 0
    actionable_pages_count: int = 0
    browser_state: str = "not_started"
    logs: List[Dict[str, Any]] = field(default_factory=list)
    tags: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
