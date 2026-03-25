from dataclasses import dataclass
from typing import Any, Optional


@dataclass
class IngestionIdentity:
    project_name: str


@dataclass
class NormalizedSpan:
    project_name: str
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]

    name: str
    kind: str
    start_time_unix_nano: int
    end_time_unix_nano: int

    status_code: Optional[str]
    status_message: Optional[str]

    service_name: Optional[str]
    scope_name: Optional[str]
    scope_version: Optional[str]
    trace_state: Optional[str]

    attributes: dict[str, Any]
    resource_attributes: dict[str, Any]
    events: list[dict[str, Any]]
    links: list[dict[str, Any]]

    raw_schema_url: Optional[str]