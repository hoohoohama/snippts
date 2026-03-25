import hashlib
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

import boto3

from config import settings
from mlflow_adapter import choose_root_span, clean_mlflow_attributes, compute_trace_status, serialize_mlflow_trace
from models import NormalizedSpan


dynamodb = boto3.resource("dynamodb")
trace_table = dynamodb.Table(settings.DYNAMODB_TABLE_NAME)
s3 = boto3.client("s3")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def trace_pk(project_name: str, trace_id: str) -> str:
    return f"TRACE#{project_name}#{trace_id}"


def trace_info_sk() -> str:
    return "TRACEINFO"


def span_sk(span_id: str) -> str:
    return f"SPAN#{span_id}"


def s3_trace_key(project_name: str, trace_id: str) -> str:
    return f"projects/{project_name}/traces/{trace_id}.json"


def bounded_list(items: list[Any] | None, max_items: int) -> list[Any]:
    return (items or [])[:max_items]


def bounded_dict(d: dict[str, Any] | None, max_entries: int) -> dict[str, Any]:
    if not d:
        return {}
    out: dict[str, Any] = {}
    for i, (k, v) in enumerate(d.items()):
        if i >= max_entries:
            break
        out[k] = v
    return out


def trace_info_item(
    project_name: str,
    trace_id: str,
    spans: list[NormalizedSpan],
    s3_key: str,
    payload_sha256: str,
) -> dict[str, Any]:
    root = choose_root_span(spans)
    start_time = min(s.start_time_unix_nano for s in spans)
    end_time = max(s.end_time_unix_nano for s in spans)

    return {
        "pk": trace_pk(project_name, trace_id),
        "sk": trace_info_sk(),
        "entity_type": "TraceInfo",
        "project_name": project_name,
        "trace_id": trace_id,
        "root_span_id": root.span_id,
        "root_span_name": root.name,
        "service_name": root.service_name,
        "start_time_unix_nano": str(start_time),
        "end_time_unix_nano": str(end_time),
        "span_count": len(spans),
        "status": compute_trace_status(spans),
        "trace_json_s3_bucket": settings.S3_BUCKET_NAME,
        "trace_json_s3_key": s3_key,
        "trace_json_sha256": payload_sha256,
        "trace_json_format": settings.TRACE_JSON_FORMAT,
        "ingestion_source": "otlp_http",
        "created_at": now_iso(),
        "updated_at": now_iso(),
    }


def span_item(project_name: str, trace_id: str, span: NormalizedSpan) -> dict[str, Any]:
    attrs_for_ddb, _, _ = clean_mlflow_attributes(span.attributes)

    return {
        "pk": trace_pk(project_name, trace_id),
        "sk": span_sk(span.span_id),
        "entity_type": "Span",
        "project_name": project_name,
        "trace_id": trace_id,
        "span_id": span.span_id,
        "parent_id": span.parent_span_id,
        "name": span.name,
        "kind": span.kind,
        "status_code": span.status_code,
        "status_message": span.status_message,
        "service_name": span.service_name,
        "scope_name": span.scope_name,
        "scope_version": span.scope_version,
        "start_time_unix_nano": str(span.start_time_unix_nano),
        "end_time_unix_nano": str(span.end_time_unix_nano),
        "attributes": bounded_dict(attrs_for_ddb, settings.MAX_ATTRS_PER_SPAN),
        "resource_attributes": bounded_dict(span.resource_attributes, settings.MAX_RESOURCE_ATTRS_PER_SPAN),
        "events": bounded_list(span.events, settings.MAX_EVENTS_PER_SPAN),
        "links": bounded_list(span.links, settings.MAX_LINKS_PER_SPAN),
        "ingestion_source": "otlp_http",
        "updated_at": now_iso(),
    }


class BackendWriter:
    async def write_spans(self, spans: list[NormalizedSpan]) -> None:
        if not spans:
            return

        spans_by_trace: dict[tuple[str, str], list[NormalizedSpan]] = defaultdict(list)
        for span in spans:
            spans_by_trace[(span.project_name, span.trace_id)].append(span)

        for (project_name, trace_id), trace_spans in spans_by_trace.items():
            trace_json = serialize_mlflow_trace(project_name, trace_id, trace_spans)
            trace_bytes = trace_json.encode("utf-8")
            trace_s3_key = s3_trace_key(project_name, trace_id)
            payload_sha = sha256_hex(trace_bytes)

            s3.put_object(
                Bucket=settings.S3_BUCKET_NAME,
                Key=trace_s3_key,
                Body=trace_bytes,
                ContentType="application/json",
                Metadata={
                    "project_name": project_name,
                    "trace_id": trace_id,
                    "payload_sha256": payload_sha,
                },
            )

            trace_table.put_item(
                Item=trace_info_item(
                    project_name=project_name,
                    trace_id=trace_id,
                    spans=trace_spans,
                    s3_key=trace_s3_key,
                    payload_sha256=payload_sha,
                )
            )

            with trace_table.batch_writer(overwrite_by_pkeys=["pk", "sk"]) as batch:
                for span in trace_spans:
                    batch.put_item(Item=span_item(project_name, trace_id, span))