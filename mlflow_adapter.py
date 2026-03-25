from typing import Any

import orjson
from mlflow.entities import Trace, TraceData, TraceInfo, Span as MlflowSpan

from models import NormalizedSpan


def choose_root_span(spans: list[NormalizedSpan]) -> NormalizedSpan:
    root = next((s for s in spans if not s.parent_span_id), None)
    return root or spans[0]


def compute_trace_status(spans: list[NormalizedSpan]) -> str:
    if any(s.status_code == "ERROR" for s in spans):
        return "ERROR"
    if any(s.status_code == "OK" for s in spans):
        return "OK"
    return "UNSET"


def clean_mlflow_attributes(attributes: dict[str, Any]) -> tuple[dict[str, Any], Any, Any]:
    attrs = dict(attributes)
    span_inputs = attrs.pop("mlflow.spanInputs", None)
    span_outputs = attrs.pop("mlflow.spanOutputs", None)
    return attrs, span_inputs, span_outputs


def to_mlflow_span(normalized_span: NormalizedSpan) -> MlflowSpan:
    attrs, span_inputs, span_outputs = clean_mlflow_attributes(normalized_span.attributes)

    if span_inputs is not None:
        attrs["mlflow.spanInputs"] = span_inputs
    if span_outputs is not None:
        attrs["mlflow.spanOutputs"] = span_outputs

    return MlflowSpan(
        trace_id=normalized_span.trace_id,
        span_id=normalized_span.span_id,
        parent_id=normalized_span.parent_span_id,
        name=normalized_span.name,
        start_time_ns=normalized_span.start_time_unix_nano,
        end_time_ns=normalized_span.end_time_unix_nano,
        attributes=attrs,
        events=normalized_span.events,
        status_code=normalized_span.status_code,
        status_message=normalized_span.status_message,
    )


def build_trace_data(spans: list[NormalizedSpan]) -> TraceData:
    return TraceData(spans=[to_mlflow_span(s) for s in spans])


def build_trace_info(project_name: str, trace_id: str, spans: list[NormalizedSpan]) -> TraceInfo:
    root = choose_root_span(spans)
    start_ns = min(s.start_time_unix_nano for s in spans)
    end_ns = max(s.end_time_unix_nano for s in spans)

    return TraceInfo(
        trace_id=trace_id,
        request_id=trace_id,
        experiment_id=project_name,
        timestamp_ms=int(root.start_time_unix_nano / 1_000_000),
        execution_time_ms=int((end_ns - start_ns) / 1_000_000),
        status=compute_trace_status(spans),
    )


def build_mlflow_trace(project_name: str, trace_id: str, spans: list[NormalizedSpan]) -> Trace:
    return Trace(
        info=build_trace_info(project_name, trace_id, spans),
        data=build_trace_data(spans),
    )


def build_fallback_trace_json(project_name: str, trace_id: str, spans: list[NormalizedSpan]) -> str:
    root = choose_root_span(spans)
    payload = {
        "info": {
            "trace_id": trace_id,
            "request_id": trace_id,
            "experiment_id": project_name,
            "timestamp_ms": int(root.start_time_unix_nano / 1_000_000),
            "execution_time_ms": int(
                (max(s.end_time_unix_nano for s in spans) - min(s.start_time_unix_nano for s in spans)) / 1_000_000
            ),
            "status": compute_trace_status(spans),
        },
        "data": {
            "spans": [
                {
                    "trace_id": s.trace_id,
                    "span_id": s.span_id,
                    "parent_id": s.parent_span_id,
                    "name": s.name,
                    "start_time_ns": s.start_time_unix_nano,
                    "end_time_ns": s.end_time_unix_nano,
                    "attributes": s.attributes,
                    "events": s.events,
                    "status_code": s.status_code,
                    "status_message": s.status_message,
                }
                for s in sorted(spans, key=lambda x: (x.start_time_unix_nano, x.span_id))
            ]
        },
    }
    return orjson.dumps(payload).decode("utf-8")


def serialize_mlflow_trace(project_name: str, trace_id: str, spans: list[NormalizedSpan]) -> str:
    try:
        trace = build_mlflow_trace(project_name, trace_id, spans)
        return trace.to_json()
    except Exception:
        return build_fallback_trace_json(project_name, trace_id, spans)