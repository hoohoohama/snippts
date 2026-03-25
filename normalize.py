from typing import Any

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from models import NormalizedSpan


def _hex_trace_id(trace_id_bytes: bytes) -> str:
    return trace_id_bytes.hex()


def _hex_span_id(span_id_bytes: bytes) -> str:
    return span_id_bytes.hex()


def _any_value_to_python(any_value) -> Any:
    which = any_value.WhichOneof("value")
    if which == "string_value":
        return any_value.string_value
    if which == "bool_value":
        return any_value.bool_value
    if which == "int_value":
        return any_value.int_value
    if which == "double_value":
        return any_value.double_value
    if which == "bytes_value":
        return any_value.bytes_value.hex()
    if which == "array_value":
        return [_any_value_to_python(v) for v in any_value.array_value.values]
    if which == "kvlist_value":
        return {kv.key: _any_value_to_python(kv.value) for kv in any_value.kvlist_value.values}
    return None


def _kv_list_to_dict(kv_list) -> dict[str, Any]:
    return {kv.key: _any_value_to_python(kv.value) for kv in kv_list}


def _span_kind_to_str(kind: int) -> str:
    mapping = {
        0: "SPAN_KIND_UNSPECIFIED",
        1: "INTERNAL",
        2: "SERVER",
        3: "CLIENT",
        4: "PRODUCER",
        5: "CONSUMER",
    }
    return mapping.get(kind, "UNKNOWN")


def _status_code_to_str(code: int) -> str:
    mapping = {
        0: "UNSET",
        1: "OK",
        2: "ERROR",
    }
    return mapping.get(code, "UNKNOWN")


def normalize_export_request(
    export_request: ExportTraceServiceRequest,
    project_name: str,
) -> list[NormalizedSpan]:
    normalized: list[NormalizedSpan] = []

    for resource_span in export_request.resource_spans:
        resource_attrs = _kv_list_to_dict(resource_span.resource.attributes)
        service_name = resource_attrs.get("service.name")
        schema_url = resource_span.schema_url or None

        for scope_span in resource_span.scope_spans:
            scope_name = scope_span.scope.name or None
            scope_version = scope_span.scope.version or None

            for span in scope_span.spans:
                span_attrs = _kv_list_to_dict(span.attributes)

                events = []
                for event in span.events:
                    events.append(
                        {
                            "name": event.name,
                            "time_unix_nano": event.time_unix_nano,
                            "attributes": _kv_list_to_dict(event.attributes),
                        }
                    )

                links = []
                for link in span.links:
                    links.append(
                        {
                            "trace_id": _hex_trace_id(link.trace_id),
                            "span_id": _hex_span_id(link.span_id),
                            "trace_state": link.trace_state or None,
                            "attributes": _kv_list_to_dict(link.attributes),
                        }
                    )

                normalized.append(
                    NormalizedSpan(
                        project_name=project_name,
                        trace_id=_hex_trace_id(span.trace_id),
                        span_id=_hex_span_id(span.span_id),
                        parent_span_id=_hex_span_id(span.parent_span_id) if span.parent_span_id else None,
                        name=span.name,
                        kind=_span_kind_to_str(span.kind),
                        start_time_unix_nano=span.start_time_unix_nano,
                        end_time_unix_nano=span.end_time_unix_nano,
                        status_code=_status_code_to_str(span.status.code) if span.HasField("status") else None,
                        status_message=span.status.message if span.HasField("status") else None,
                        service_name=service_name,
                        scope_name=scope_name,
                        scope_version=scope_version,
                        trace_state=span.trace_state or None,
                        attributes=span_attrs,
                        resource_attributes=resource_attrs,
                        events=events,
                        links=links,
                        raw_schema_url=schema_url,
                    )
                )

    return normalized