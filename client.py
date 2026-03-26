import json
import random
import time
from typing import Any

from opentelemetry import trace
from opentelemetry.trace import SpanKind, Status, StatusCode
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter


def j(value: Any) -> str:
    return json.dumps(value, separators=(",", ":"))


# ============================================================
# OTLP exporter config
# ============================================================

exporter = OTLPSpanExporter(
    endpoint="http://localhost:4318/v1/traces",
    headers={
        "x-project-name": "demo-agent-project",
        "x-ingest-key": "super-secret",
    },
)

provider = TracerProvider(
    resource=Resource.create(
        {
            "service.name": "demo-agent",
            "service.version": "1.0.0",
            "deployment.environment": "local",
        }
    )
)

provider.add_span_processor(BatchSpanProcessor(exporter))
trace.set_tracer_provider(provider)
tracer = trace.get_tracer("demo.agent")


# ============================================================
# Fake agent pieces
# ============================================================

def fake_planner(user_query: str) -> dict[str, Any]:
    time.sleep(0.15)
    return {
        "goal": "answer user question",
        "steps": [
            "understand request",
            "call weather tool",
            "summarize result",
        ],
        "query": user_query,
    }


def fake_weather_tool(city: str) -> dict[str, Any]:
    time.sleep(0.25)
    return {
        "city": city,
        "forecast": "sunny",
        "temperature_f": random.choice([68, 70, 72, 74]),
    }


def fake_model_response(plan: dict[str, Any], tool_result: dict[str, Any]) -> str:
    time.sleep(0.30)
    return (
        f"The weather in {tool_result['city']} looks {tool_result['forecast']} "
        f"at about {tool_result['temperature_f']}°F. "
        f"Plan steps were: {', '.join(plan['steps'])}."
    )


# ============================================================
# Agent execution
# ============================================================

def run_agent(user_query: str, city: str) -> str:
    with tracer.start_as_current_span("agent.run", kind=SpanKind.INTERNAL) as root:
        root.set_attribute("mlflow.spanType", "AGENT")
        root.set_attribute("agent.name", "demo-weather-agent")
        root.set_attribute("agent.framework", "custom")
        root.set_attribute("user.query", user_query)
        root.set_attribute(
            "mlflow.spanInputs",
            j(
                {
                    "messages": [
                        {"role": "user", "content": user_query},
                    ],
                    "city": city,
                }
            ),
        )

        try:
            with tracer.start_as_current_span("agent.plan", kind=SpanKind.INTERNAL) as plan_span:
                plan_span.set_attribute("mlflow.spanType", "CHAIN")
                plan_span.set_attribute("planner.name", "fake_planner")

                plan = fake_planner(user_query)

                plan_span.set_attribute("planner.step_count", len(plan["steps"]))
                plan_span.add_event(
                    "planning.complete",
                    {"goal": plan["goal"]},
                )

            with tracer.start_as_current_span("tool.weather_lookup", kind=SpanKind.CLIENT) as tool_span:
                tool_span.set_attribute("mlflow.spanType", "TOOL")
                tool_span.set_attribute("tool.name", "weather_lookup")
                tool_span.set_attribute("tool.city", city)
                tool_span.set_attribute(
                    "mlflow.spanInputs",
                    j({"city": city}),
                )

                tool_result = fake_weather_tool(city)

                tool_span.set_attribute(
                    "mlflow.spanOutputs",
                    j(tool_result),
                )
                tool_span.add_event(
                    "tool.result.received",
                    {
                        "city": tool_result["city"],
                        "forecast": tool_result["forecast"],
                    },
                )

            with tracer.start_as_current_span("model.generate", kind=SpanKind.CLIENT) as llm_span:
                llm_span.set_attribute("mlflow.spanType", "LLM")
                llm_span.set_attribute("model.provider", "fake")
                llm_span.set_attribute("model.name", "demo-model-v1")
                llm_span.set_attribute(
                    "mlflow.spanInputs",
                    j(
                        {
                            "plan": plan,
                            "tool_result": tool_result,
                        }
                    ),
                )

                answer = fake_model_response(plan, tool_result)

                llm_span.set_attribute(
                    "mlflow.spanOutputs",
                    j({"answer": answer}),
                )
                llm_span.add_event(
                    "generation.complete",
                    {"output_length": len(answer)},
                )

            root.set_attribute(
                "mlflow.spanOutputs",
                j({"final_answer": answer}),
            )
            root.set_status(Status(StatusCode.OK))
            return answer

        except Exception as exc:
            root.record_exception(exc)
            root.set_status(Status(StatusCode.ERROR, str(exc)))
            raise


if __name__ == "__main__":
    result = run_agent(
        user_query="What is the weather like today?",
        city="New York",
    )
    print("Agent answer:")
    print(result)

    # Give the batch exporter time to flush before process exit.
    time.sleep(2)