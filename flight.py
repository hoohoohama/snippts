import os
import re
from dataclasses import dataclass, asdict
from typing import Any, Dict, List, Optional

import mlflow
import pandas as pd


# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------

EXPERIMENT_NAME = "hosted-mlflow-flight-demo-structured"


# ------------------------------------------------------------------------------
# Fake flight backend
# ------------------------------------------------------------------------------

@dataclass
class FlightOption:
    airline: str
    flight_number: str
    price: int
    currency: str
    origin: str
    destination: str
    depart_date: str
    return_date: str
    cabin: str
    stops: int


FLIGHT_DB = [
    FlightOption(
        airline="Delta",
        flight_number="DL100",
        price=950,
        currency="USD",
        origin="NYC",
        destination="LON",
        depart_date="2026-05-10",
        return_date="2026-05-15",
        cabin="economy",
        stops=0,
    ),
    FlightOption(
        airline="United",
        flight_number="UA220",
        price=870,
        currency="USD",
        origin="NYC",
        destination="LON",
        depart_date="2026-05-10",
        return_date="2026-05-15",
        cabin="economy",
        stops=1,
    ),
    FlightOption(
        airline="Norse",
        flight_number="NO777",
        price=620,
        currency="USD",
        origin="NYC",
        destination="LON",
        depart_date="2026-05-10",
        return_date="2026-05-15",
        cabin="economy",
        stops=0,
    ),
    FlightOption(
        airline="BudgetAir",
        flight_number="BA800",
        price=780,
        currency="USD",
        origin="NYC",
        destination="LON",
        depart_date="2026-05-10",
        return_date="2026-05-15",
        cabin="economy",
        stops=1,
    ),
    FlightOption(
        airline="Alaska",
        flight_number="AS404",
        price=190,
        currency="USD",
        origin="NYC",
        destination="SFO",
        depart_date="2026-05-10",
        return_date="2026-05-15",
        cabin="economy",
        stops=0,
    ),
    FlightOption(
        airline="JetBlue",
        flight_number="B6202",
        price=410,
        currency="USD",
        origin="NYC",
        destination="SFO",
        depart_date="2026-05-10",
        return_date="2026-05-15",
        cabin="economy",
        stops=0,
    ),
]


# ------------------------------------------------------------------------------
# Parsing helpers
# ------------------------------------------------------------------------------

def extract_budget(text: str) -> Optional[int]:
    match = re.search(r"under\s*\$?(\d+)", text.lower())
    return int(match.group(1)) if match else None


def extract_dates(text: str) -> tuple[str, str]:
    lower = text.lower()
    if "may 10" in lower and "may 15" in lower:
        return ("2026-05-10", "2026-05-15")
    return ("2026-05-10", "2026-05-15")


def parse_trip_request(query: str) -> Dict[str, Any]:
    lower = query.lower()

    origin = "NYC" if ("from nyc" in lower or "nyc to" in lower) else "UNKNOWN"

    if "to london" in lower:
        destination = "LON"
    elif "to san francisco" in lower or "to sf" in lower or "to sfo" in lower:
        destination = "SFO"
    else:
        destination = "UNKNOWN"

    depart_date, return_date = extract_dates(query)
    budget = extract_budget(query)

    return {
        "intent": "book_round_trip_flight",
        "constraints": {
            "origin": origin,
            "destination": destination,
            "depart_date": depart_date,
            "return_date": return_date,
            "max_price": budget,
            "cabin": "economy",
        },
    }


# ------------------------------------------------------------------------------
# Structured tool calls
# ------------------------------------------------------------------------------

@mlflow.trace(name="tool.parse_request")
def tool_parse_request(query: str) -> Dict[str, Any]:
    parsed = parse_trip_request(query)
    return parsed


@mlflow.trace(name="tool.search_flights")
def tool_search_flights(
    origin: str,
    destination: str,
    depart_date: str,
    return_date: str,
    cabin: str = "economy",
) -> Dict[str, Any]:
    matches = [
        asdict(f)
        for f in FLIGHT_DB
        if f.origin == origin
        and f.destination == destination
        and f.depart_date == depart_date
        and f.return_date == return_date
        and f.cabin == cabin
    ]

    return {
        "query": {
            "origin": origin,
            "destination": destination,
            "depart_date": depart_date,
            "return_date": return_date,
            "cabin": cabin,
        },
        "result_count": len(matches),
        "results": matches,
    }


@mlflow.trace(name="tool.filter_by_budget")
def tool_filter_by_budget(
    search_payload: Dict[str, Any],
    max_price: Optional[int],
) -> Dict[str, Any]:
    candidates = search_payload["results"]
    if max_price is None:
        filtered = candidates
    else:
        filtered = [r for r in candidates if r["price"] <= max_price]

    return {
        "max_price": max_price,
        "input_count": len(candidates),
        "output_count": len(filtered),
        "results": filtered,
    }


@mlflow.trace(name="tool.select_flight_good")
def tool_select_flight_good(filtered_payload: Dict[str, Any]) -> Dict[str, Any]:
    results = filtered_payload["results"]
    if not results:
        return {
            "selected": None,
            "selection_reason": "no matching flight within constraints",
        }

    selected = sorted(results, key=lambda x: x["price"])[0]
    return {
        "selected": selected,
        "selection_reason": "lowest fare within constraints",
    }


@mlflow.trace(name="tool.select_flight_bad")
def tool_select_flight_bad(search_payload: Dict[str, Any]) -> Dict[str, Any]:
    """
    Deliberately ignores user budget and picks a preferred airline.
    """
    results = search_payload["results"]
    if not results:
        return {
            "selected": None,
            "selection_reason": "no results available",
        }

    preferred_order = ["Delta", "United", "JetBlue", "Alaska", "Norse", "BudgetAir"]
    selected = sorted(
        results,
        key=lambda x: preferred_order.index(x["airline"])
        if x["airline"] in preferred_order else 999
    )[0]

    return {
        "selected": selected,
        "selection_reason": "preferred full-service airline",
    }


@mlflow.trace(name="tool.compose_response")
def tool_compose_response(selection_payload: Dict[str, Any]) -> Dict[str, Any]:
    selected = selection_payload["selected"]
    if not selected:
        return {
            "status": "no_match",
            "message": "I couldn't find a flight matching your request.",
            "itinerary": None,
        }

    return {
        "status": "success",
        "message": (
            f"Found {selected['airline']} flight {selected['flight_number']} for "
            f"${selected['price']} from {selected['origin']} to {selected['destination']}."
        ),
        "itinerary": {
            "airline": selected["airline"],
            "flight_number": selected["flight_number"],
            "price": selected["price"],
            "currency": selected["currency"],
            "origin": selected["origin"],
            "destination": selected["destination"],
            "depart_date": selected["depart_date"],
            "return_date": selected["return_date"],
            "cabin": selected["cabin"],
            "stops": selected["stops"],
        },
    }


# ------------------------------------------------------------------------------
# Agents
# ------------------------------------------------------------------------------

@mlflow.trace(name="agent.flight_booking_good")
def flight_agent_good(inputs: Dict[str, Any]) -> Dict[str, Any]:
    query = inputs["query"]

    parsed = tool_parse_request(query)
    constraints = parsed["constraints"]

    search_payload = tool_search_flights(
        origin=constraints["origin"],
        destination=constraints["destination"],
        depart_date=constraints["depart_date"],
        return_date=constraints["return_date"],
        cabin=constraints["cabin"],
    )

    filtered_payload = tool_filter_by_budget(
        search_payload=search_payload,
        max_price=constraints["max_price"],
    )

    selection_payload = tool_select_flight_good(filtered_payload)
    response_payload = tool_compose_response(selection_payload)

    return response_payload


@mlflow.trace(name="agent.flight_booking_bad")
def flight_agent_bad(inputs: Dict[str, Any]) -> Dict[str, Any]:
    query = inputs["query"]

    parsed = tool_parse_request(query)
    constraints = parsed["constraints"]

    search_payload = tool_search_flights(
        origin=constraints["origin"],
        destination=constraints["destination"],
        depart_date=constraints["depart_date"],
        return_date=constraints["return_date"],
        cabin=constraints["cabin"],
    )

    # Intentionally skip budget filtering to create a bad trace
    selection_payload = tool_select_flight_bad(search_payload)
    response_payload = tool_compose_response(selection_payload)

    return response_payload


@mlflow.trace(name="agent.flight_booking_price_hallucination")
def flight_agent_price_hallucination(inputs: Dict[str, Any]) -> Dict[str, Any]:
    query = inputs["query"]

    parsed = tool_parse_request(query)
    constraints = parsed["constraints"]

    search_payload = tool_search_flights(
        origin=constraints["origin"],
        destination=constraints["destination"],
        depart_date=constraints["depart_date"],
        return_date=constraints["return_date"],
        cabin=constraints["cabin"],
    )

    filtered_payload = tool_filter_by_budget(
        search_payload=search_payload,
        max_price=constraints["max_price"],
    )
    selection_payload = tool_select_flight_good(filtered_payload)
    response_payload = tool_compose_response(selection_payload)

    # Deliberately corrupt the final reported price
    if response_payload["itinerary"] is not None:
        response_payload["itinerary"]["price"] -= 100
        response_payload["message"] = (
            f"Found {response_payload['itinerary']['airline']} flight "
            f"{response_payload['itinerary']['flight_number']} for "
            f"${response_payload['itinerary']['price']} from "
            f"{response_payload['itinerary']['origin']} to "
            f"{response_payload['itinerary']['destination']}."
        )

    return response_payload


# ------------------------------------------------------------------------------
# Evaluation scorers for structured outputs
# ------------------------------------------------------------------------------

@mlflow.genai.scorer(name="constraint_adherence")
def constraint_adherence(inputs, outputs, expectations=None):
    """
    Checks whether the structured itinerary price respects the user budget.
    """
    query = inputs["query"]
    budget = extract_budget(query)

    itinerary = outputs.get("itinerary")
    if budget is None or itinerary is None:
        return 0.0

    return 1.0 if itinerary["price"] <= budget else 0.0


@mlflow.genai.scorer(name="price_accuracy")
def price_accuracy(inputs, outputs, expectations=None):
    """
    Compares reported structured price to expected price.
    """
    itinerary = outputs.get("itinerary")
    if itinerary is None or not expectations:
        return 0.0

    expected_price = expectations[0].get("expected_price")
    if expected_price is None:
        return 0.0

    reported_price = itinerary["price"]
    if reported_price == expected_price:
        return 1.0

    delta = abs(reported_price - expected_price)
    return max(0.0, 1.0 - (delta / max(expected_price, 1)))


@mlflow.genai.scorer(name="flight_selection_correctness")
def flight_selection_correctness(inputs, outputs, expectations=None):
    """
    Checks whether the expected airline was selected.
    """
    itinerary = outputs.get("itinerary")
    if itinerary is None or not expectations:
        return 0.0

    expected_airline = expectations[0].get("expected_airline")
    if expected_airline is None:
        return 0.0

    return 1.0 if itinerary["airline"] == expected_airline else 0.0


# ------------------------------------------------------------------------------
# Evaluation dataset
# ------------------------------------------------------------------------------

def build_eval_data() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "inputs": {
                    "query": (
                        "Find me a round-trip flight from NYC to London, "
                        "leaving May 10 and returning May 15, under $800"
                    )
                },
                "expectations": [
                    {
                        "expected_airline": "Norse",
                        "expected_price": 620,
                    }
                ],
            },
            {
                "inputs": {
                    "query": (
                        "Find me a round-trip flight from NYC to San Francisco, "
                        "leaving May 10 and returning May 15, under $200"
                    )
                },
                "expectations": [
                    {
                        "expected_airline": "Alaska",
                        "expected_price": 190,
                    }
                ],
            },
        ]
    )


# ------------------------------------------------------------------------------
# Demo runners
# ------------------------------------------------------------------------------

def run_sample_traces() -> None:
    sample = {
        "query": (
            "Find me a round-trip flight from NYC to London, "
            "leaving May 10 and returning May 15, under $800"
        )
    }

    print("\n=== GOOD STRUCTURED TRACE ===")
    print(flight_agent_good(sample))

    print("\n=== BAD STRUCTURED TRACE ===")
    print(flight_agent_bad(sample))

    print("\n=== PRICE HALLUCINATION STRUCTURED TRACE ===")
    print(flight_agent_price_hallucination(sample))


def run_evaluation(agent_fn, run_name: str):
    eval_df = build_eval_data()

    with mlflow.start_run(run_name=run_name):
        mlflow.log_param("demo", "flight_booking_structured")
        mlflow.log_param("agent_fn", agent_fn.__name__)

        results = mlflow.genai.evaluate(
            data=eval_df,
            predict_fn=agent_fn,
            scorers=[
                constraint_adherence,
                price_accuracy,
                flight_selection_correctness,
            ],
        )

    return results


def main():
    mlflow.set_experiment(EXPERIMENT_NAME)

    run_sample_traces()

    print("\n=== EVAL: GOOD AGENT ===")
    print(run_evaluation(flight_agent_good, "eval_good_structured"))

    print("\n=== EVAL: BAD AGENT ===")
    print(run_evaluation(flight_agent_bad, "eval_bad_structured"))

    print("\n=== EVAL: PRICE HALLUCINATION AGENT ===")
    print(
        run_evaluation(
            flight_agent_price_hallucination,
            "eval_price_hallucination_structured",
        )
    )

    print("\nOpen MLflow UI and inspect:")
    print("1. Trace tree with structured tool outputs")
    print("2. Evaluation runs with aggregated metrics")
    print("3. Per-row outputs and scores")


if __name__ == "__main__":
    main()