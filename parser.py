from pyparsing import (
    Word, alphas, alphanums, nums, oneOf, infixNotation, opAssoc, Optional, Keyword
)

# Define basic elements
identifier = Word(alphas + "_", alphanums + "_")
number = Word(nums).setParseAction(lambda t: int(t[0]))
string = dblQuotedString.setParseAction(removeQuotes) | identifier
comparison_op = oneOf("= != > >= < <=")
logical_op = oneOf("AND OR")
field_name = (
    (oneOf("metrics params tags") + "." + identifier) |
    oneOf("status user_id experiment_id run_id start_time end_time")
)
order_direction = oneOf("ASC DESC")

# Define clauses
condition = Group(field_name + comparison_op + (number | string))
order_by_clause = Keyword("ORDER BY") + Group(field_name + Optional(order_direction, default="ASC"))
limit_clause = Keyword("LIMIT") + number

# Define the full grammar
expression = infixNotation(
    condition,
    [
        (logical_op, 2, opAssoc.LEFT),
    ],
)
query = Group(expression) + Optional(order_by_clause) + Optional(limit_clause)

def parse_filter_expression(expression_str):
    try:
        parsed = query.parseString(expression_str, parseAll=True)
        filter_ast = _build_ast(parsed[0])
        order_by = _parse_order_by(parsed[1]) if len(parsed) > 1 and "ORDER BY" in parsed[1:] else None
        limit = _parse_limit(parsed[-1]) if "LIMIT" in parsed else None
        return {
            "query": filter_ast.to_query(),
            "sort": order_by,
            "size": limit,
        }
    except Exception as e:
        raise ValueError(f"Invalid filter expression: {expression_str}") from e

def _build_ast(parsed_expr):
    if isinstance(parsed_expr, ParseResults):
        if len(parsed_expr) == 1:
            return _build_ast(parsed_expr[0])
        elif len(parsed_expr) == 3 and parsed_expr[1] in ["AND", "OR"]:
            return LogicalOperation(parsed_expr)
        else:
            return Condition(parsed_expr)
    else:
        return parsed_expr

def _parse_order_by(order_clause):
    field, direction = order_clause[1][0], order_clause[1][1]
    return [{field: {"order": direction.lower()}}]

def _parse_limit(limit_clause):
    return limit_clause[1]
    
if __name__ == "__main__":
    filter_expression = (
        'metrics.accuracy >= 0.9 AND params.model = "resnet" ORDER BY metrics.accuracy DESC LIMIT 10'
    )
    query = parse_filter_expression(filter_expression)
    print("Filter Expression:")
    print(filter_expression)
    print("\nGenerated OpenSearch Query:")
    print(json.dumps(query, indent=2))
    
    test_expressions = [
    'status = "FINISHED" ORDER BY start_time DESC LIMIT 5',
    'metrics.loss < 0.05 ORDER BY metrics.loss ASC LIMIT 20',
    'params.batch_size >= 32 AND params.learning_rate <= 0.01 LIMIT 50',
    'user_id != "user123" AND start_time >= "2023-10-01T00:00:00Z" ORDER BY end_time DESC'
]

for expr in test_expressions:
    query = parse_filter_expression(expr)
    print(f"Filter Expression: {expr}")
    print("Generated OpenSearch Query:")
    print(json.dumps(query, indent=2))
    print("-" * 80)
    
    