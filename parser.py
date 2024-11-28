import json
from opensearchpy import OpenSearch, helpers
from pyparsing import (
    Word, alphas, alphanums, nums, oneOf, opAssoc, infixNotation,
    Keyword, Literal, CaselessKeyword, Group, ParserElement, quotedString,
    removeQuotes, Regex
)

# Enable packrat parsing for speed
ParserElement.enablePackrat()

# Initialize OpenSearch client
client = OpenSearch(
    hosts=[{'host': 'localhost', 'port': 9200}],
    http_compress=True,
    timeout=30,
    max_retries=3,
    retry_on_timeout=True
)

# Define valid top-level fields
VALID_TOP_LEVEL_FIELDS = {
    'run_id',
    'experiment_id',
    'user_id',
    'status',
    'start_time',
    'end_time'
}

# Define comparison operators
comparison_ops = oneOf("= != > >= < <= eq ne gt ge lt le", caseless=True)

# Define logical operators
and_ = CaselessKeyword("and")
or_ = CaselessKeyword("or")
not_ = CaselessKeyword("not")

# Define field types
field_type = oneOf("metrics params tags attributes", caseless=True)

# Define identifier (e.g., 'accuracy', 'optimizer')
identifier = Word(alphas + "_", alphanums + "_.:")

# Define numeric value (as string)
number = Regex(r'\d+(\.\d*)?([eE][+-]?\d+)?')

# Define string value
string = quotedString.setParseAction(removeQuotes)

# Define value (number or string)
value = number | string

# Define field, which can be either field_type.identifier or just identifier
field = (
    Group(field_type + Literal('.').suppress() + identifier)('field_nested') |
    identifier('field_top')
)

# Define comparison expression (removed Group)
comparison_expr = (
    field +
    comparison_ops('op') +
    value('value')
)

# Define the full expression grammar
bool_expr = infixNotation(
    comparison_expr,
    [
        (not_, 1, opAssoc.RIGHT),
        (and_, 2, opAssoc.LEFT),
        (or_, 2, opAssoc.LEFT),
    ],
)

def parse_filter_expression(filter_expr):
    try:
        parsed_expr = bool_expr.parseString(filter_expr, parseAll=True)[0]
        # print("Parsed Expression:", parsed_expr)
        return parsed_expr
    except Exception as e:
        raise ValueError(f"Error parsing filter expression: {e}")

def ast_to_query(ast):
    # print("AST Node:", ast)
    if isinstance(ast, str):
        # Should not reach here in correct parsing
        return {}
    elif isinstance(ast, list):
        if len(ast) == 1:
            # Directly process the single element
            return ast_to_query(ast[0])
        elif len(ast) == 2:
            # NOT expression (unary operator)
            op, operand = ast
            if op.lower() == 'not':
                return {
                    "bool": {
                        "must_not": [
                            ast_to_query(operand)
                        ]
                    }
                }
            else:
                raise ValueError(f"Unknown operator '{op}' in expression.")
        elif len(ast) == 3:
            left, op, right = ast
            if isinstance(op, str) and op.lower() in ('and', 'or'):
                left_query = ast_to_query(left)
                right_query = ast_to_query(right)
                if op.lower() == 'and':
                    return {
                        "bool": {
                            "filter": [
                                left_query,
                                right_query
                            ]
                        }
                    }
                elif op.lower() == 'or':
                    return {
                        "bool": {
                            "should": [
                                left_query,
                                right_query
                            ],
                            "minimum_should_match": 1
                        }
                    }
            else:
                # This is a comparison expression
                return comparison_to_query(ast)
        else:
            raise ValueError("Invalid expression structure.")
    else:
        # This is a comparison expression
        return comparison_to_query(ast)

def comparison_to_query(tokens):
    operator = tokens.op
    value = tokens.value[0]  # Extract the string value

    # Check if it's a nested field or a top-level field
    if 'field_nested' in tokens:
        field_info = tokens.field_nested
        field_type = field_info[0].lower()
        key = field_info[1]
        
        path = field_type
        key_field = f"{field_type}.key"
        
        # Map operators to OpenSearch equivalents
        operator_map = {
            '=': 'term',
            'eq': 'term',
            '!=': 'must_not',
            'ne': 'must_not',
            '>': 'gt',
            'gt': 'gt',
            '>=': 'gte',
            'ge': 'gte',
            '<': 'lt',
            'lt': 'lt',
            '<=': 'lte',
            'le': 'lte'
        }
        
        range_operators = {'>', 'gt', '>=', 'ge', '<', 'lt', '<=', 'le'}
        equality_operators = {'=', 'eq', '!=', 'ne'}
        
        if operator.lower() in equality_operators:
            value_field = f"{field_type}.value"
            
            term_query = {
                "nested": {
                    "path": path,
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {key_field: key}},
                                {"term": {value_field: value}}
                            ]
                        }
                    }
                }
            }
            
            if operator.lower() in ['!=', 'ne']:
                return {"bool": {"must_not": [term_query]}}
            else:
                return term_query
        
        elif operator.lower() in range_operators:
            # Attempt to parse value as float
            try:
                numeric_value = float(value)
            except ValueError:
                raise ValueError(f"Range operator '{operator}' requires a numeric value for field '{field_type}.{key}'.")
            
            value_field = f"{field_type}.value.double"
            range_op = operator_map[operator.lower()]
            
            range_query = {
                "nested": {
                    "path": path,
                    "query": {
                        "bool": {
                            "filter": [
                                {"term": {key_field: key}},
                                {"range": {value_field: {range_op: numeric_value}}}
                            ]
                        }
                    }
                }
            }
            return range_query
        else:
            raise ValueError(f"Unsupported operator '{operator}'.")
    
    elif 'field_top' in tokens:
        # Handle top-level fields as before
        field_name = tokens.field_top
        
        # Validate the top-level field
        if field_name not in VALID_TOP_LEVEL_FIELDS:
            raise ValueError(f"Invalid top-level field in filter expression: '{field_name}'")
        
        # Map operators to OpenSearch equivalents
        operator_map = {
            '=': 'term',
            'eq': 'term',
            '!=': 'must_not',
            'ne': 'must_not',
            '>': 'gt',
            'gt': 'gt',
            '>=': 'gte',
            'ge': 'gte',
            '<': 'lt',
            'lt': 'lt',
            '<=': 'lte',
            'le': 'lte'
        }
        
        range_operators = {'>', 'gt', '>=', 'ge', '<', 'lt', '<=', 'le'}
        equality_operators = {'=', 'eq', '!=', 'ne'}
        
        if operator.lower() in equality_operators:
            term_query = {"term": {field_name: value}}
            if operator.lower() in ['!=', 'ne']:
                return {"bool": {"must_not": [term_query]}}
            else:
                return term_query
        elif operator.lower() in range_operators:
            # Attempt to parse value as float
            try:
                numeric_value = float(value)
            except ValueError:
                raise ValueError(f"Range operator '{operator}' requires a numeric value for field '{field_name}'.")
            
            range_op = operator_map[operator.lower()]
            range_query = {"range": {field_name: {range_op: numeric_value}}}
            return range_query
        else:
            raise ValueError(f"Unsupported operator '{operator}'.")
    else:
        raise ValueError("Invalid field in filter expression.")

def build_sort(order_by):
    # [No changes needed here]
    # ...

def search_runs(request_body):
    # [No changes needed here]
    # ...

if __name__ == "__main__":
    request_body = {
        "experiment_id": "exp1",
        "filter": "metrics.accuracy > 0.9",
        "max_results": 100,
        "order_by": ["metrics.accuracy DESC"]
    }
    try:
        result = search_runs(request_body)
        print(json.dumps(result, indent=2))
    except ValueError as e:
        print(f"Validation Error: {e}")