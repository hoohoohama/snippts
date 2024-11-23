from pyparsing import (
    Word, alphas, alphanums, nums, oneOf, infixNotation,
    opAssoc, ParseResults, dblQuotedString, removeQuotes, Group, Keyword
)

# Define basic elements
identifier = Word(alphas + "_", alphanums + "_")
number = Word(nums + ".").setParseAction(lambda t: float(t[0]))
string = dblQuotedString.setParseAction(removeQuotes) | identifier
comparison_op = oneOf("= != > >= < <=")
logical_op = oneOf("AND OR")
field_name = (
    (oneOf("metrics params tags") + "." + identifier) |
    oneOf("status user_id experiment_id run_id start_time end_time")
)

# Define the grammar
condition = Group(field_name + comparison_op + (number | string))
expression = infixNotation(
    condition,
    [
        (logical_op, 2, opAssoc.LEFT),
    ],
)
class Condition:
    def __init__(self, tokens):
        self.field = tokens[0]
        self.operator = tokens[1]
        self.value = tokens[2]

    def to_query(self):
        field_parts = self.field.split('.')
        if len(field_parts) == 2:  # Nested field
            path, key = field_parts
            return {
                "nested": {
                    "path": path,
                    "query": {
                        "bool": {
                            "must": [
                                {"term": {f"{path}.key": key}},
                                self._build_condition(f"{path}.value")
                            ]
                        }
                    }
                }
            }
        else:  # Regular field
            return self._build_condition(self.field)

    def _build_condition(self, field):
        op_map = {
            "=": lambda f, v: {"term": {f: v}},
            "!=": lambda f, v: {"bool": {"must_not": {"term": {f: v}}}},
            ">": lambda f, v: {"range": {f: {"gt": v}}},
            ">=": lambda f, v: {"range": {f: {"gte": v}}},
            "<": lambda f, v: {"range": {f: {"lt": v}}},
            "<=": lambda f, v: {"range": {f: {"lte": v}}},
        }
        return op_map[self.operator](field, self.value)

class LogicalOperation:
    def __init__(self, tokens):
        self.operator = tokens[1]
        self.left = tokens[0]
        self.right = tokens[2]

    def to_query(self):
        op_map = {
            "AND": "must",
            "OR": "should",
        }
        return {
            "bool": {
                op_map[self.operator]: [
                    self.left.to_query(),
                    self.right.to_query(),
                ]
            }
        }
        
        

