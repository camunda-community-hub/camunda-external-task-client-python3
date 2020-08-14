import json
import enum


class Variables:
    class ValueType(enum.Enum):

        BOOLEAN = "boolean"
        DATE = "date"
        FILE = "file"
        FLOAT = "float"
        INTEGER = "integer"
        JSON = "json"
        LONG = "long"
        SHORT = "short"
        STRING = "string"
        XML = "XML"

        @classmethod
        def is_valid(cls, value):
            return any(value == item.value for item in cls)

    def __init__(self, variables={}):
        self.variables = variables

    def get_variable(self, variable_name):
        variable = self.variables.get(variable_name, None)
        if not variable:
            return None

        return variable["value"]

    def set_variable(self, name, value, value_type=None):
        # value_type could integer, long, string, boolean, json
        data = {"value": value}
        if self.ValueType.is_valid(value_type):
            if value_type == self.ValueType.JSON:
                data["value"] = json.dumps(value)
            data["type"] = value_type
            data["valueInfo"] = {}
        self.variables[name] = data

    @classmethod
    def format(cls, variables):
        """
        Gives the correct format to variables.
        :param variables: dict - Dictionary of variable names to values.
        :return: Dictionary of well formed variables
            {"var1": 1, "var2": True}
            ->
            {"var1": {"value": 1}, "var2": {"value": True}}
        """
        if isinstance(variables, Variables):
            return variables.variables
        else:
            formatted_vars = {}
            if variables:
                formatted_vars = {k: {"value": v} for k, v in variables.items()}
            return formatted_vars

    def to_dict(self):
        """
        Converts the variables to a simple dictionary
        :return: dict
            {"var1": {"value": 1}, "var2": {"value": True}}
            ->
            {"var1": 1, "var2": True}
        """
        result = {}
        for k, v in self.variables.items():
            result[k] = v["value"]
        return result
