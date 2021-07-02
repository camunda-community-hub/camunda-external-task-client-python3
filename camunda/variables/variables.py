
import json

class Variables:
    def __init__(self, variables={}):
        self.variables = variables

    def get_variable(self, variable_name, with_meta=False):
        variable = self.variables.get(variable_name, None)
        if not variable:
            return None
        if with_meta:
            return variable
        return variable["value"]

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
        formatted_vars = {}
        if variables:
            formatted_vars = {
                k: v if isinstance(v, dict) else {"value": v}
                for k, v in variables.items()
            }
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
