from unittest import TestCase

from camunda.variables.variables import Variables


class VariablesTest(TestCase):

    def test_get_variable_returns_none_when_variable_absent(self):
        variables = Variables({})
        self.assertIsNone(variables.get_variable("var1"))

    def test_get_variable_returns_value_when_variable_present(self):
        variables = Variables({"var1": {"value": 1}})
        self.assertEqual(1, variables.get_variable("var1"))

    def test_get_variable_returns_with_meta(self):
        var1_raw = {"value": 1}
        variables = Variables({"var1": var1_raw})
        self.assertEqual(var1_raw, variables.get_variable("var1", True))

    def test_get_variable_returns_without_meta(self):
        var1_raw = {"value": 1}
        variables = Variables({"var1": var1_raw})
        self.assertEqual(1, variables.get_variable("var1", False))

    def test_format_returns_empty_dict_when_none_is_passed(self):
        variables = None
        self.assertDictEqual({}, Variables.format(variables))

    def test_format_returns_empty_dict_when_variables_absent(self):
        variables = {}
        self.assertDictEqual({}, Variables.format(variables))

    def test_format_returns_formatted_variables_when_variables_present(self):
        variables = {"var1": 1, "var2": True, "var3": "string"}
        formatted_vars = Variables.format(variables)
        self.assertDictEqual({"var1": {"value": 1},
                              "var2": {"value": True},
                              "var3": {"value": "string"}}, formatted_vars)

    def test_format_returns_formatted_variables_keeps_already_formatted(self):
        variables = {"var1": 1, "var2": True, "var3": "string", "var4": {"value": 1}}
        formatted_vars = Variables.format(variables)
        self.assertDictEqual({"var1": {"value": 1},
                              "var2": {"value": True},
                              "var3": {"value": "string"},
                              "var4": {"value": 1}}, formatted_vars)

    def test_to_dict_returns_variables_as_dict(self):
        variables = Variables({"var1": {"value": 1},
                               "var2": {"value": True},
                               "var3": {"value": "string"}})
        self.assertDictEqual({"var1": 1, "var2": True, "var3": "string"}, variables.to_dict())

    def test_json_returns_variables_as_json(self):
        variables = {"list": ["a", "b", "c"], "obj": [{"z":2, "h":3}, {"t":5, "s":9}], "zobj": {"az": [0, 1, 2], "zot": [True, False, False]}}
        formatted_vars = Variables.format(variables)
        self.assertDictEqual({"list": {"type": "json", "value": "[\"a\", \"b\", \"c\"]"},
                              "obj":  {"type": "json", "value": "[{\"z\": 2, \"h\": 3}, {\"t\": 5, \"s\": 9}]"},
                              "zobj": {"type": "json", "value": "{\"az\": [0, 1, 2], \"zot\": [true, false, false]}"}}, formatted_vars)

