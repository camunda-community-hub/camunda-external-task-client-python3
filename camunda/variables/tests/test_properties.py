from unittest import TestCase

from camunda.variables.properties import Properties


class PropertiesTest(TestCase):

    def test_get_variable_returns_none_when_variable_absent(self):
        properties = Properties({})
        self.assertIsNone(properties.get_property("var1"))

    def test_get_variable_returns_value_when_variable_present(self):
        properties = Properties({"var1": 1})
        self.assertEqual(1, properties.get_property("var1"))

    def test_to_dict_returns_variables_as_dict(self):
        properties = Properties({"var1": 1,
                                "var2": True,
                                "var3": "string"})
        self.assertDictEqual({"var1": 1, "var2": True, "var3": "string"}, properties.to_dict())
