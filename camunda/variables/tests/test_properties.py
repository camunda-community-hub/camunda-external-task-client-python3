from unittest import TestCase

from camunda.variables.properties import Properties


class PropertiesTest(TestCase):

    def test_get_variable_returns_none_when_variable_absent(self):
        properties = Properties({})
        self.assertIsNone(properties.get_property("var1"))

    def test_get_variable_returns_value_when_variable_present(self):
        properties = Properties({"var1": "one"})
        self.assertEqual("one", properties.get_property("var1"))

    def test_to_dict_returns_variables_as_dict(self):
        properties = Properties({"var1": "Sample1",
                                 "var2": "Sample2",
                                 "var3": "Sample3"})
        self.assertDictEqual({"var1": "Sample1", "var2": "Sample2", "var3": "Sample3"}, properties.to_dict())
