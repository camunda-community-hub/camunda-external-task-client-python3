from unittest import TestCase

from camunda.utils.utils import str_to_list


class TestUtils(TestCase):
    def test_str_to_list_returns_list_as_is(self):
        self.assertEqual([], str_to_list([]))
        self.assertEqual([1, 2, 3], str_to_list([1, 2, 3]))
        self.assertEqual(["a", "b", "c"], str_to_list(["a", "b", "c"]))

    def test_str_to_list_returns_list_with_string_passed(self):
        self.assertEqual(["hello"], str_to_list("hello"))
