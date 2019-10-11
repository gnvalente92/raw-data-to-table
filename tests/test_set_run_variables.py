import hashlib
import os
import unittest

from bq_external_table.set_run_variables import set_run_variables
from bq_external_table.utils import utils_functions


class TestSetRunVariables(unittest.TestCase):

    def test_set_run_variables(self):
        json_config = utils_functions.load_json_config(os.path.abspath("bq_external_table/resources/actions.json"))
        expected = "ad6c13ba8ff9982acade80766126950269419e45f949a359ac710a98b9d37092"
        actual = hashlib.sha256(str(set_run_variables(json_config=json_config)).encode()).hexdigest()
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
