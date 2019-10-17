import hashlib
import os
import unittest

from bq_external_table.set_run_variables import set_run_variables
from bq_external_table.utils import utils_functions


class TestSetRunVariables(unittest.TestCase):

    def test_set_run_variables(self):
        json_config = utils_functions.load_json_config(os.path.abspath("bq_external_table/resources/actions.json"))
        expected = "ab4210c90c16151ee38d7210b07dcb2d3e79f93b068668a6e837172e400b0f7b"
        actual = hashlib.sha256(str(set_run_variables(json_config=json_config)).encode()).hexdigest()
        self.assertEqual(expected, actual)


if __name__ == '__main__':
    unittest.main()
