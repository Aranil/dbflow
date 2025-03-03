import unittest
import os
from dbflow.src.db_utility import create_sql


from dbflow.logging_config import logger

# TODO: add tests for critical functions !!

class TestCreateSQL(unittest.TestCase):
    def setUp(self):
        # Create a mock SQL file
        self.sql_file = '_mock_query.sql'
        with open(self.sql_file, 'w') as f:
            f.write("SELECT * FROM users WHERE id = :user_id;")

    def tearDown(self):
        # Remove the mock SQL file
        if os.path.exists(self.sql_file):
            os.remove(self.sql_file)

    def test_create_sql_happy_path(self):
        # Test with valid replacements
        replacements = {':user_id': '42'}
        result = create_sql(self.sql_file, replacements)
        expected = "SELECT * FROM users WHERE id = 42;"
        self.assertEqual(result, expected)

    def test_create_sql_missing_placeholder(self):
        # Test when no replacements are provided
        replacements = {}
        result = create_sql(self.sql_file, replacements)
        expected = "SELECT * FROM users WHERE id = :user_id;"
        self.assertEqual(result, expected)

    def test_create_sql_invalid_file(self):
        # Test with a non-existent file
        with self.assertRaises(FileNotFoundError):
            create_sql('non_existent.sql', {})

if __name__ == '__main__':
    unittest.main()