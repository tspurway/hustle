import unittest
from hustle import Table

class TestTable(unittest.TestCase):
    def test_create_syntax(self):
        full_columns = ['wide index uint32 x', 'index string y', 'int16 z', 'lz4 a', 'trie32 b', 'binary c']
        full_fields = ['=@4x', '+$y', '#2z', '*a', '%4b', '&c']
        fields = Table.parse_column_specs(full_columns)
        self.assertListEqual(fields, full_fields)

        default_columns = ['wide index x', 'index int y', 'uint z', 'trie b', 'c']
        default_fields = ['=x', '+#y', '@z', '%b', 'c']
        fields = Table.parse_column_specs(default_columns)
        self.assertListEqual(fields, default_fields)

    def test_create_errors(self):
        self.assertRaises(ValueError, Table.parse_column_specs, ['wide wide index x'])
        self.assertRaises(ValueError, Table.parse_column_specs, ['index wide x'])
        self.assertRaises(ValueError, Table.parse_column_specs, ['index blah16 x'])
        self.assertRaises(ValueError, Table.parse_column_specs, ['uint24 x'])
