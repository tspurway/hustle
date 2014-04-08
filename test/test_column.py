import unittest
from hustle.core.marble import Column
import mdb


_NAME = "test"


class TestColumn(unittest.TestCase):
    def setUp(self):
        pass

    def test_column_errors(self):
        str_column = Column(_NAME, None, index_indicator=0, partition=False,
                            type_indicator=mdb.MDB_STR,
                            compression_indicator=0,
                            rtrie_indicator=mdb.MDB_UINT_16)
        with self.assertRaises(TypeError):
            str_column > 'hello'

        with self.assertRaises(TypeError):
            str_column == 'hello'

    def test_is_int(self):
        str_column = Column(_NAME, None, index_indicator=False, partition=False,
                            type_indicator=mdb.MDB_STR,
                            compression_indicator=0,
                            rtrie_indicator=mdb.MDB_UINT_16)
        self.assertTrue(str_column.is_int)

        str_column = Column(_NAME, None, index_indicator=False, partition=False,
                            type_indicator=mdb.MDB_STR,
                            compression_indicator=1,
                            rtrie_indicator=mdb.MDB_UINT_16)
        self.assertFalse(str_column.is_int)

    def test_is_boolean(self):
        b_column = Column(_NAME, None, boolean=True)
        self.assertTrue(b_column.is_int)
        self.assertTrue(b_column.is_index)
        self.assertTrue(b_column.is_boolean)
        self.assertFalse(b_column.is_wide)
        self.assertFalse(b_column.is_trie)


    def test_is_trie(self):
        str_column = Column(_NAME, None, index_indicator=False, partition=False,
                            type_indicator=mdb.MDB_STR,
                            compression_indicator=0,
                            rtrie_indicator=mdb.MDB_UINT_16)
        self.assertTrue(str_column.is_trie)

        str_column = Column(_NAME, None, index_indicator=False, partition=False,
                            type_indicator=mdb.MDB_INT_16,
                            compression_indicator=0,
                            rtrie_indicator=mdb.MDB_UINT_16)
        self.assertFalse(str_column.is_trie)

        str_column = Column(_NAME, None, index_indicator=False, partition=False,
                            type_indicator=mdb.MDB_STR,
                            compression_indicator=1,
                            rtrie_indicator=mdb.MDB_UINT_16)
        self.assertFalse(str_column.is_trie)

    def test_schema_string(self):
        c = Column(_NAME, None, index_indicator=False, partition=False,
                   type_indicator=mdb.MDB_UINT_16, compression_indicator=0,
                   rtrie_indicator=mdb.MDB_UINT_16)
        self.assertEqual(c.schema_string(), "%s%s" % ('@2', _NAME))

        c.type_indicator = mdb.MDB_INT_16
        self.assertEqual(c.schema_string(), "%s%s" % ('#2', _NAME))
        c.type_indicator = mdb.MDB_INT_32
        self.assertEqual(c.schema_string(), "%s%s" % ('#4', _NAME))
        c.type_indicator = mdb.MDB_UINT_32
        self.assertEqual(c.schema_string(), "%s%s" % ('@4', _NAME))
        c.type_indicator = mdb.MDB_INT_64
        self.assertEqual(c.schema_string(), "%s%s" % ('#8', _NAME))
        c.type_indicator = mdb.MDB_UINT_64
        self.assertEqual(c.schema_string(), "%s%s" % ('@8', _NAME))

        c.type_indicator = mdb.MDB_STR
        c.compression_indicator = 0
        self.assertEqual(c.schema_string(), "%s%s" % ('%2', _NAME))
        c.rtrie_indicator = mdb.MDB_UINT_32
        self.assertEqual(c.schema_string(), "%s%s" % ('%4', _NAME))
        c.compression_indicator = 1
        self.assertEqual(c.schema_string(), "%s%s" % ('$', _NAME))
        c.compression_indicator = 2
        self.assertEqual(c.schema_string(), "%s%s" % ('*', _NAME))

    def test_get_effective_inttype(self):
        c = Column(_NAME, None, index_indicator=False, partition=False,
                   type_indicator=mdb.MDB_UINT_16, compression_indicator=0,
                   rtrie_indicator=mdb.MDB_INT_16)
        self.assertEqual(c.get_effective_inttype(), mdb.MDB_UINT_16)

        c.type_indicator = mdb.MDB_STR
        self.assertEqual(c.get_effective_inttype(), mdb.MDB_INT_16)

    def test_check_range_query_for_trie(self):
        c = Column(_NAME, None, index_indicator=1, partition=False,
                   type_indicator=mdb.MDB_STR, compression_indicator=0,
                   rtrie_indicator=mdb.MDB_INT_16)
        with self.assertRaises(TypeError):
            c < "foo"
        with self.assertRaises(TypeError):
            c <= "foo"
        with self.assertRaises(TypeError):
            c > "foo"
        with self.assertRaises(TypeError):
            c >= "foo"
        c == "foo"
        c != "foo"

    def test_check_range_query_for_lz4(self):
        c = Column(_NAME, None, index_indicator=1, partition=False,
                   type_indicator=mdb.MDB_STR, compression_indicator=2,
                   rtrie_indicator=None)
        with self.assertRaises(TypeError):
            c < "foo"
        with self.assertRaises(TypeError):
            c <= "foo"
        with self.assertRaises(TypeError):
            c > "foo"
        with self.assertRaises(TypeError):
            c >= "foo"
        c == "foo"
        c != "foo"

    def test_check_range_query_for_partition(self):
        c = Column(_NAME, None, index_indicator=1, partition=True,
                   type_indicator=mdb.MDB_STR, compression_indicator=1,
                   rtrie_indicator=None)
        c < "foo"
        c <= "foo"
        c > "foo"
        c >= "foo"
        c == "foo"
        c != "foo"

    def test_check_range_query(self):
        c = Column(_NAME, None, index_indicator=True, partition=False,
                   type_indicator=mdb.MDB_INT_16, compression_indicator=1,
                   rtrie_indicator=None)
        c < 1
        c <= 1
        c > 1
        c >= 1
        c == 1
        c != 1
