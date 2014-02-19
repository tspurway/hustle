import unittest
from hustle.core.marble import Marble, check_query


_FIELDS = ("+@4id", "+*name", "+$date", "+%2genre", "+@2rating", "artist", "@4quantity")
_PARTITIONS = "date"
_FIELDS_SELL = ("+@4id", "+@4item_id", "+$date", "@4store_id", "@4quantity", "$price")


class TestChecker(unittest.TestCase):
    def setUp(self):
        self.albums = Marble(name="Albums",
                             fields=_FIELDS,
                             partition=_PARTITIONS)
        self.transaction = Marble(name="Transcation",
                                  fields=_FIELDS_SELL,
                                  partition=_PARTITIONS)
        self.single_where = [(self.albums.rating > 3)]
        self.multi_wheres = [(self.albums.rating > 3) & (self.albums.id == 1000)]
        self.cross_wheres = [self.albums.rating > 3, self.transaction.id == 1000]
        self.single_select = [self.albums.name]
        self.multi_select = [self.albums.name, self.albums.date, self.albums.rating]
        self.cross_select = [self.albums.name, self.albums.artist,
                             self.transaction.store_id, self.transaction.price]
        self.order_by = [self.albums.quantity, self.albums.rating]
        self.join = [self.albums.id, self.transaction.item_id]
        self.join_invalid = [self.albums.id, self.transaction.price]
        self.join_invalid_1 = [self.albums.id, self.albums.id]
        self.join_invalid_2 = [self.albums.id, self.transaction.price]
        self.limit_single = 100
        self.limit_single_invalid = -100

    def test_select_clauses(self):
        # test empty select
        with self.assertRaises(ValueError):
            check_query([],
                        [],
                        self.order_by,
                        None,
                        self.single_where)
        # test duplicate select
        with self.assertRaises(ValueError):
            check_query(self.single_select + self.single_select,
                        [],
                        self.order_by,
                        None,
                        self.single_where)
        self.assertTrue(check_query(self.single_select, [], [],
                                    None, self.single_where))

    def test_where_clauses(self):
        # should raise if a single table shows up in multi-wheres
        # should raise if where and select are from different tables
        with self.assertRaises(ValueError):
            check_query(self.single_select,
                        [],
                        [],
                        self.order_by,
                        [self.transaction.id == 1000])
        self.assertTrue(check_query(self.single_select, [], [],
                                    None, self.single_where))

    def test_join(self):
        # test join with single table
        with self.assertRaises(ValueError):
            check_query(self.single_select,
                        self.join,
                        [],
                        None,
                        self.single_where)

        # test invalid join
        with self.assertRaises(ValueError):
            check_query(self.single_select,
                        self.join_invalid,
                        [],
                        None,
                        self.cross_wheres)

        # test invalid join
        with self.assertRaises(ValueError):
            check_query(self.single_select,
                        self.join_invalid_1,
                        [],
                        None,
                        self.cross_wheres)

        # test invalid join
        with self.assertRaises(ValueError):
            check_query(self.single_select,
                        self.join_invalid_2,
                        [],
                        None,
                        self.cross_wheres)
        self.assertTrue(check_query(self.single_select,
                                    self.join, [], None, self.cross_wheres))

    def test_order_by(self):
        # should raise if select columns don't contain the order column
        with self.assertRaises(ValueError):
            check_query(self.single_select,
                        [],
                        self.order_by,
                        None,
                        self.single_where)
        self.assertTrue(check_query(self.single_select, [], [self.albums.name],
                                    None, self.single_where))

    def test_limit(self):
        with self.assertRaises(ValueError):
            check_query(self.single_select,
                        [],
                        [],
                        self.limit_single_invalid,
                        self.single_where)

        self.assertTrue(
            check_query(self.single_select,
                        [],
                        [],
                        self.limit_single,
                        self.single_where))

    def test_full_query(self):
        self.assertTrue(
            check_query(
                self.cross_select,
                self.join,
                self.single_select,
                self.limit_single,
                self.cross_wheres))
