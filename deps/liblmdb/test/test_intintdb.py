# -*- coding: utf-8 -*-
import mdb
from unittest import TestCase


class TestDB(TestCase):

    def setUp(self):
        import os
        import errno
        self.path = './testdbmii'
        try:
            os.makedirs(self.path)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(self.path):
                pass
            else:
                raise
        self.env = mdb.Env(self.path, mapsize=1 * mdb.MB, max_dbs=8,
                           flags=mdb.MDB_WRITEMAP|mdb.MDB_NOSYNC)

    def tearDown(self):
        import shutil
        self.env.close()
        shutil.rmtree(self.path)

    def drop_mdb(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP)
        db.drop(txn, 0)
        txn.commit()
        db.close()

    def test_drop(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP)
        items = db.items(txn)
        self.assertRaises(StopIteration, items.next)
        txn.commit()
        db.close()

    def test_put(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP)
        db.put(txn, -11, -11)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, -11), -11)
        txn.commit()
        db.close()

    def test_mget(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP)
        db.put(txn, 1, 1)
        db.put(txn, 1, 11)
        db.put(txn, 2, 2)
        db.put(txn, 2, 21)
        db.put(txn, 2, 22)
        db.put(txn, 3, 3)
        db.put(txn, 4, 4)
        db.put(txn, 5, 5)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(list(db.mget(txn, [1, 2, 3, 5])),
                         [1, 2, 3, 5])
        self.assertEqual(list(db.mget(txn, [3, 1, 2, 5])),
                         [3, 1, 2, 5])

    def test_get_neighbours(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP)
        db.put(txn, 1, 1)
        db.put(txn, 5, 2)
        db.put(txn, 7, 3)
        db.put(txn, 8, 5)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get_neighbours(txn, 0),
                         ((1, 1), (1, 1)))
        self.assertEqual(db.get_neighbours(txn, 1),
                         ((1, 1), (1, 1)))
        self.assertEqual(db.get_neighbours(txn, 2),
                         ((1, 1), (5, 2)))
        self.assertEqual(db.get_neighbours(txn, 3),
                         ((1, 1), (5, 2)))
        self.assertEqual(db.get_neighbours(txn, 4),
                         ((1, 1), (5, 2)))
        self.assertEqual(db.get_neighbours(txn, 5),
                         ((5, 2), (5, 2)))
        self.assertEqual(db.get_neighbours(txn, 6),
                         ((5, 2), (7, 3)))
        self.assertEqual(db.get_neighbours(txn, 7),
                         ((7, 3), (7, 3)))
        self.assertEqual(db.get_neighbours(txn, 8),
                         ((8, 5), (8, 5)))
        self.assertEqual(db.get_neighbours(txn, 9),
                         ((8, 5), (8, 5)))

    def test_contains(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP)
        db.put(txn, 1024, 2)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertTrue(db.contains(txn, 1024))
        db.close()

    def test_get_exception(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP)
        self.assertEqual(db.get(txn, 1321312312, 12), 12)
        txn.commit()
        db.close()

    def test_put_duplicate(self):
        # all values must be sorted as well
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP,
                              key_inttype=mdb.MDB_INT_16, value_inttype=mdb.MDB_INT_64)
        db.put(txn, 13, 1312321313123)
        db.put(txn, 13, 1431231231231)
        db.put(txn, 13123, 1431231231231)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_dup(txn, 13)],
                         [1312321313123, 1431231231231])
        self.assertEqual(db.get(txn, 13123), 1431231231231)
        txn.commit()
        db.close()

    def test_get_less_than(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP)
        db.put(txn, 1, 1)
        db.put(txn, 1, 11)
        db.put(txn, 2, 2)
        db.put(txn, 2, 21)
        db.put(txn, 2, 22)
        db.put(txn, 3, 3)
        db.put(txn, 4, 4)
        db.put(txn, 5, 5)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, 2)],
                         [(2, 2), (2, 21), (2, 22)])
        self.assertEqual([value for value in db.get_eq(txn, 3)],
                         [(3, 3)])
        self.assertEqual([value for value in db.get_lt(txn, 1)],
                         [])
        self.assertEqual([value for value in db.get_lt(txn, 3)],
                         [(1, 1), (1, 11), (2, 2), (2, 21), (2, 22)])
        self.assertEqual([value for value in db.get_lt(txn, 2)],
                         [(1, 1), (1, 11)])
        self.assertEqual([value for value in db.get_gt(txn, 2)],
                         [(3, 3), (4, 4), (5, 5)])
        self.assertEqual([value for value in db.get_gt(txn, 3)],
                         [(4, 4), (5, 5)])
        self.assertEqual([value for value in db.get_gt(txn, 4)],
                         [(5, 5)])
        self.assertEqual([value for value in db.get_gt(txn, 5)],
                         [])
        self.assertEqual([value for value in db.get_le(txn, 2)],
                         [(1, 1), (1, 11), (2, 2), (2, 21), (2, 22)])
        self.assertEqual([value for value in db.get_ge(txn, 2)],
                         [(2, 2), (2, 21), (2, 22),
                          (3, 3), (4, 4), (5, 5)])
        self.assertEqual([value for value in db.get_ne(txn, 2)],
                         [(1, 1), (1, 11),
                          (3, 3), (4, 4), (5, 5)])
        self.assertEqual([value for value in db.get_range(txn, 2, 4)],
                         [(2, 2), (2, 21), (2, 22),
                          (3, 3), (4, 4)])
        txn.commit()
        db.close()

    def test_range_uint8(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP,
                              key_inttype=mdb.MDB_UINT_8,
                              value_inttype=mdb.MDB_UINT_8)
        db.put(txn, 1, 1)
        db.put(txn, 1, 11)
        db.put(txn, 2, 2)
        db.put(txn, 2, 21)
        db.put(txn, 2, 22)
        db.put(txn, 3, 3)
        db.put(txn, 4, 4)
        db.put(txn, 5, 5)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, 2)],
                         [(2, 2), (2, 21), (2, 22)])
        self.assertEqual([value for value in db.get_eq(txn, 3)],
                         [(3, 3)])
        self.assertEqual([value for value in db.get_lt(txn, 1)],
                         [])
        self.assertEqual([value for value in db.get_lt(txn, 3)],
                         [(1, 1), (1, 11), (2, 2), (2, 21), (2, 22)])
        self.assertEqual([value for value in db.get_lt(txn, 2)],
                         [(1, 1), (1, 11)])
        self.assertEqual([value for value in db.get_gt(txn, 2)],
                         [(3, 3), (4, 4), (5, 5)])
        self.assertEqual([value for value in db.get_gt(txn, 3)],
                         [(4, 4), (5, 5)])
        self.assertEqual([value for value in db.get_gt(txn, 4)],
                         [(5, 5)])
        self.assertEqual([value for value in db.get_gt(txn, 5)],
                         [])
        self.assertEqual([value for value in db.get_le(txn, 2)],
                         [(1, 1), (1, 11), (2, 2), (2, 21), (2, 22)])
        self.assertEqual([value for value in db.get_ge(txn, 2)],
                         [(2, 2), (2, 21), (2, 22),
                          (3, 3), (4, 4), (5, 5)])
        self.assertEqual([value for value in db.get_ne(txn, 2)],
                         [(1, 1), (1, 11),
                          (3, 3), (4, 4), (5, 5)])
        self.assertEqual([value for value in db.get_range(txn, 2, 4)],
                         [(2, 2), (2, 21), (2, 22),
                          (3, 3), (4, 4)])
        txn.commit()
        db.close()

    def test_range_int8(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP,
                              key_inttype=mdb.MDB_INT_8,
                              value_inttype=mdb.MDB_INT_8)
        db.put(txn, 1, 1)
        db.put(txn, 1, -11)
        db.put(txn, 2, 2)
        db.put(txn, 2, -21)
        db.put(txn, 2, 22)
        db.put(txn, 3, 3)
        db.put(txn, 4, 4)
        db.put(txn, 5, 5)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, 2)],
                         [(2, -21), (2, 2), (2, 22)])
        self.assertEqual([value for value in db.get_eq(txn, 3)],
                         [(3, 3)])
        self.assertEqual([value for value in db.get_lt(txn, 1)],
                         [])
        self.assertEqual([value for value in db.get_lt(txn, 3)],
                         [(1, -11), (1, 1), (2, -21), (2, 2), (2, 22)])
        self.assertEqual([value for value in db.get_lt(txn, 2)],
                         [(1, -11), (1, 1)])
        self.assertEqual([value for value in db.get_gt(txn, 2)],
                         [(3, 3), (4, 4), (5, 5)])
        self.assertEqual([value for value in db.get_gt(txn, 3)],
                         [(4, 4), (5, 5)])
        self.assertEqual([value for value in db.get_gt(txn, 4)],
                         [(5, 5)])
        self.assertEqual([value for value in db.get_gt(txn, 5)],
                         [])
        self.assertEqual([value for value in db.get_le(txn, 2)],
                         [(1, -11), (1, 1), (2, -21), (2, 2), (2, 22)])
        self.assertEqual([value for value in db.get_ge(txn, 2)],
                         [(2, -21), (2, 2), (2, 22),
                          (3, 3), (4, 4), (5, 5)])
        self.assertEqual([value for value in db.get_ne(txn, 2)],
                         [(1, -11), (1, 1),
                          (3, 3), (4, 4), (5, 5)])
        self.assertEqual([value for value in db.get_range(txn, 2, 4)],
                         [(2, -21), (2, 2), (2, 22),
                          (3, 3), (4, 4)])
        txn.commit()
        db.close()

    def test_get_all_items(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP)
        db.put(txn, 14, 14)
        db.put(txn, 15, 15)
        db.put(txn, 14, 141)
        txn.commit()
        txn = self.env.begin_txn()
        values = [value for key, value in db.items(txn)]
        self.assertEqual(values,
                         [14, 15])
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(list(db.dup_items(txn)),
                         [(14, 14), (14, 141), (15, 15)])
        txn.commit()
        db.close()

    def test_delete_by_key(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP)
        db.put(txn, 16, 16)
        db.put(txn, 16, 161)
        txn.commit()
        txn = self.env.begin_txn()
        db.delete(txn, 16)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 16), None)
        txn.abort()
        db.close()

    def test_delete_by_key_value(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY|mdb.MDB_INTEGERDUP)
        db.put(txn, 17, 17)
        db.put(txn, 17, 171)
        txn.commit()
        txn = self.env.begin_txn()
        db.delete(txn, 17, 17)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 17), 171)
        txn.commit()
        db.close()
