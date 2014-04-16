# -*- coding: utf-8 -*-
import mdb
from unittest import TestCase


class TestDB(TestCase):

    def setUp(self):
        import os
        import errno
        self.path = './testdbmi'
        try:
            os.makedirs(self.path)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(self.path):
                pass
            else:
                raise
        self.env = mdb.Env(self.path, mapsize=1 * mdb.MB, max_dbs=8)

    def tearDown(self):
        import shutil
        self.env.close()
        shutil.rmtree(self.path)

    def drop_mdb(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        db.drop(txn, 0)
        txn.commit()
        db.close()

    def test_drop(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        items = db.items(txn)
        self.assertRaises(StopIteration, items.next)
        txn.commit()
        db.close()

    def test_put(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        db.put(txn, -11, 'bar')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, -11), 'bar')
        txn.commit()
        db.close()

    def test_contains(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        db.put(txn, 1024, "zzz")
        txn.commit()
        txn = self.env.begin_txn()
        self.assertTrue(db.contains(txn, 1024))
        db.close()

    def test_get_neighbours(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', flags=mdb.MDB_CREATE|mdb.MDB_INTEGERKEY)
        db.put(txn, 1, "1")
        db.put(txn, 5, "2")
        db.put(txn, 7, "3")
        db.put(txn, 8, "5")
        db.put(txn, 18, "6")
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get_neighbours(txn, 0),
                         ((1, "1"), (1, "1")))
        self.assertEqual(db.get_neighbours(txn, 1),
                         ((1, "1"), (1, "1")))
        self.assertEqual(db.get_neighbours(txn, 2),
                         ((1, "1"), (5, "2")))
        self.assertEqual(db.get_neighbours(txn, 3),
                         ((1, "1"), (5, "2")))
        self.assertEqual(db.get_neighbours(txn, 4),
                         ((1, "1"), (5, "2")))
        self.assertEqual(db.get_neighbours(txn, 5),
                         ((5, "2"), (5, "2")))
        self.assertEqual(db.get_neighbours(txn, 6),
                         ((5, "2"), (7, "3")))
        self.assertEqual(db.get_neighbours(txn, 7),
                         ((7, "3"), (7, "3")))
        self.assertEqual(db.get_neighbours(txn, 8),
                         ((8, "5"), (8, "5")))
        self.assertEqual(db.get_neighbours(txn, 9),
                         ((8, "5"), (18, "6")))
        self.assertEqual(db.get_neighbours(txn, 99),
                         ((18, "6"), (18, "6")))

    def test_put_unicode(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        db.put(txn, 32768, 'b∑r')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 32768), 'b∑r')
        txn.commit()
        db.close()

    def test_get_exception(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        self.assertEqual(db.get(txn, 1321312312, 1), 1)
        txn.commit()
        db.close()

    def test_put_excetion(self):
        import random
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        with self.assertRaises(mdb.MapFullError):
            while True:
                db.put(txn,
                       random.randint(0, 10000),
                       'HHHh' * 100)
        txn.abort()
        db.close()

    def test_put_nodup(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        db.put(txn, 12345, 'no', mdb.MDB_NODUPDATA)
        with self.assertRaises(mdb.KeyExistError):
            db.put(txn, 12345, 'no', mdb.MDB_NODUPDATA)
        txn.abort()
        db.close()

    def test_put_duplicate(self):
        # all values must be sorted as well
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        db.put(txn, 13, 'bar')
        db.put(txn, 13, 'bar1')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_dup(txn, 13)],
                         ['bar', 'bar1'])
        txn.commit()
        db.close()

    def test_mget(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        db.put(txn, 1, 'bar')
        db.put(txn, 1, 'bar1')
        db.put(txn, 2, 'bar2')
        db.put(txn, 2, 'bar2-1')
        db.put(txn, 2, 'bar2-2')
        db.put(txn, 3, 'bar3')
        db.put(txn, 4, 'bar4')
        db.put(txn, 5, 'bar5')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(list(db.mget(txn, [1, 2, 3, 5])),
                         ['bar', 'bar2', 'bar3', 'bar5'])

    def test_mget_1(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        db.put(txn, 1, 'bar')
        db.put(txn, 1, 'bar1')
        db.put(txn, 2, 'bar2')
        db.put(txn, 2, 'bar2-1')
        db.put(txn, 2, 'bar2-2')
        db.put(txn, 3, 'bar3')
        db.put(txn, 4, 'bar4')
        db.put(txn, 5, 'bar5')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(list(db.mget(txn, [2, 5])),
                         ['bar2', 'bar5'])

    def test_get_less_than(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        db.put(txn, 1, 'bar')
        db.put(txn, 1, 'bar1')
        db.put(txn, 2, 'bar2')
        db.put(txn, 2, 'bar2-1')
        db.put(txn, 2, 'bar2-2')
        db.put(txn, 3, 'bar3')
        db.put(txn, 4, 'bar4')
        db.put(txn, 5, 'bar5')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, 2)],
                         [(2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2')])
        self.assertEqual([value for value in db.get_eq(txn, 3)],
                         [(3, 'bar3')])
        self.assertEqual([value for value in db.get_lt(txn, 1)],
                         [])
        self.assertEqual([value for value in db.get_lt(txn, 3)],
                         [(1, 'bar'), (1, 'bar1'), (2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2')])
        self.assertEqual([value for value in db.get_lt(txn, 2)],
                         [(1, 'bar'), (1, 'bar1')])
        self.assertEqual([value for value in db.get_gt(txn, 2)],
                         [(3, 'bar3'), (4, 'bar4'), (5, 'bar5')])
        self.assertEqual([value for value in db.get_gt(txn, 3)],
                         [(4, 'bar4'), (5, 'bar5')])
        self.assertEqual([value for value in db.get_gt(txn, 4)],
                         [(5, 'bar5')])
        self.assertEqual([value for value in db.get_gt(txn, 5)],
                         [])
        self.assertEqual([value for value in db.get_le(txn, 2)],
                         [(1, 'bar'), (1, 'bar1'), (2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2')])
        self.assertEqual([value for value in db.get_ge(txn, 2)],
                         [(2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2'),
                          (3, 'bar3'), (4, 'bar4'), (5, 'bar5')])
        self.assertEqual([value for value in db.get_ne(txn, 2)],
                         [(1, 'bar'), (1, 'bar1'),
                          (3, 'bar3'), (4, 'bar4'), (5, 'bar5')])
        self.assertEqual([value for value in db.get_range(txn, 2, 4)],
                         [(2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2'),
                          (3, 'bar3'), (4, 'bar4')])
        txn.commit()
        db.close()

    def test_get_all_items(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        db.put(txn, 14, 'items')
        db.put(txn, 15, 'items1')
        db.put(txn, 14, 'items2')
        txn.commit()
        txn = self.env.begin_txn()
        values = [value for key, value in db.items(txn)]
        self.assertEqual(values,
                         ['items', 'items1'])
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(list(db.dup_items(txn)),
                         [(14, 'items'), (14, 'items2'), (15, 'items1')])
        txn.commit()
        db.close()

    def test_delete_by_key(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        db.put(txn, 16, 'done')
        db.put(txn, 16, 'done1')
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
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY)
        db.put(txn, 17, 'done')
        db.put(txn, 17, 'done1')
        txn.commit()
        txn = self.env.begin_txn()
        db.delete(txn, 17, 'done')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 17), 'done1')
        txn.commit()
        db.close()

    def test_range_for_int8(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY,
                              key_inttype=mdb.MDB_INT_8)
        db.put(txn, -1, 'bar')
        db.put(txn, -1, 'bar1')
        db.put(txn, -2, 'bar2')
        db.put(txn, -2, 'bar2-1')
        db.put(txn, -2, 'bar2-2')
        db.put(txn, -3, 'bar3')
        db.put(txn, -4, 'bar4')
        db.put(txn, -5, 'bar5')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, -2)],
                         [(-2, 'bar2'), (-2, 'bar2-1'), (-2, 'bar2-2')])
        self.assertEqual([value for value in db.get_eq(txn, -3)],
                         [(-3, 'bar3')])
        self.assertEqual([value for value in db.get_gt(txn, -1)],
                         [])
        self.assertEqual([value for value in db.get_gt(txn, -3)],
                         [(-2, 'bar2'), (-2, 'bar2-1'), (-2, 'bar2-2'),
                          (-1, 'bar'), (-1, 'bar1')])
        self.assertEqual([value for value in db.get_gt(txn, -2)],
                         [(-1, 'bar'), (-1, 'bar1')])
        self.assertEqual([value for value in db.get_lt(txn, -2)],
                         [(-5, 'bar5'), (-4, 'bar4'), (-3, 'bar3')])
        self.assertEqual([value for value in db.get_lt(txn, -3)],
                         [(-5, 'bar5'), (-4, 'bar4')])
        self.assertEqual([value for value in db.get_lt(txn, -4)],
                         [(-5, 'bar5')])
        self.assertEqual([value for value in db.get_lt(txn, -5)],
                         [])
        self.assertEqual([value for value in db.get_ge(txn, -2)],
                         [(-2, 'bar2'), (-2, 'bar2-1'), (-2, 'bar2-2'),
                          (-1, 'bar'), (-1, 'bar1')])
        self.assertEqual([value for value in db.get_le(txn, -2)],
                         [(-5, 'bar5'), (-4, 'bar4'), (-3, 'bar3'),
                          (-2, 'bar2'), (-2, 'bar2-1'), (-2, 'bar2-2')])
        self.assertEqual([value for value in db.get_ne(txn, -2)],
                         [(-5, 'bar5'), (-4, 'bar4'), (-3, 'bar3'),
                          (-1, 'bar'), (-1, 'bar1'), ])
        self.assertEqual([value for value in db.get_range(txn, -4, -2)],
                         [(-4, 'bar4'), (-3, 'bar3'), (-2, 'bar2'),
                          (-2, 'bar2-1'), (-2, 'bar2-2'), ])
        txn.commit()
        db.close()

    def test_range_for_uint8(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY,
                              key_inttype=mdb.MDB_INT_8)
        db.put(txn, 1, 'bar')
        db.put(txn, 1, 'bar1')
        db.put(txn, 2, 'bar2')
        db.put(txn, 2, 'bar2-1')
        db.put(txn, 2, 'bar2-2')
        db.put(txn, 3, 'bar3')
        db.put(txn, 4, 'bar4')
        db.put(txn, 5, 'bar5')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, 2)],
                         [(2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2')])
        self.assertEqual([value for value in db.get_eq(txn, 3)],
                         [(3, 'bar3')])
        self.assertEqual([value for value in db.get_lt(txn, 1)],
                         [])
        self.assertEqual([value for value in db.get_lt(txn, 3)],
                         [(1, 'bar'), (1, 'bar1'), (2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2')])
        self.assertEqual([value for value in db.get_lt(txn, 2)],
                         [(1, 'bar'), (1, 'bar1')])
        self.assertEqual([value for value in db.get_gt(txn, 2)],
                         [(3, 'bar3'), (4, 'bar4'), (5, 'bar5')])
        self.assertEqual([value for value in db.get_gt(txn, 3)],
                         [(4, 'bar4'), (5, 'bar5')])
        self.assertEqual([value for value in db.get_gt(txn, 4)],
                         [(5, 'bar5')])
        self.assertEqual([value for value in db.get_gt(txn, 5)],
                         [])
        self.assertEqual([value for value in db.get_le(txn, 2)],
                         [(1, 'bar'), (1, 'bar1'), (2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2')])
        self.assertEqual([value for value in db.get_ge(txn, 2)],
                         [(2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2'),
                          (3, 'bar3'), (4, 'bar4'), (5, 'bar5')])
        self.assertEqual([value for value in db.get_ne(txn, 2)],
                         [(1, 'bar'), (1, 'bar1'),
                          (3, 'bar3'), (4, 'bar4'), (5, 'bar5')])
        self.assertEqual([value for value in db.get_range(txn, 2, 4)],
                         [(2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2'),
                          (3, 'bar3'), (4, 'bar4')])
        txn.commit()
        db.close()

    def test_range_for_int16(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY,
                              key_inttype=mdb.MDB_INT_16)
        db.put(txn, -1, 'bar')
        db.put(txn, -1, 'bar1')
        db.put(txn, -2, 'bar2')
        db.put(txn, -2, 'bar2-1')
        db.put(txn, -2, 'bar2-2')
        db.put(txn, -3, 'bar3')
        db.put(txn, -4, 'bar4')
        db.put(txn, -5, 'bar5')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, -2)],
                         [(-2, 'bar2'), (-2, 'bar2-1'), (-2, 'bar2-2')])
        self.assertEqual([value for value in db.get_eq(txn, -3)],
                         [(-3, 'bar3')])
        self.assertEqual([value for value in db.get_gt(txn, -1)],
                         [])
        self.assertEqual([value for value in db.get_gt(txn, -3)],
                         [(-2, 'bar2'), (-2, 'bar2-1'), (-2, 'bar2-2'),
                          (-1, 'bar'), (-1, 'bar1')])
        self.assertEqual([value for value in db.get_gt(txn, -2)],
                         [(-1, 'bar'), (-1, 'bar1')])
        self.assertEqual([value for value in db.get_lt(txn, -2)],
                         [(-5, 'bar5'), (-4, 'bar4'), (-3, 'bar3')])
        self.assertEqual([value for value in db.get_lt(txn, -3)],
                         [(-5, 'bar5'), (-4, 'bar4')])
        self.assertEqual([value for value in db.get_lt(txn, -4)],
                         [(-5, 'bar5')])
        self.assertEqual([value for value in db.get_lt(txn, -5)],
                         [])
        self.assertEqual([value for value in db.get_ge(txn, -2)],
                         [(-2, 'bar2'), (-2, 'bar2-1'), (-2, 'bar2-2'),
                          (-1, 'bar'), (-1, 'bar1')])
        self.assertEqual([value for value in db.get_le(txn, -2)],
                         [(-5, 'bar5'), (-4, 'bar4'), (-3, 'bar3'),
                          (-2, 'bar2'), (-2, 'bar2-1'), (-2, 'bar2-2')])
        self.assertEqual([value for value in db.get_ne(txn, -2)],
                         [(-5, 'bar5'), (-4, 'bar4'), (-3, 'bar3'),
                          (-1, 'bar'), (-1, 'bar1'), ])
        self.assertEqual([value for value in db.get_range(txn, -4, -2)],
                         [(-4, 'bar4'), (-3, 'bar3'), (-2, 'bar2'),
                          (-2, 'bar2-1'), (-2, 'bar2-2'), ])
        txn.commit()
        db.close()

    def test_range_for_uint16(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY,
                              key_inttype=mdb.MDB_UINT_16)
        db.put(txn, 1, 'bar')
        db.put(txn, 1, 'bar1')
        db.put(txn, 2, 'bar2')
        db.put(txn, 2, 'bar2-1')
        db.put(txn, 2, 'bar2-2')
        db.put(txn, 3, 'bar3')
        db.put(txn, 4, 'bar4')
        db.put(txn, 5, 'bar5')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, 2)],
                         [(2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2')])
        self.assertEqual([value for value in db.get_eq(txn, 3)],
                         [(3, 'bar3')])
        self.assertEqual([value for value in db.get_lt(txn, 1)],
                         [])
        self.assertEqual([value for value in db.get_lt(txn, 3)],
                         [(1, 'bar'), (1, 'bar1'), (2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2')])
        self.assertEqual([value for value in db.get_lt(txn, 2)],
                         [(1, 'bar'), (1, 'bar1')])
        self.assertEqual([value for value in db.get_gt(txn, 2)],
                         [(3, 'bar3'), (4, 'bar4'), (5, 'bar5')])
        self.assertEqual([value for value in db.get_gt(txn, 3)],
                         [(4, 'bar4'), (5, 'bar5')])
        self.assertEqual([value for value in db.get_gt(txn, 4)],
                         [(5, 'bar5')])
        self.assertEqual([value for value in db.get_gt(txn, 5)],
                         [])
        self.assertEqual([value for value in db.get_le(txn, 2)],
                         [(1, 'bar'), (1, 'bar1'), (2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2')])
        self.assertEqual([value for value in db.get_ge(txn, 2)],
                         [(2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2'),
                          (3, 'bar3'), (4, 'bar4'), (5, 'bar5')])
        self.assertEqual([value for value in db.get_ne(txn, 2)],
                         [(1, 'bar'), (1, 'bar1'),
                          (3, 'bar3'), (4, 'bar4'), (5, 'bar5')])
        self.assertEqual([value for value in db.get_range(txn, 2, 4)],
                         [(2, 'bar2'), (2, 'bar2-1'), (2, 'bar2-2'),
                          (3, 'bar3'), (4, 'bar4')])
        txn.commit()
        db.close()
