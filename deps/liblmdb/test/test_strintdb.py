# -*- coding: utf-8 -*-
import mdb
from unittest import TestCase


class TestDB(TestCase):

    def setUp(self):
        import os
        import errno
        self.path = './testdbm'
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
        db = self.env.open_db(txn, 'test_db')
        db.drop(txn, 0)
        txn.commit()
        db.close()

    def test_drop(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        items = db.items(txn)
        self.assertRaises(StopIteration, items.next)
        db.close()

    def test_put(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE,
                              value_inttype=mdb.MDB_UINT_64)
        db.put(txn, 'foo', 9223371238321823122)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 'foo'), 9223371238321823122)
        db.close()

    def test_put_unicode(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        db.put(txn, 'fΩo', 2)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 'fΩo'), 2)
        db.close()

    def test_contains(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        db.put(txn, 'fΩo', 2)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertTrue(db.contains(txn, 'fΩo'))
        db.close()

    def test_get_exception(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        self.assertEqual(db.get(txn, "Not Existed", ""), "")
        txn.commit()
        db.close()

    def test_put_duplicate(self):
        # all values must be sorted as well
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        db.put(txn, 'foo', 1)
        db.put(txn, 'foo', 2)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_dup(txn, 'foo')],
                         [1, 2])
        db.close()

    def test_mget(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE,
                              value_inttype=mdb.MDB_UINT_8)
        db.drop(txn)
        db.put(txn, 'bar1', 1)
        db.put(txn, 'bar1', 2)
        db.put(txn, 'bar2', 3)
        db.put(txn, 'bar3', 4)
        db.put(txn, 'bar4', 5)
        db.put(txn, 'bar5', 6)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertListEqual(list(db.mget(txn, ['bar1', 'bar2', 'bar5'])),
                             [1, 3, 6])
        self.assertListEqual(list(db.mget(txn, ['bar5', 'bar1', 'bar3', 'bar4'])),
                             [6, 1, 4, 5])

    def test_get_all_items(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE,
                              value_inttype=mdb.MDB_INT_64)
        db.put(txn, 'all', 1323123132131231312)
        db.put(txn, 'all1', 2123123123131312313)
        db.put(txn, 'all', 1231231231231312313)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(list(db.dup_items(txn)),
                         [('all', 1231231231231312313), ('all', 1323123132131231312),
                          ('all1', 2123123123131312313)])
        db.close()

    def test_get_less_than(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        db.drop(txn)
        db.put(txn, 'bar1', 1)
        db.put(txn, 'bar1', 2)
        db.put(txn, 'bar2', 3)
        db.put(txn, 'bar3', 4)
        db.put(txn, 'bar4', 5)
        db.put(txn, 'bar5', 6)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, 'bar1')],
                         [('bar1', 1), ('bar1', 2)])
        self.assertEqual([value for value in db.get_ne(txn, 'bar1')],
                         [('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_lt(txn, 'bar3')],
                         [('bar1', 1), ('bar1', 2), ('bar2', 3)])
        self.assertEqual([value for value in db.get_le(txn, 'bar3')],
                         [('bar1', 1), ('bar1', 2), ('bar2', 3), ('bar3', 4)])
        self.assertEqual([value for value in db.get_gt(txn, 'bar1')],
                         [('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_ge(txn, 'bar1')],
                         [('bar1', 1), ('bar1', 2), ('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_range(txn, 'bar1', 'bar3')],
                         [('bar1', 1), ('bar1', 2), ('bar2', 3), ('bar3', 4)])
        txn.commit()
        db.close()

    def test_range_uint8(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE,
                              value_inttype=mdb.MDB_UINT_8)
        db.drop(txn)
        db.put(txn, 'bar1', 1)
        db.put(txn, 'bar1', 2)
        db.put(txn, 'bar2', 3)
        db.put(txn, 'bar3', 4)
        db.put(txn, 'bar4', 5)
        db.put(txn, 'bar5', 6)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, 'bar1')],
                         [('bar1', 1), ('bar1', 2)])
        self.assertEqual([value for value in db.get_ne(txn, 'bar1')],
                         [('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_lt(txn, 'bar3')],
                         [('bar1', 1), ('bar1', 2), ('bar2', 3)])
        self.assertEqual([value for value in db.get_le(txn, 'bar3')],
                         [('bar1', 1), ('bar1', 2), ('bar2', 3), ('bar3', 4)])
        self.assertEqual([value for value in db.get_gt(txn, 'bar1')],
                         [('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_ge(txn, 'bar1')],
                         [('bar1', 1), ('bar1', 2), ('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_range(txn, 'bar1', 'bar3')],
                         [('bar1', 1), ('bar1', 2), ('bar2', 3), ('bar3', 4)])
        txn.commit()
        db.close()

    def test_range_int8(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE,
                              value_inttype=mdb.MDB_INT_8)
        db.drop(txn)
        db.put(txn, 'bar1', -1)
        db.put(txn, 'bar1', -2)
        db.put(txn, 'bar2', 3)
        db.put(txn, 'bar3', 4)
        db.put(txn, 'bar4', 5)
        db.put(txn, 'bar5', 6)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, 'bar1')],
                         [('bar1', -2), ('bar1', -1)])
        self.assertEqual([value for value in db.get_ne(txn, 'bar1')],
                         [('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_lt(txn, 'bar3')],
                         [('bar1', -2), ('bar1', -1), ('bar2', 3)])
        self.assertEqual([value for value in db.get_le(txn, 'bar3')],
                         [('bar1', -2), ('bar1', -1), ('bar2', 3), ('bar3', 4)])
        self.assertEqual([value for value in db.get_gt(txn, 'bar1')],
                         [('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_ge(txn, 'bar1')],
                         [('bar1', -2), ('bar1', -1), ('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_range(txn, 'bar1', 'bar3')],
                         [('bar1', -2), ('bar1', -1), ('bar2', 3), ('bar3', 4)])
        txn.commit()
        db.close()

    def test_range_uint16(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE,
                              value_inttype=mdb.MDB_UINT_16)
        db.drop(txn)
        db.put(txn, 'bar1', 1)
        db.put(txn, 'bar1', 2)
        db.put(txn, 'bar2', 3)
        db.put(txn, 'bar3', 4)
        db.put(txn, 'bar4', 5)
        db.put(txn, 'bar5', 6)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, 'bar1')],
                         [('bar1', 1), ('bar1', 2)])
        self.assertEqual([value for value in db.get_ne(txn, 'bar1')],
                         [('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_lt(txn, 'bar3')],
                         [('bar1', 1), ('bar1', 2), ('bar2', 3)])
        self.assertEqual([value for value in db.get_le(txn, 'bar3')],
                         [('bar1', 1), ('bar1', 2), ('bar2', 3), ('bar3', 4)])
        self.assertEqual([value for value in db.get_gt(txn, 'bar1')],
                         [('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_ge(txn, 'bar1')],
                         [('bar1', 1), ('bar1', 2), ('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_range(txn, 'bar1', 'bar3')],
                         [('bar1', 1), ('bar1', 2), ('bar2', 3), ('bar3', 4)])
        txn.commit()
        db.close()

    def test_range_int16(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE,
                              value_inttype=mdb.MDB_INT_16)
        db.drop(txn)
        db.put(txn, 'bar1', -1)
        db.put(txn, 'bar1', -2)
        db.put(txn, 'bar2', 3)
        db.put(txn, 'bar3', 4)
        db.put(txn, 'bar4', 5)
        db.put(txn, 'bar5', 6)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, 'bar1')],
                         [('bar1', -2), ('bar1', -1)])
        self.assertEqual([value for value in db.get_ne(txn, 'bar1')],
                         [('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_lt(txn, 'bar3')],
                         [('bar1', -2), ('bar1', -1), ('bar2', 3)])
        self.assertEqual([value for value in db.get_le(txn, 'bar3')],
                         [('bar1', -2), ('bar1', -1), ('bar2', 3), ('bar3', 4)])
        self.assertEqual([value for value in db.get_gt(txn, 'bar1')],
                         [('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_ge(txn, 'bar1')],
                         [('bar1', -2), ('bar1', -1), ('bar2', 3), ('bar3', 4), ('bar4', 5), ('bar5', 6)])
        self.assertEqual([value for value in db.get_range(txn, 'bar1', 'bar3')],
                         [('bar1', -2), ('bar1', -1), ('bar2', 3), ('bar3', 4)])
        txn.commit()
        db.close()

    def test_delete_by_key(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        db.put(txn, 'delete', 1)
        db.put(txn, 'delete', 11)
        txn.commit()
        txn = self.env.begin_txn()
        db.delete(txn, 'delete')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 'delete'), None)
        txn.abort()
        db.close()

    def test_delete_by_key_value(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db', mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP|mdb.MDB_CREATE)
        db.put(txn, 'delete', 1)
        db.put(txn, 'delete', 11)
        txn.commit()
        txn = self.env.begin_txn()
        db.delete(txn, 'delete', 1)
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 'delete'), 11)
        db.close()
