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
        db = self.env.open_db(txn, 'test_db')
        db.put(txn, 'foo', 'bar')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 'foo'), 'bar')
        db.close()

    def test_contains(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        db.put(txn, 'fΩo', 'bar')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertTrue(db.contains(txn, 'fΩo'))
        db.close()

    def test_put_unicode(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        db.put(txn, 'fΩo', 'b∑r')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 'fΩo'), 'b∑r')
        db.close()

    def test_put_zerostring(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        db.put(txn, 'nopstring', '')
        db.put(txn, '', '')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 'nopstring'), '')
        self.assertEqual(db.get(txn, ''), '')
        txn.abort()
        db.close()

    def test_get_exception(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        self.assertEqual(db.get(txn, "Not Existed", "Default"), "Default")
        txn.commit()
        db.close()

    def test_put_excetion(self):
        import random
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        with self.assertRaises(mdb.MapFullError):
            while True:
                db.put(txn,
                       "%d" % random.randint(0, 10000),
                       'HHHh' * 100)
        txn.abort()
        db.close()

    def test_put_nodup(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        db.put(txn, 'dupkey', 'no', mdb.MDB_NODUPDATA)
        with self.assertRaises(mdb.KeyExistError):
            db.put(txn, 'dupkey', 'no', mdb.MDB_NODUPDATA)
        txn.abort()
        db.close()

    def test_put_duplicate(self):
        # all values must be sorted as well
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        db.put(txn, 'foo', 'bar')
        db.put(txn, 'foo', 'bar1')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_dup(txn, 'foo')],
                         ['bar', 'bar1'])
        db.close()

    def test_get_all_items(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        db.put(txn, 'all', 'items')
        db.put(txn, 'all1', 'items1')
        db.put(txn, 'all', 'items2')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(list(db.dup_items(txn)),
                         [('all', 'items'), ('all', 'items2'), ('all1', 'items1')])
        db.close()

    def test_get_less_than(self):
        self.drop_mdb()
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        db.put(txn, '1', 'bar')
        db.put(txn, '1', 'bar1')
        db.put(txn, '2', 'bar2')
        db.put(txn, '3', 'bar3')
        db.put(txn, '4', 'bar4')
        db.put(txn, '5', 'bar5')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual([value for value in db.get_eq(txn, '1')],
                         [('1', 'bar'), ('1', 'bar1')])
        self.assertEqual([value for value in db.get_ne(txn, '1')],
                         [('2', 'bar2'), ('3', 'bar3'), ('4', 'bar4'), ('5', 'bar5')])
        self.assertEqual([value for value in db.get_lt(txn, '3')],
                         [('1', 'bar'), ('1', 'bar1'), ('2', 'bar2')])
        self.assertEqual([value for value in db.get_le(txn, '3')],
                         [('1', 'bar'), ('1', 'bar1'), ('2', 'bar2'), ('3', 'bar3')])
        self.assertEqual([value for value in db.get_gt(txn, '1')],
                         [('2', 'bar2'), ('3', 'bar3'), ('4', 'bar4'), ('5', 'bar5')])
        self.assertEqual([value for value in db.get_ge(txn, '3')],
                         [('3', 'bar3'), ('4', 'bar4'), ('5', 'bar5')])
        self.assertEqual([value for value in db.get_range(txn, '1', '3')],
                         [('1', 'bar'), ('1', 'bar1'), ('2', 'bar2'), ('3', 'bar3')])
        txn.commit()
        db.close()

    def test_delete_by_key(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        db.put(txn, 'delete', 'done')
        db.put(txn, 'delete', 'done1')
        txn.commit()
        txn = self.env.begin_txn()
        db.delete(txn, 'delete')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 'delete', None), None)
        txn.abort()
        db.close()

    def test_delete_by_key_value(self):
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db')
        db.put(txn, 'delete', 'done')
        db.put(txn, 'delete', 'done1')
        txn.commit()
        txn = self.env.begin_txn()
        db.delete(txn, 'delete', 'done')
        txn.commit()
        txn = self.env.begin_txn()
        self.assertEqual(db.get(txn, 'delete'), 'done1')
        db.close()
