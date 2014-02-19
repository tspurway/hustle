# -*- coding: utf-8 -*-
import mdb
from unittest import TestCase


class TestCursor(TestCase):

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
        self.env = mdb.Env(self.path, max_dbs=8)
        self.txn = self.env.begin_txn()
        self.db = self.env.open_db(self.txn, 'test_cursor')
        self.db.drop(self.txn, 0)
        self.txn.commit()
        self.txn = self.env.begin_txn()

    def tearDown(self):
        import shutil
        self.txn.commit()
        self.db.close()
        self.env.close()
        shutil.rmtree(self.path)

    def test_put(self):
        # all keys must be sorted
        cursor = mdb.Cursor(self.txn, self.db)
        cursor.put('foo', 'bar', mdb.MDB_APPENDDUP)
        self.assertEqual(cursor.get('foo'), ('foo', 'bar'))

    def test_put_unicode(self):
        # all keys must be sorted
        cursor = mdb.Cursor(self.txn, self.db)
        cursor.put('fΩo', 'b∑r', mdb.MDB_APPENDDUP)
        self.assertEqual(cursor.get('fΩo'), ('fΩo', 'b∑r'))

    def test_put_duplicate(self):
        # all values must be sorted as well
        cursor = mdb.Cursor(self.txn, self.db)
        cursor.put('foo', 'bar', mdb.MDB_APPENDDUP)
        cursor.put('foo', 'bar1', mdb.MDB_APPENDDUP)
        self.assertEqual(cursor.count_dups(), 2)
        self.assertEqual(cursor.get('foo'), ('foo', 'bar'))
        while 1:
            key, value = cursor.get(op=mdb.MDB_NEXT_DUP)
            if not key:
                break
            self.assertEqual((key, value), ('foo', 'bar1'))

    def test_delete_by_key(self):
        cursor = mdb.Cursor(self.txn, self.db)
        cursor.put('delete', 'done', mdb.MDB_APPENDDUP)
        cursor.put('delete', 'done1', mdb.MDB_APPENDDUP)
        key, value = cursor.get('delete')
        cursor.delete(mdb.MDB_NODUPDATA)
        self.assertEqual(cursor.get('delete'), (None, None))

    def test_delete_by_key_value(self):
        cursor = mdb.Cursor(self.txn, self.db)
        cursor.put('delete', 'done', mdb.MDB_APPENDDUP)
        cursor.put('delete', 'done1', mdb.MDB_APPENDDUP)
        key, value = cursor.get('delete')
        cursor.delete()
        self.assertEqual(cursor.get('delete'), ('delete', 'done1'))

    def test_delete_by_key_value_1(self):
        cursor = mdb.Cursor(self.txn, self.db)
        cursor.put('delete', 'done', mdb.MDB_APPENDDUP)
        cursor.put('delete', 'done1', mdb.MDB_APPENDDUP)
        cursor.put('delete', 'done2', mdb.MDB_APPENDDUP)
        key, value = cursor.get('delete', 'done2', op=mdb.MDB_NEXT_DUP)
        cursor.delete()
        self.assertEqual(cursor.get('delete'), ('delete', 'done'))
