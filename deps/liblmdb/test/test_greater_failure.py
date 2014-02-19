# -*- coding: utf-8 -*-
import mdb
from unittest import TestCase


class TestGreaterFailure(TestCase):

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
                              flags=mdb.MDB_CREATE|mdb.MDB_DUPSORT|mdb.MDB_INTEGERKEY,
                              key_inttype=mdb.MDB_INT_32)
        db.drop(txn, 0)
        txn.commit()
        db.close()

    def test_intstr_greater_failure(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_INTEGERKEY|mdb.MDB_DUPSORT,
                              key_inttype=mdb.MDB_INT_32)
        db.put(txn,184504, 'bar1')
        db.put(txn,184031, 'bar2')
        db.put(txn,145248, 'bar3')
        db.put(txn,84131 , 'bar4')
        db.put(txn,3869  , 'bar5')
        db.put(txn,124034, 'bar6')
        db.put(txn,90752 , 'bar7')
        db.put(txn,48288 , 'bar8')
        db.put(txn,97573 , 'bar9')
        db.put(txn,18455 , 'bar0')

        txn.commit()
        txn = self.env.begin_txn()
        res = list(db.get_gt(txn, 50000))
        self.assertEqual(len(res), 7)
        res = list(db.get_gt(txn, 84131))
        self.assertEqual(len(res), 6)
        res = list(db.get_ge(txn, 84131))
        self.assertEqual(len(res), 7)
        txn.commit()
        db.close()

    def test_intint_greater_failure(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_INTEGERKEY|mdb.MDB_DUPSORT|mdb.MDB_INTEGERDUP,
                              key_inttype=mdb.MDB_INT_32)
        db.put(txn,184504, 1)
        db.put(txn,184031, 2)
        db.put(txn,145248, 3)
        db.put(txn,84131 , 4)
        db.put(txn,3869  , 5)
        db.put(txn,124034, 6)
        db.put(txn,90752 , 7)
        db.put(txn,48288 , 8)
        db.put(txn,97573 , 9)
        db.put(txn,18455 , 0)

        txn.commit()
        txn = self.env.begin_txn()
        res = list(db.get_gt(txn, 50000))
        self.assertEqual(len(res), 7)
        res = list(db.get_gt(txn, 84131))
        self.assertEqual(len(res), 6)
        res = list(db.get_ge(txn, 84131))
        self.assertEqual(len(res), 7)
        txn.commit()
        db.close()

    def test_strstr_greater_failure(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE)
        db.put(txn,'holy', 'bar1')
        db.put(txn,'smolly', 'bar2')
        db.put(txn,'abacus', 'bar3')
        db.put(txn,'dreadlock' , 'bar4')
        db.put(txn,'inno'  , 'bar5')
        db.put(txn,'db', 'bar6')
        db.put(txn,'idiotic' , 'bar7')
        db.put(txn,'idioms' , 'bar8')

        txn.commit()
        txn = self.env.begin_txn()
        res = list(db.get_gt(txn, 'grover'))
        self.assertEqual(len(res), 5)
        res = list(db.get_gt(txn, 'db'))
        self.assertEqual(len(res), 6)
        res = list(db.get_ge(txn, 'db'))
        self.assertEqual(len(res), 7)
        txn.commit()
        db.close()

    def test_strint_greater_failure(self):
        # all keys must be sorted
        txn = self.env.begin_txn()
        db = self.env.open_db(txn, 'test_db',
                              flags=mdb.MDB_CREATE|mdb.MDB_INTEGERDUP)
        db.put(txn,'holy', 1)
        db.put(txn,'smolly', 2)
        db.put(txn,'abacus', 3)
        db.put(txn,'dreadlock' , 4)
        db.put(txn,'inno'  , 5)
        db.put(txn,'db', 6)
        db.put(txn,'idiotic' , 7)
        db.put(txn,'idioms' , 8)

        txn.commit()
        txn = self.env.begin_txn()
        res = list(db.get_gt(txn, 'grover'))
        self.assertEqual(len(res), 5)
        res = list(db.get_gt(txn, 'db'))
        self.assertEqual(len(res), 6)
        res = list(db.get_ge(txn, 'db'))
        self.assertEqual(len(res), 7)
        txn.commit()
        db.close()

