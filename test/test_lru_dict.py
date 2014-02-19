import mdb
import os
import unittest
from functools import partial
from hustle.core.lru_dict import mdb_evict, mdb_fetch
from pylru import LRUDict
from pyebset import BitSet


class TestLRUDict(unittest.TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        try:
            os.unlink('/tmp/lru_test')
            os.unlink('/tmp/lru_test-lock')
        except:
            pass

    def test_lru(self):
        def get(db, txn, key):
            try:
                return db.get(txn, key)
            except:
                return None

        env = mdb.Env('/tmp/lru_test', flags=mdb.MDB_WRITEMAP | mdb.MDB_NOSYNC | mdb.MDB_NOSUBDIR)
        txn = env.begin_txn()
        ixdb = env.open_db(txn, 'ix', flags=mdb.MDB_CREATE)

        lru = LRUDict.getDict(5,
                      partial(mdb_fetch, txn=txn, ixdb=ixdb),
                      partial(mdb_evict, txn=txn, ixdb=ixdb))

        lru.set('hello', BitSet())
        lru.set('goodbye', BitSet())
        lru.set('amine', BitSet())
        lru.set('solution', BitSet())
        lru.set('lsd', BitSet())
        self.assertEqual(len(lru._getContents()), 5)

        lru.set('proxy', BitSet())
        store = lru._getContents()
        self.assertNotIn('hello', store)
        self.assertIsNotNone(get(ixdb, txn, 'hello'))
        self.assertEqual(len(store), 5)

        bitmap = lru['hello']
        store = lru._getContents()
        self.assertIn('hello', store)
        self.assertEqual(len(store), 5)
        self.assertIsInstance(bitmap, BitSet)
        self.assertIsNone(lru.get('skibiddles'))

        # test eviction order
        self.assertIsNotNone(lru.get('goodbye')) # this now should 'reset' goodbye so that it won't be evicted
        lru.set('whammy bar', BitSet()) # amine should be evicted
        store = lru._getContents()
        self.assertNotIn('amine', store)
        self.assertIn('goodbye', store)

        txn.commit()
        env.close()
