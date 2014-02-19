# -*- coding: utf-8 -*-
import unittest
import rtrie
import mdb
from wtrie import Trie

class TestRTrie(unittest.TestCase):
    def test_rtrie_in_memory(self):

        s = unicode(u'séllsink').encode('utf-8')
        #print "HELLSINK: %s" % s

        t = Trie()
        self.assertEqual(t.add('hello'), 1)
        self.assertEqual(t.add('hell'), 2)
        self.assertEqual(t.add('hello'), 1)
        self.assertEqual(t.add('hellothere'), 3)
        self.assertEqual(t.add('good'), 4)
        self.assertEqual(t.add('goodbye'), 5)
        self.assertEqual(t.add('hello'), 1)
        self.assertEqual(t.add('hellsink'), 6)
        self.assertEqual(t.add(s), 7)
        t.print_it()

        nodes, kids, _ = t.serialize()
        nodeaddr, nodelen = nodes.buffer_info()
        kidaddr, kidlen = kids.buffer_info()
        print "LENS %s %s" % (nodelen, kidlen)

        for i in range(8):
            val = rtrie.value_for_vid(nodeaddr, kidaddr, i)
            print "Value", i, val

        self.assertEqual(rtrie.vid_for_value(nodeaddr, kidaddr, 'hello'), 1)
        self.assertEqual(rtrie.vid_for_value(nodeaddr, kidaddr, 'hell'), 2)
        self.assertEqual(rtrie.vid_for_value(nodeaddr, kidaddr, 'goodbye'), 5)
        self.assertEqual(rtrie.vid_for_value(nodeaddr, kidaddr, 'hellsink'), 6)
        self.assertEqual(rtrie.vid_for_value(nodeaddr, kidaddr, 'hellothere'), 3)
        self.assertEqual(rtrie.vid_for_value(nodeaddr, kidaddr, 'good'), 4)
        self.assertEqual(rtrie.vid_for_value(nodeaddr, kidaddr, s), 7)
        self.assertIsNone(rtrie.vid_for_value(nodeaddr, kidaddr, 'notthere'))
        self.assertIsNone(rtrie.vid_for_value(nodeaddr, kidaddr, 'h'))
        self.assertIsNone(rtrie.vid_for_value(nodeaddr, kidaddr, 'he'))
        self.assertIsNone(rtrie.vid_for_value(nodeaddr, kidaddr, 'hel'))
        self.assertIsNone(rtrie.vid_for_value(nodeaddr, kidaddr, 'hells'))

    def test_rtrie_in_mdb(self):
        t = Trie()
        self.assertEqual(t.add('hello'), 1)
        self.assertEqual(t.add('hell'), 2)
        self.assertEqual(t.add('hello'), 1)
        self.assertEqual(t.add('hellothere'), 3)
        self.assertEqual(t.add('good'), 4)
        self.assertEqual(t.add('goodbye'), 5)
        self.assertEqual(t.add('hello'), 1)
        self.assertEqual(t.add('hellsink'), 6)

        nodes, kids, _ = t.serialize()
        nodeaddr, nodelen = nodes.buffer_info()
        kidaddr, kidlen = kids.buffer_info()
        try:
            env = mdb.Env('/tmp/test_rtrie', flags=mdb.MDB_WRITEMAP | mdb.MDB_NOSYNC | mdb.MDB_NOSUBDIR)
            txn = env.begin_txn()
            db = env.open_db(txn, name='_meta_', flags=mdb.MDB_CREATE)
            db.put_raw(txn, 'nodes', nodeaddr, nodelen)
            db.put_raw(txn, 'kids', kidaddr, kidlen)

            n, ns = db.get_raw(txn, 'nodes')
            k, ks = db.get_raw(txn, 'kids')
            txn.commit()
            env.close()

            env = mdb.Env('/tmp/test_rtrie', flags=mdb.MDB_NOSYNC | mdb.MDB_NOSUBDIR)
            txn = env.begin_txn()
            db = env.open_db(txn, name='_meta_')

            n, ns = db.get_raw(txn, 'nodes')
            k, ks = db.get_raw(txn, 'kids')
            self.assertEqual(rtrie.vid_for_value(n, k, 'hello'), 1)
            self.assertEqual(rtrie.vid_for_value(n, k, 'hell'), 2)
            self.assertEqual(rtrie.vid_for_value(n, k, 'goodbye'), 5)
            self.assertEqual(rtrie.vid_for_value(n, k, 'hellsink'), 6)
            self.assertEqual(rtrie.vid_for_value(n, k, 'hellothere'), 3)
            self.assertEqual(rtrie.vid_for_value(n, k, 'good'), 4)
            self.assertIsNone(rtrie.vid_for_value(n, k, 'notthere'))

            txn.commit()
            env.close()
        finally:
            import os
            os.unlink('/tmp/test_rtrie')
            os.unlink('/tmp/test_rtrie-lock')

