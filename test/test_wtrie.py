import struct
import unittest
from wtrie import Trie


class TestWTrie(unittest.TestCase):
    def test_wtrie(self):
        t = Trie()
        self.assertEqual(t.add('hello'), 1)
        self.assertEqual(t.add('hell'), 2)
        self.assertEqual(t.add('hello'), 1)
        self.assertEqual(t.add('hellothere'), 3)
        self.assertEqual(t.add('good'), 4)
        self.assertEqual(t.add('goodbye'), 5)
        self.assertEqual(t.add('hello'), 1)
        self.assertEqual(t.add('hellsink'), 6)
        self.assertEqual(t.add(''), 0)

        # nodes = t.nodes
        # t.print_it()

        key, sz, pt = t.node_at_path()
        self.assertEqual(sz, 2)

        key, sz, pt = t.node_at_path(104)
        self.assertEqual(key, 'hell')
        self.assertEqual(pt, 0)
        self.assertEqual(sz, 2, 'actual %s' % sz)

        key2, sz, pt = t.node_at_path(104, 111)
        self.assertEqual(key2, 'o', 'actual %s' % key)
        self.assertEqual(pt, 2)
        self.assertEqual(sz, 1)

        key, sz, pt = t.node_at_path(104, 111, 116)
        self.assertEqual(key, 'there')
        self.assertEqual(pt, 1)
        self.assertEqual(sz, 0)

        n, k, _ = t.serialize()
        self.assertEqual(len(n), 7 * 4, "actual %d" % len(n))
        self.assertEqual(len(k), 100, "actual %d" % len(k))
        # print "sqork: %s" % t.kid_space

        print 'nodes', n
        print 'kids', k

        unpacked = struct.unpack_from("7I", n, 0)
        expected = (0x02000000, 0x01000010, 0x0200000b, 0x00000013, 0x01000004, 0x00000008, 0x00000016)
        self.assertEqual(unpacked, expected, 'actual %s' % str(unpacked))

        unpacked = struct.unpack_from("IH2I", k, 0)
        expected = (0, 0, 0x67000004, 0x68000002)
        self.assertEqual(unpacked, expected, unpacked)

        unpacked = struct.unpack_from("IH4cI", k, 16)
        expected = (0x0000, 0x0004, 'g', 'o', 'o', 'd', 0x62000005)
        self.assertEqual(unpacked, expected, 'actual %s' % str(unpacked))

        unpacked = struct.unpack_from("IH3c", k, 32)
        expected = (0x0004, 0x0003, 'b', 'y', 'e')
        self.assertEqual(unpacked, expected, 'actual %s' % str(unpacked))

        unpacked = struct.unpack_from("IH4c2I", k, 44)
        expected = (0x0000, 0x0004, 'h', 'e', 'l', 'l', 0x6f000001, 0x73000006)
        self.assertEqual(unpacked, expected, 'actual %s' % str(unpacked))

        unpacked = struct.unpack_from("IHcI", k, 64)
        expected = (0x0002, 1, 'o', 0x74000003)
        self.assertEqual(unpacked, expected, 'actual %s' % str(unpacked))

        unpacked = struct.unpack_from("IH5c", k, 76)
        expected = (0x0001, 0x0005, 't', 'h', 'e', 'r', 'e')
        self.assertEqual(unpacked, expected, 'actual %s' % str(unpacked))

        unpacked = struct.unpack_from("IH4c", k, 88)
        expected = (0x0002, 0x0004, 's', 'i', 'n', 'k')
        self.assertEqual(unpacked, expected, 'actual %s' % str(unpacked))
