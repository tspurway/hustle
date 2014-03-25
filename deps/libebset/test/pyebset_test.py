import unittest
from pyebset import BitSet


class BitSetTest(unittest.TestCase):
    """Docstring for BitSetTest """

    def test_set(self):
        b = BitSet()
        self.assertTrue(b.set(0))
        self.assertTrue(b.set(1))
        self.assertTrue(b.set(2))
        self.assertTrue(b.set(3))
        self.assertFalse(b.set(1))

    def test_dumps_loads(self):
        b = BitSet()
        self.assertTrue(b.set(0))
        self.assertTrue(b.set(1))
        self.assertTrue(b.set(4))
        self.assertTrue(b.set(8))
        self.assertTrue(b.set(16))
        s = BitSet()
        s.loads(b.dumps())
        self.assertEqual(b, s)

    def test_logical_ops(self):
        b = BitSet()
        b.set(0)
        b.set(1)
        b.set(4)
        b.set(8)
        b.set(16)
        bb = BitSet()
        bb.set(0)
        bb.set(1)
        bb.set(4)
        bb.set(9)
        cc = BitSet()
        cc.set(0)
        cc.set(1)
        cc.set(4)
        cc.set(8)
        cc.set(9)
        cc.set(16)
        dd = BitSet()
        dd.set(0)
        dd.set(1)
        dd.set(4)
        ee = BitSet()
        ee.set(2)
        ee.set(3)

        la = b & bb
        lo = b | bb
        ln = ~ dd
        ll = ~ ln
        self.assertEqual(lo, cc)
        self.assertNotEqual(la, dd)
        self.assertEqual(list(ln), list(ee))
        self.assertEqual(len(b), 5)
        self.assertEqual(len(bb), 4)
        self.assertEqual(len(cc), 6)
        self.assertEqual(len(dd), 3)
        self.assertEqual(len(ee), 2)
        self.assertEqual(len(la), 3)
        self.assertEqual(len(lo), 6)
        self.assertEqual(len(ln), 2)
        self.assertEqual(len(ll), 3)

    def test_logical_not(self):
        b = BitSet()
        b.set(0)
        b.set(1)
        b.set(8)
        b.set(9)
        c = ~b
        # test the logical not doesn't generate any numbers that are greater
        # than 9 in this case
        self.assertEqual(list(c), [2, 3, 4, 5, 6, 7])
        d = ~c
        self.assertListEqual(list(d), [0, 1, 8, 9])

    def test_logical_not_1(self):
        b = BitSet()
        b.set(0)
        b.set(1)
        b.set(7)
        b.set(8)
        c = ~b
        # test the logical not doesn't generate any numbers that are greater
        # than 9 in this case
        self.assertEqual(list(c), [2, 3, 4, 5, 6])
        d = ~c
        self.assertListEqual(list(d), [0, 1, 7, 8])

    def test_generator(self):
        b = BitSet()
        b.set(1)
        b.set(4)
        b.set(10)
        b.set(100000)
        b.set(12323131)
        self.assertEqual(list(b), [1, 4, 10, 100000, 12323131])

    def test_contains(self):
        b = BitSet()
        b.set(1)
        b.set(4)
        b.set(10)
        b.set(100000)
        b.set(12323131)
        for i in [1, 4, 10, 100000, 12323131]:
            self.assertTrue(i in b)

    def test_eq_ne(self):
        b = BitSet()
        b.set(1)
        b.set(2)
        bb = BitSet()
        bb.set(1)
        bb.set(2)
        cc = BitSet()
        cc.set(2)
        cc.set(3)
        self.assertTrue(b == bb)
        self.assertTrue(bb != cc)
