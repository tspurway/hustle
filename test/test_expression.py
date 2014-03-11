import unittest
from hustle.core.marble import Column
from pyebset import BitSet


ZERO_BS = BitSet()
ZERO_BS.set(0)


class Tablet(object):
    def __init__(self, l=()):
        self.l = BitSet()
        for i in l:
            self.l.set(i)

    def iter_all(self):
        return iter(self.l)

    def bit_eq(self, col, other):
        b = BitSet()
        for i in self.l:
            if i == other:
                b.set(i)
        return b

    def bit_ne(self, col, other):
        b = BitSet()
        for i in self.l:
            if i != other:
                b.set(i)
        return b

    def bit_lt(self, col, other):
        b = BitSet()
        for i in self.l:
            if i < other:
                b.set(i)
        return b

    def bit_gt(self, col, other):
        b = BitSet()
        for i in self.l:
            if i > other:
                b.set(i)
        return b

    def bit_ge(self, col, other):
        b = BitSet()
        for i in self.l:
            if i >= other:
                b.set(i)
        return b

    def bit_le(self, col, other):
        b = BitSet()
        for i in self.l:
            if i <= other:
                b.set(i)
        return b

    def bit_eq_ex(self, col, keys):
        rval = BitSet()
        for i in self.l:
            if i in keys:
                rval.set(i)
        return rval

    def bit_ne_ex(self, col, keys):
        rval = BitSet()
        for i in self.l:
            if i not in keys:
                rval.set(i)
        return rval


class TestExpr(unittest.TestCase):
    def test_expr_without_partitions(self):
        cee_vals = Tablet([1, 5, 7, 9, 12, 13, 14, 19, 27, 38])
        cee = Column('cee', None, type_indicator=1, index_indicator=1, partition=False)

        ex = (cee < 8)
        self.assertEqual(list(ex(cee_vals)), [1, 5, 7])

        ex = (cee > 7)
        self.assertEqual(list(ex(cee_vals)), [9, 12, 13, 14, 19, 27, 38])

        ex = (cee <= 7)
        self.assertEqual(list(ex(cee_vals)), [1, 5, 7])

        ex = (cee >= 7)
        self.assertEqual(list(ex(cee_vals)), [7, 9, 12, 13, 14, 19, 27, 38])

        ex = (cee == 7)
        self.assertEqual(list(ex(cee_vals)), [7])

        ex = (cee != 7)
        self.assertEqual(list(ex(cee_vals)), [1, 5, 9, 12, 13, 14, 19, 27, 38])

        # test AND
        ex = (cee > 7) & (cee < 20)
        self.assertEqual(list(ex(cee_vals)), [9, 12, 13, 14, 19])

        ex = (cee > 7) & (cee < 20) & (cee > 13)
        self.assertEqual(list(ex(cee_vals)), [14, 19])

        # test OR
        ex = (cee < 7) | (cee > 20)
        x = sorted(ex(cee_vals))
        self.assertEqual(x, [1, 5, 27, 38])

        ex = (cee == 7) | (cee == 20) | (cee == 13)
        self.assertEqual(list(ex(cee_vals)), [7, 13])

        # test NOT
        ex = ~((cee >= 7) & (cee <= 20))
        x = sorted(ex(cee_vals))
        self.assertEqual(x, [1, 5, 27, 38])

        # test NOT
        ex = ~((cee < 7) | (cee == 19))
        x = sorted(ex(cee_vals))
        self.assertEqual(x, [7, 9, 12, 13, 14, 27, 38])

        # test in
        ex = (cee << [1, 5])
        self.assertEqual(list(ex(cee_vals)), [1, 5])
        ex = (cee << [1, 3, 5, 7])
        self.assertEqual(list(ex(cee_vals)), [1, 5, 7])
        ex = (cee << [1, 5, 7]) & (cee > 4)
        self.assertEqual(list(ex(cee_vals)), [5, 7])

        # test not in
        ex = (cee >> [1, 5, 7, 9])
        self.assertEqual(list(ex(cee_vals)), [12, 13, 14, 19, 27, 38])
        ex = (cee >> [1, 3, 5, 7, 9, 12, 15, 19])
        self.assertEqual(list(ex(cee_vals)), [13, 14, 27, 38])

        # test in & not in
        ex = (cee << [1, 5, 7]) & (cee >> [3, 7])
        self.assertEqual(list(ex(cee_vals)), [1, 5])
        ex = (cee << [1, 5, 7]) & (cee >> [3, 7]) & (cee >> [1, 5])
        self.assertEqual(list(ex(cee_vals)), [])
        ex = (cee << [1, 5, 7]) & (cee >> [3, 7]) | (cee >> [1, 5])
        self.assertEqual(list(ex(cee_vals)), [1, 5, 7, 9, 12, 13, 14, 19, 27, 38])
        ex = (cee << [1, 5, 7]) & (cee >> [3, 7]) | (cee == 9)
        self.assertEqual(list(ex(cee_vals)), [1, 5, 9])

    def test_expr_with_partitions(self):
        pee = Column('pee', None, type_indicator=1, index_indicator=1, partition=True)
        pee_tags = [1, 5, 7, 9, 12, 13, 14, 19, 27, 38]
        cee = Column('cee', None, type_indicator=1, index_indicator=1, partition=False)

        p_and_p = (pee < 7)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [1, 5])

        p_and_p = (pee > 7)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [9, 12, 13, 14, 19, 27, 38])

        p_and_p = (pee == 7)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [7])

        p_and_p = (pee != 7)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [1, 5, 9, 12, 13, 14, 19, 27, 38])

        p_and_p = (pee >= 7)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [7, 9, 12, 13, 14, 19, 27, 38])

        p_and_p = (pee <= 7)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [1, 5, 7])

        p_and_p = ~(pee > 7)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [1, 5, 7])

        # test pure partition combination
        p_and_p = (pee > 5) | (pee == 1)
        self.assertEqual(sorted(p_and_p.partition(pee_tags)), [1, 7, 9, 12, 13, 14, 19, 27, 38])

        p_and_p = ~((pee <= 5) | (pee > 14))
        self.assertEqual(list(p_and_p.partition(pee_tags)), [7, 9, 12, 13, 14])

        p_and_p = (pee == 5) | (pee == 99)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [5])

        p_and_p = (pee > 5) & (pee <= 14) & (pee > 12)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [13, 14])

        p_and_p = ((pee > 5) & (pee <= 14)) | (pee == 5)
        x = sorted(p_and_p.partition(pee_tags))
        self.assertEqual(x, [5, 7, 9, 12, 13, 14])

        p_and_p = ~(~(((pee > 5) & (pee <= 14))) & (pee != 5))
        x = sorted(p_and_p.partition(pee_tags))
        self.assertEqual(x, [5, 7, 9, 12, 13, 14])

        p_and_p = ~(((pee <= 5) | (pee > 14)) & (pee != 5))
        x = sorted(p_and_p.partition(pee_tags))
        self.assertEqual(x, [5, 7, 9, 12, 13, 14])

        # test combined partition/index combinations
        # p & c == p
        p_and_p = (pee > 5) & (cee <= 14)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [7, 9, 12, 13, 14, 19, 27, 38])

        # test combined partition/index combinations
        # p & ~c == p
        p_and_p = (pee > 5) & ~(cee <= 14)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [7, 9, 12, 13, 14, 19, 27, 38])
        p_and_p = (pee > 5) & ~~(cee <= 14)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [7, 9, 12, 13, 14, 19, 27, 38])

        # p & ~c & ~c == p
        p_and_p = (pee > 5) & ~(cee <= 14) & ~(cee >= 5)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [7, 9, 12, 13, 14, 19, 27, 38])
        p_and_p = (cee > 5) & ~((pee > 5) & ~(cee <= 14))
        self.assertEqual(list(p_and_p.partition(pee_tags)), [1, 5])
        p_and_p = (cee > 5) & (~(pee > 5) & ~(cee <= 14))
        self.assertEqual(list(p_and_p.partition(pee_tags)), [1, 5])
        p_and_p = (cee > 5) & ~(~(pee > 5) & ~(cee <= 14))
        self.assertEqual(list(p_and_p.partition(pee_tags)), [7, 9, 12, 13, 14, 19, 27, 38])
        p_and_p = (cee > 5) & ~~((pee > 5) & ~(cee <= 14))
        self.assertEqual(list(p_and_p.partition(pee_tags)), [7, 9, 12, 13, 14, 19, 27, 38])
        p_and_p = (cee > 5) & ~((pee > 5) | ~(cee <= 14))
        self.assertEqual(list(p_and_p.partition(pee_tags)), pee_tags)
        p_and_p = (cee > 5) & ~~((pee > 5) | ~(cee <= 14))
        self.assertEqual(list(p_and_p.partition(pee_tags)), pee_tags)

        # p & ~c | ~c == all
        p_and_p = (pee > 5) & ~(cee <= 14) | ~(cee >= 5)
        self.assertEqual(list(p_and_p.partition(pee_tags)), pee_tags)

        # ~c & ~c & p == p
        p_and_p = ~(cee <= 14) & ~(cee >= 5) & (pee > 5)
        self.assertEqual(list(p_and_p.partition(pee_tags)), [7, 9, 12, 13, 14, 19, 27, 38])

        # p | c == universe
        p_and_p = (pee == 5) | (pee == 8) | (cee == 99)
        x = list(p_and_p.partition(pee_tags))
        self.assertEqual(x, pee_tags)

        p_and_p = (pee == 5) | (pee == 8) | (cee == 99)
        x = list(p_and_p.partition(pee_tags))
        self.assertEqual(x, pee_tags)

        # p | c == universe
        p_and_p = ((pee == 5) | (pee > 14)) | (cee > 12)
        self.assertEqual(list(p_and_p.partition(pee_tags)), pee_tags)

        # c & p == p ==> p | p
        p_and_p = ((pee == 5) | (pee > 14)) | ((cee > 12) & (pee == 1))
        self.assertEqual(sorted(p_and_p.partition(pee_tags)), [1, 5, 19, 27, 38])
