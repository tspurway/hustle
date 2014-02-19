import unittest
from math import sqrt
from cardunion import Cardunion


class TestCardunion(unittest.TestCase):
    def setUp(self):
        self.log2m = 12
        self.error = 1.04 / sqrt(2 ** self.log2m)

    def test_mid_range_with_strings(self):
        self.execute(10000, self.log2m, self.error)

    def test_long_range_with_strings(self):
        self.execute(100000, self.log2m, self.error)

    def test_low_range_with_strings(self):
        self.execute(100, self.log2m, self.error)

    def execute(self, set_size, m, p):
        hll = Cardunion(m)
        for i in range(set_size):
            hll.add(str(i))

        estimate = hll.count()
        error = abs(estimate / float(set_size) - 1)
        self.assertLess(error, p)

    def test_with_duplicates(self):
        hll = Cardunion(self.log2m)
        set_size = 100000
        for i in range(set_size):
            if i % 3:
                hll.add(str(i + 1))
            else:
                hll.add(str(i))

        estimate = hll.count()
        expected = set_size * 2.0 / 3.0
        error = abs(estimate / float(expected) - 1)
        self.assertLess(error, self.error)

    def test_with_heavy_duplicates(self):
        hll = Cardunion(self.log2m)
        set_size = 100000
        for i in range(set_size):
            if i % 2 or i < set_size / 2:
                hll.add(str(1))
            else:
                hll.add(str(i))

        estimate = hll.count()
        expected = set_size * 1.0 / 4.0
        error = abs(estimate / float(expected) - 1)
        self.assertLess(error, self.error)

    def test_dumps(self):
        hll = Cardunion(self.log2m)
        hll_copy = Cardunion(self.log2m)
        for i in range(10000):
            hll.add(str(i))

        hll_copy.loads(hll.dumps())
        self.assertEqual(hll.count(), hll_copy.count())

    def test_sparse_dumps(self):
        hll = Cardunion(self.log2m)
        hll_copy = Cardunion(self.log2m)
        for i in range(500):
            hll.add(str(i))

        hll_copy.loads(hll.dumps())
        self.assertEqual(hll.count(), hll_copy.count())

    def test_union(self):
        hll = Cardunion(self.log2m)
        hll_1 = Cardunion(self.log2m)
        for i in range(10000):
            hll.add(str(i))
        for i in range(10000, 20000):
            hll_1.add(str(i))

        hll.union([hll_1])
        estimate = hll.count()
        error = abs(estimate / float(20000) - 1)
        self.assertLess(error, self.error)

    def test_bunion(self):
        hll = Cardunion(self.log2m)
        hll_1 = Cardunion(self.log2m)
        hll_2 = Cardunion(self.log2m)
        for i in range(10000):
            hll.add(str(i))
        for i in range(10000, 20000):
            hll_1.add(str(i))
        for i in range(20000, 30000):
            hll_2.add(str(i))

        hll.bunion([hll_1.dumps(), hll_2.dumps()])
        estimate = hll.count()
        error = abs(estimate / float(30000) - 1)
        self.assertLess(error, self.error)

    def test_intersect(self):
        """Since there is no theoretical error bound for intersection,
        we'd use 3-sigma rule instead.
        """
        hll = Cardunion()
        hll_1 = Cardunion()
        for i in range(10000):
            hll.add(str(i))
        for i in range(5000, 15000):
            hll_1.add(str(i))

        estimate, error, _ = Cardunion.intersect([hll_1, hll])
        print estimate, error
        self.assertTrue(5000 - 3 * error <= estimate <= 5000 + 3 * error)

    def test_intersect_big_small(self):
        hll = Cardunion()
        hll_1 = Cardunion()
        for i in range(50):
            hll.add(str(i))
        for i in range(1, 100000):
            hll_1.add(str(i))

        estimate, error, _ = Cardunion.intersect([hll_1, hll])
        print estimate, error
        self.assertTrue(50 - 3 * error <= estimate <= 50 + 3 * error)

    def test_intersect_a_few(self):
        hll = Cardunion()
        hll_1 = Cardunion()
        hll_2 = Cardunion()
        for i in range(5000):
            hll.add(str(i))
        for i in range(1, 100000):
            hll_1.add(str(i))
        for i in range(25, 1000):
            hll_2.add(str(i))

        estimate, error, _ = Cardunion.intersect([hll_2, hll_1, hll])
        print estimate, error
        self.assertTrue(975 - 3 * error <= estimate <= 975 + 3 * error)

    def test_intersect_a_lot(self):
        hlls = []
        actual = 100000
        nset = 10
        for i in range(nset):
            hll = Cardunion()
            for j in range(actual):
                hll.add(str(i * 5000 + j))
            hlls.append(hll)

        estimate, error, _ = Cardunion.intersect(hlls)
        print estimate, error
        self.assertTrue(actual - (nset - 1) * 5000 - 3 * error
                        <= estimate <= actual - (nset - 1) * 5000 + 3 * error)

    def test_nonzero_counters(self):
        h = Cardunion()
        h.update_counter(1, 2)
        h.update_counter(3, 4)
        h.update_counter(5, 8)
        self.assertEquals(list(h.nonzero_counters), [(1, 2), (3, 4), (5, 8)])
