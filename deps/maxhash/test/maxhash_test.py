import unittest
from maxhash import MinHeap, MaxHash


class TestMinHeap(unittest.TestCase):
    def test_pop(self):
        m = MinHeap(1024, (3, 5, 2, 1))
        self.assertEqual(m.pop(), 1)
        self.assertEqual(m.pop(), 2)
        self.assertEqual(m.pop(), 3)
        self.assertEqual(m.pop(), 5)

    def test_push(self):
        m = MinHeap(1024, ())
        m.push(1)
        m.push(3)
        m.push(2)
        self.assertEqual(m.pop(), 1)
        self.assertEqual(m.pop(), 2)
        self.assertEqual(m.pop(), 3)
        m.push(1)
        m.push(3)
        m.push(2)
        m.push(4)
        m.push(6)
        m.push(5)
        self.assertEqual(m.pop(), 1)
        self.assertEqual(m.pop(), 2)
        self.assertEqual(m.pop(), 3)
        self.assertEqual(m.pop(), 4)
        self.assertEqual(m.pop(), 5)
        self.assertEqual(m.pop(), 6)

    def test_nlargest(self):
        m = MinHeap(1024, [1, 2, 3, 4, 2, 1, 5, 6])
        l = list(m.nlargest(3))
        l.sort()
        self.assertEqual(l, [4, 5, 6])


class TestMaxHash(unittest.TestCase):
    def test_add(self):
        m = MaxHash(8192)
        m.add(str(1))
        m.add(str(2))
        m.add(str(3))
        m.add(str(4))
        self.assertEqual(len(m.uniq()), 4)

    def test_merge(self):
        r1 = range(10000)
        m1 = MaxHash(8192)
        r2 = range(2000, 12000)
        m2 = MaxHash(8192)
        r3 = range(15000)
        m3 = MaxHash(8192)
        for i in r1:
            m1.add(str(i))
        for i in r2:
            m2.add(str(i))
        for i in r3:
            m3.add(str(i))
        m2.merge(m1)
        ix = MaxHash.get_jaccard_index([m2, m3])
        self.assertAlmostEqual(ix, 0.80, 2)

    def test_union(self):
        r1 = range(10000)
        m1 = MaxHash(8192)
        r2 = range(2000, 12000)
        m2 = MaxHash(8192)
        r3 = range(15000)
        m3 = MaxHash(8192)
        for i in r1:
            m1.add(str(i))
        for i in r2:
            m2.add(str(i))
        for i in r3:
            m3.add(str(i))
        m4 = m1.union(m2)
        ix = MaxHash.get_jaccard_index([m3, m4])
        self.assertAlmostEqual(ix, 0.80, 2)

    def test_jarcard_index(self):
        r1 = range(10000)
        m1 = MaxHash(8192)
        r2 = range(2000, 10000)
        m2 = MaxHash(8192)
        for i in r1:
            m1.add(str(i))
        for i in r2:
            m2.add(str(i))
        ix = MaxHash.get_jaccard_index([m1, m2])
        self.assertAlmostEqual(ix, 0.80, 2)
