import unittest
from pylru import CharLRUDict, IntLRUDict


def Fetch(key):
    pass


def Evict(key, value):
    pass


class LruTest(unittest.TestCase):

    def test_basic_char(self):
        mdict = {}

        def fetch(key):
            try:
                res = mdict[key]
            except:
                return None
            return res

        def evict(key, value):
            mdict[key] = value

        l = CharLRUDict(10, fetch, evict)

        a = 100000
        b = 200000

        for i in range(a, b):
            l.set(str(i * i), i * i)

        for i in range(b - 1, a, -1):
            v = l.get(str(i * i))
            self.assertEqual(i * i, v)

    def test_basic_int(self):
        mdict = {}

        def fetch(key):
            try:
                res = mdict[key]
            except:
                return None
            return res

        def evict(key, value):
            mdict[key] = value

        l = IntLRUDict(10, fetch, evict)

        a = 100000
        b = 200000

        for i in range(a , b):
            l.set(i * i, i * i)

        for i in range(b - 1, a, -1):
            v = l.get(i * i)
            self.assertEqual(i * i, v)

    def test_no_eviction(self):
        def fetch(key):
            return None

        def evict(key, value):
            self.fail("Nothing should be evicted: " + str(key) + " " + str(value))

        l = CharLRUDict(1, fetch, evict, list)
        s = l["10"]
        self.assertListEqual(s, [])
