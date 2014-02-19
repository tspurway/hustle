import unittest
from hustle.core.pipeworker import merge_wrapper


class TestMarble(unittest.TestCase):
    def test_up(self):
        a = [(('keya', 5), 22), (('lima', 9), 23), (('oebra', 21), 24), (('qeya', 5), 22), (('tima', 9), 23), (('zebra', 21), 24)]
        b = [(('aeya', 5), 22), (('fima', 12), 23), (('hebra', 8), 24), (('xya', 5), 22), (('yima', 12), 23), (('zzebra', 8), 24)]
        c = [(('beya', 5), 22), (('fliea', 9), 23), (('gray', 21), 24), (('morie', 5), 22), (('steel', 9), 23), (('yale', 21), 24)]
        d = [(('vera', 5), 22), (('wera', 12), 23), (('xera', 8), 24), (('yolanda', 5), 22), (('yolo', 12), 23), (('zanadu', 8), 24)]
        from heapq import merge
        res = merge(merge_wrapper(a), merge_wrapper(b), merge_wrapper(c), merge_wrapper(d))
        lowest = 'aaaaaa'
        for k, v in res:
            self.assertTrue(lowest < k[0])
            lowest = k[0]

    def test_down(self):
        a = [(('zebra', 21), 24), (('lima', 9), 23), (('keya', 5), 22), ]
        b = [(('zzebra', 8), 24), (('sima', 12), 23), (('aeya', 5), 22)]
        from heapq import merge
        res = merge(merge_wrapper(a, desc=True), merge_wrapper(b, desc=True))
        highest = 'zzzzzzzzz'
        for k, v in res:
            self.assertTrue(highest > k[0])
            highest = k[0]

    def test_nulls(self):
        a = [(('zebra', 21), 24), (('keya', 5), 22), ((None, 9), 23)]
        b = [(('zzebra', 8), 24), (('sima', 12), 23), (('aeya', 5), 22), ((None, 12), 18)]
        from heapq import merge
        res = merge(merge_wrapper(a, desc=True), merge_wrapper(b, desc=True))
        highest = 'zzzzzzzzz'
        for k, v in res:
            print k, v
            self.assertTrue(highest >= k[0])
            highest = k[0]

    def test_multi(self):
        a = [(('zebra', 12), 24), (('webra', 12), 24), (('bebra', 12), 24), (('aebra', 12), 24), (('zebra', 11), 24), (('keya', 5), 22), (('aeya', 5), 22), ]
        b = [(('sima', 12), 23), (('zzebra', 8), 28), (('yzebra', 8), 28), (('azebra', 8), 28), (('aeya', 5), 22)]
        from heapq import merge
        res = merge(merge_wrapper(a, sort_range=(1, 0), desc=True), merge_wrapper(b, sort_range=(1, 0), desc=True))
        highest = 999999999
        highest_2nd = 'zzzzzzzz'
        same_count = 0
        for k, v in res:
            print "kev", k, v
            if highest == k[1]:
                self.assertTrue(highest_2nd >= k[0])
                same_count += 1
            self.assertGreaterEqual(highest, k[1])
            highest = k[1]
            highest_2nd = k[0]
        self.assertEqual(same_count, 8)

    def test_lopsided(self):
        a = [(('zebra', 21), 24)]
        b = [(('zzebra', 8), 24), (('sima', 12), 23), (('aeya', 5), 22)]
        from heapq import merge
        res = merge(merge_wrapper(a, desc=True), merge_wrapper(b, desc=True))
        highest = 'zzzzzzzzz'
        for k, v in res:
            self.assertTrue(highest > k[0])
            highest = k[0]




