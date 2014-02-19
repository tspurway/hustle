import unittest
from disco.core import result_iterator
from hustle import select, Table
from setup import IMPS
from hustle.core.settings import Settings, overrides


class TestProjectOrder(unittest.TestCase):
    def setUp(self):
        overrides['server'] = 'disco://localhost'
        overrides['dump'] = False
        overrides['nest'] = False
        self.settings = Settings()

    def tearDown(self):
        pass

    def test_single_int_order(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.date == '2014-01-27', order_by=imps.cpm_millis)
        lowest = 0
        for (a, d, c), _ in result_iterator(res):
            self.assertLessEqual(lowest, c)
            lowest = c

    def test_combo_order(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis,
                     where=imps.date > '2014-01-21',
                     order_by=(imps.date, imps.cpm_millis))
        lowest_cpm = 0
        lowest_date = '2000-01-01'
        for (a, d, c), _ in result_iterator(res):
            if lowest_date == d:
                self.assertLessEqual(lowest_cpm, c)
                lowest_cpm = c
            else:
                self.assertLessEqual(lowest_date, d)
                lowest_date = d
                lowest_cpm = c

    def test_combo_descending(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis,
                     where=imps.date > '2014-01-21',
                     order_by=(imps.date, imps.cpm_millis),
                     desc=True)
        highest_cpm = 1000000000
        highest_date = '2222-01-01'
        for (a, d, c), _ in result_iterator(res):
            if highest_date == d:
                self.assertGreaterEqual(highest_cpm, c)
                highest_cpm = c
            else:
                self.assertGreaterEqual(highest_date, d)
                highest_date = d
                highest_cpm = c

    def test_high_limit(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.date == '2014-01-27', limit=100)
        results = [c for c, _ in result_iterator(res)]
        self.assertEqual(len(results), 10)

    def test_low_limit(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.date == '2014-01-27', limit=4)
        results = [c for c, _ in result_iterator(res)]
        self.assertEqual(len(results), 4)

    def test_distinct(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, where=imps.date == '2014-01-27', distinct=True)
        results = [c for c, _ in result_iterator(res)]
        self.assertEqual(len(results), 8)

    def test_overall(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, where=imps.date == '2014-01-27', distinct=True, limit=4,
                     order_by='ad_id', desc=True)
        results = [a for (a, d), _ in result_iterator(res)]
        self.assertEqual(len(results), 4)
        self.assertListEqual(results, [30019, 30018, 30017, 30015])
