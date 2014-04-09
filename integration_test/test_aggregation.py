import unittest
from disco.core import result_iterator
from hustle import select, Table, h_sum, h_count, star
from setup import IMPS
from hustle.core.settings import Settings, overrides


class TestAggregation(unittest.TestCase):
    def setUp(self):
        overrides['server'] = 'disco://localhost'
        overrides['dump'] = False
        overrides['nest'] = False
        self.settings = Settings()

    def tearDown(self):
        pass

    def test_count(self):
        imps = Table.from_tag(IMPS)
        res = select(h_count(), where=imps)
        count = list(result_iterator(res))[0][0][0]
        self.assertEqual(count, 200)

    def test_simple_aggregation(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.cpm_millis, where=imps.date == '2014-01-27')
        results = [c for c, _ in result_iterator(res)]

        sum_millis = {}
        for ad_id, millis in results:
            if ad_id not in sum_millis:
                sum_millis[ad_id] = [0, 0]
            sum_millis[ad_id][0] += millis
            sum_millis[ad_id][1] += 1

        res = select(imps.ad_id, h_sum(imps.cpm_millis), h_count(), where=imps.date == '2014-01-27')
        results = [c for c, _ in result_iterator(res)]
        self.assertGreater(len(results), 0)
        for ad_id, millis, count in results:
            ad_tup = sum_millis[ad_id]
            self.assertEqual(millis, ad_tup[0])
            self.assertEqual(count, ad_tup[1])

    def test_ordered_aggregation(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.cpm_millis, where=imps.date == '2014-01-27')
        resx = [c for c, _ in result_iterator(res)]

        sum_millis = {}
        for ad_id, millis in resx:
            if ad_id not in sum_millis:
                sum_millis[ad_id] = [0, 0]
            sum_millis[ad_id][0] += millis
            sum_millis[ad_id][1] += 1

        res = select(imps.ad_id, h_sum(imps.cpm_millis), h_count(),
                     where=imps.date == '2014-01-27',
                     order_by=2,
                     limit=3)
        results = [c for c, _ in result_iterator(res)]
        self.assertGreater(len(results), 0)
        lowest = 0
        for ad_id, millis, count in results:
            self.assertLessEqual(lowest, count)
            lowest = count
            ad_tup = sum_millis[ad_id]
            self.assertEqual(millis, ad_tup[0])
            self.assertEqual(count, ad_tup[1])
        self.assertTrue(len(results) == min(len(sum_millis), 3))

    def test_multiple_group_bys(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.date > '2014-01-22')
        results = [c for c, _ in result_iterator(res)]

        sum_millis = {}
        for ad_id, dt, millis in results:
            key = str(ad_id) + dt
            if key not in sum_millis:
                sum_millis[key] = [0, 0]
            sum_millis[key][0] += millis
            sum_millis[key][1] += 1

        res = select(imps.ad_id, imps.date, h_sum(imps.cpm_millis), h_count(), where=imps.date > '2014-01-22')
        results = [c for c, _ in result_iterator(res)]
        self.assertGreater(len(results), 0)
        for ad_id, dt, millis, count in results:
            ad_tup = sum_millis[str(ad_id) + dt]
            self.assertEqual(millis, ad_tup[0])
            self.assertEqual(count, ad_tup[1])

    def test_nested_agg(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.date > '2014-01-22')
        results = [c for c, _ in result_iterator(res)]

        sum_millis = {}
        for ad_id, dt, millis in results:
            key = str(ad_id) + dt
            if key not in sum_millis:
                sum_millis[key] = [0, 0]
            sum_millis[key][0] += millis
            sum_millis[key][1] += 1

        newtab = select(imps.ad_id, imps.date, h_sum(imps.cpm_millis), h_count(),
                        where=imps.date > '2014-01-22',
                        nest=True)
        res = select(*star(newtab), where=newtab)
        results = [c for c, _ in result_iterator(res)]
        self.assertGreater(len(results), 0)
        for ad_id, dt, millis, count in results:
            ad_tup = sum_millis[str(ad_id) + dt]
            self.assertEqual(millis, ad_tup[0])
            self.assertEqual(count, ad_tup[1])
