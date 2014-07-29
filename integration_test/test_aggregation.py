import unittest
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
        count = list(res)[0][0]
        res.purge()
        self.assertEqual(count, 200)

    def test_simple_aggregation(self):
        imps = Table.from_tag(IMPS)
        results = select(imps.ad_id, imps.cpm_millis, where=imps.date == '2014-01-27')

        sum_millis = {}
        for ad_id, millis in results:
            if ad_id not in sum_millis:
                sum_millis[ad_id] = [0, 0]
            sum_millis[ad_id][0] += millis
            sum_millis[ad_id][1] += 1
        results.purge()

        results = select(imps.ad_id, h_sum(imps.cpm_millis), h_count(), where=imps.date == '2014-01-27')
        self.assertGreater(len(list(results)), 0)
        for ad_id, millis, count in results:
            ad_tup = sum_millis[ad_id]
            self.assertEqual(millis, ad_tup[0])
            self.assertEqual(count, ad_tup[1])
        results.purge()

    def test_ordered_aggregation(self):
        imps = Table.from_tag(IMPS)
        resx = select(imps.ad_id, imps.cpm_millis, where=imps.date == '2014-01-27')

        sum_millis = {}
        for ad_id, millis in resx:
            if ad_id not in sum_millis:
                sum_millis[ad_id] = [0, 0]
            sum_millis[ad_id][0] += millis
            sum_millis[ad_id][1] += 1

        results = select(imps.ad_id, h_sum(imps.cpm_millis), h_count(),
                         where=imps.date == '2014-01-27',
                         order_by=2,
                         limit=3,
                         nest=True)
        self.assertGreater(len(list(results)), 0)
        lowest = 0
        for ad_id, millis, count in results:
            self.assertLessEqual(lowest, count)
            lowest = count
            ad_tup = sum_millis[ad_id]
            self.assertEqual(millis, ad_tup[0])
            self.assertEqual(count, ad_tup[1])
        self.assertEqual(len(list(results)), min(len(sum_millis), 3))

        resx.purge()

    def test_multiple_group_bys(self):
        imps = Table.from_tag(IMPS)
        results = select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.date > '2014-01-22')

        sum_millis = {}
        for ad_id, dt, millis in results:
            key = str(ad_id) + dt
            if key not in sum_millis:
                sum_millis[key] = [0, 0]
            sum_millis[key][0] += millis
            sum_millis[key][1] += 1
        results.purge()

        results = select(imps.ad_id, imps.date, h_sum(imps.cpm_millis), h_count(), where=imps.date > '2014-01-22')
        self.assertGreater(len(list(results)), 0)
        for ad_id, dt, millis, count in results:
            ad_tup = sum_millis[str(ad_id) + dt]
            self.assertEqual(millis, ad_tup[0])
            self.assertEqual(count, ad_tup[1])
        results.purge()

    def test_nested_agg(self):
        imps = Table.from_tag(IMPS)
        results = select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.date > '2014-01-22')

        sum_millis = {}
        for ad_id, dt, millis in results:
            key = str(ad_id) + dt
            if key not in sum_millis:
                sum_millis[key] = [0, 0]
            sum_millis[key][0] += millis
            sum_millis[key][1] += 1
        results.purge()

        newtab = select(imps.ad_id, imps.date, h_sum(imps.cpm_millis), h_count(),
                        where=imps.date > '2014-01-22',
                        nest=True)
        results = select(*star(newtab), where=newtab)
        self.assertGreater(len(list(results)), 0)
        for ad_id, dt, millis, count in results:
            ad_tup = sum_millis[str(ad_id) + dt]
            self.assertEqual(millis, ad_tup[0])
            self.assertEqual(count, ad_tup[1])
        results.purge()

    def test_overflow(self):
        from itertools import izip

        imps = Table.from_tag(IMPS)
        fly_results = select(imps.date, h_sum(imps.impression), where=imps, order_by=imps.date)

        nest_tab = select(imps.date, h_sum(imps.impression), where=imps, nest=True)
        nest_results = select(*star(nest_tab), where=nest_tab, order_by=0)

        for ((fdate, fimps), (ndate, nimps)) in izip(fly_results, nest_results):
            self.assertEqual(fdate, ndate)
            self.assertEqual(fimps, nimps)
        nest_results.purge()

