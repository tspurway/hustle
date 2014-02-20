import unittest
from disco.core import result_iterator
from hustle import select, Table, h_sum, h_count
from setup import IMPS, PIXELS
from hustle.core.settings import Settings, overrides


class TestJoin(unittest.TestCase):
    def setUp(self):
        overrides['server'] = 'disco://localhost'
        overrides['dump'] = False
        overrides['nest'] = False
        self.settings = Settings()

    def tearDown(self):
        pass

    def test_simple_join(self):
        imps = Table.from_tag(IMPS)
        pix  = Table.from_tag(PIXELS)

        imp_sites = [(s, a) for (s, a), _ in result_iterator(select(imps.site_id, imps.ad_id,
                                                                    where=imps.date < '2014-01-13'))]
        pix_sites = [(s, a) for (s, a), _ in result_iterator(select(pix.site_id, pix.amount,
                                                                    where=pix.date < '2014-01-13'))]

        join = []
        for imp_site, imp_ad_id in imp_sites:
            for pix_site, pix_amount in pix_sites:
                if imp_site == pix_site:
                    join.append((imp_ad_id, pix_amount))

        res = select(imps.ad_id, pix.amount,
                     where=(imps.date < '2014-01-13', pix.date < '2014-01-13'),
                     join=(imps.site_id, pix.site_id),
                     order_by='amount')
        results = [(ad_id, amount) for (ad_id, amount), _ in result_iterator(res)]
        self.assertTrue(len(results), len(join))

        for jtup in join:
            self.assertIn(jtup, results)

        lowest = 0
        for ad_id, amount in results:
            self.assertLessEqual(lowest, amount)
            lowest = amount

    def test_nested_join(self):
        imps = Table.from_tag(IMPS)
        pix  = Table.from_tag(PIXELS)

        imp_sites = [(s, a) for (s, a), _ in result_iterator(select(imps.site_id, imps.ad_id,
                                                                    where=imps.date < '2014-01-13'))]
        pix_sites = [(s, a) for (s, a), _ in result_iterator(select(pix.site_id, pix.amount,
                                                                    where=((pix.date < '2014-01-13') &
                                                                           (pix.isActive > 0))))]

        join = []
        for imp_site, imp_ad_id in imp_sites:
            for pix_site, pix_amount in pix_sites:
                if imp_site == pix_site:
                    join.append((imp_ad_id, pix_amount))

        sub_pix = select(pix.site_id, pix.amount, pix.date,
                         where=((pix.date < '2014-01-15') & (pix.isActive > 0)),
                         nest=True)

        res = select(imps.ad_id, sub_pix.amount,
                     where=(imps.date < '2014-01-13', sub_pix.date < '2014-01-13'),
                     join=(imps.site_id, sub_pix.site_id))
        results = [(ad_id, amount) for (ad_id, amount), _ in result_iterator(res)]
        self.assertTrue(len(results), len(join))

        for jtup in join:
            self.assertIn(jtup, results)

    def test_aggregate_join(self):
        imps = Table.from_tag(IMPS)
        pix  = Table.from_tag(PIXELS)

        imp_sites = [(s, a) for (s, a), _ in result_iterator(select(imps.site_id, imps.ad_id,
                                                                    where=imps.date < '2014-01-13'))]
        pix_sites = [(s, a) for (s, a), _ in result_iterator(select(pix.site_id, pix.amount,
                                                                    where=pix.date < '2014-01-13'))]

        join = {}
        for imp_site, imp_ad_id in imp_sites:
            for pix_site, pix_amount in pix_sites:
                if imp_site == pix_site:
                    if imp_ad_id not in join:
                        join[imp_ad_id] = [0, 0]
                    join[imp_ad_id][0] += pix_amount
                    join[imp_ad_id][1] += 1

        res = select(imps.ad_id, h_sum(pix.amount), h_count(),
                     where=(imps.date < '2014-01-13', pix.date < '2014-01-13'),
                     join=(imps.site_id, pix.site_id))
        results = [(ad_id, amount, count) for (ad_id, amount, count), _ in result_iterator(res)]
        self.assertTrue(len(results), len(join))

        for (ad_id, amount, count) in results:
            ramount, rcount = join[ad_id]
            self.assertEqual(ramount, amount)
            self.assertEqual(rcount, count)

