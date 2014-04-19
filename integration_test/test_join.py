import unittest
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
        pix = Table.from_tag(PIXELS)

        imp_sites = [(s, a) for (s, a) in select(imps.site_id, imps.ad_id,
                                                 where=imps.date < '2014-01-13')]
        pix_sites = [(s, a) for (s, a) in select(pix.site_id, pix.amount,
                                                 where=pix.date < '2014-01-13')]

        join = []
        for imp_site, imp_ad_id in imp_sites:
            for pix_site, pix_amount in pix_sites:
                if imp_site == pix_site:
                    join.append((imp_ad_id, pix_amount))

        res = select(imps.ad_id, pix.amount,
                     where=(imps.date < '2014-01-13', pix.date < '2014-01-13'),
                     join=(imps.site_id, pix.site_id),
                     order_by='amount')
        results = list(res)
        self.assertEqual(len(results), len(join))

        for jtup in join:
            self.assertIn(jtup, results)

        lowest = 0
        for ad_id, amount in results:
            self.assertLessEqual(lowest, amount)
            lowest = amount

    def test_nested_join(self):
        imps = Table.from_tag(IMPS)
        pix = Table.from_tag(PIXELS)

        imp_sites = list(select(imps.site_id, imps.ad_id,
                                where=imps.date < '2014-01-13'))
        pix_sites = list(select(pix.site_id, pix.amount,
                                where=((pix.date < '2014-01-13') &
                                       (pix.isActive == True))))

        join = []
        for imp_site, imp_ad_id in imp_sites:
            for pix_site, pix_amount in pix_sites:
                if imp_site == pix_site:
                    join.append((imp_ad_id, pix_amount))

        sub_pix = select(pix.site_id, pix.amount, pix.date,
                         where=((pix.date < '2014-01-15') & (pix.isActive == True)),
                         nest=True)

        res = select(imps.ad_id, sub_pix.amount,
                     where=(imps.date < '2014-01-13', sub_pix.date < '2014-01-13'),
                     join=(imps.site_id, sub_pix.site_id))
        results = [tuple(c) for c in res]
        self.assertEqual(len(results), len(join))

        for jtup in join:
            self.assertIn(jtup, results)

    def test_nested_self_join(self):
        """
        A self join is joining the table against itself.  This requires the use of aliases.
        """
        imps = Table.from_tag(IMPS)

        early = list(select(imps.ad_id, imps.cpm_millis,
                            where=imps.date < '2014-01-20'))
        late = list(select(imps.ad_id, imps.cpm_millis,
                           where=imps.date >= '2014-01-20'))

        join = {}
        for eid, ecpm in early:
            for lid, lcpm in late:
                if eid == lid:
                    if eid not in join:
                        join[eid] = [0, 0, 0]
                    join[eid][0] += ecpm
                    join[eid][1] += lcpm
                    join[eid][2] += 1

        early = select(imps.ad_id, imps.cpm_millis, where=imps.date < '2014-01-20', nest=True)
        late = select(imps.ad_id, imps.cpm_millis, where=imps.date >= '2014-01-20', nest=True)
        jimmy = select(early.ad_id.named('adididid'),
                       h_sum(early.cpm_millis).named('emillis'),
                       h_sum(late.cpm_millis).named('lmillis'),
                       h_count(),
                       where=(early, late),
                       join='ad_id')

        james = list(jimmy)
        self.assertEqual(len(join), len(james))

        for (ad_id, emillis, lmillis, cnt) in james:
            ecpm, lcpm, ocnt = join[ad_id]
            self.assertEqual(emillis, ecpm)
            self.assertEqual(lmillis, lcpm)
            self.assertEqual(cnt, ocnt)


    def test_aggregate_join(self):
        imps = Table.from_tag(IMPS)
        pix = Table.from_tag(PIXELS)

        imp_sites = list(select(imps.site_id, imps.ad_id,
                                where=imps.date < '2014-01-13'))
        pix_sites = list(select(pix.site_id, pix.amount,
                                where=pix.date < '2014-01-13'))

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
        results = list(res)
        self.assertEqual(len(results), len(join))

        for (ad_id, amount, count) in results:
            ramount, rcount = join[ad_id]
            self.assertEqual(ramount, amount)
            self.assertEqual(rcount, count)

