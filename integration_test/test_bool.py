import unittest
from disco.core import result_iterator
from hustle import select, Table, h_sum, h_count
from setup import IMPS, PIXELS
from hustle.core.settings import Settings, overrides


class TestBool(unittest.TestCase):
    def setUp(self):
        overrides['server'] = 'disco://localhost'
        overrides['dump'] = False
        overrides['nest'] = False
        self.settings = Settings()

    def tearDown(self):
        pass

    def test_project(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.click, imps.conversion, imps.impression, where=imps)
        clicks = conversions = impressions = 0
        for (click, conv, imp), _ in result_iterator(res):
            clicks += click
            conversions += conv
            impressions += imp

        self.assertEqual(clicks, 21)
        self.assertEqual(conversions, 5)
        self.assertEqual(impressions, 174)

    def test_aggregate(self):
        imps = Table.from_tag(IMPS)
        res = select(h_sum(imps.click), h_sum(imps.conversion), h_sum(imps.impression), where=imps)

        (clicks, conversions, impressions), _ = list(result_iterator(res))[0]

        self.assertEqual(clicks, 21)
        self.assertEqual(conversions, 5)
        self.assertEqual(impressions, 174)

    def test_bool_values(self):
        pix = Table.from_tag(PIXELS)
        res = select(pix.isActive, where=pix.isActive == True)
        actives = 0
        for (act, ), _ in result_iterator(res):
            actives += act

        self.assertEqual(actives, 234)

        res = select(pix.isActive, where=pix.isActive == 0)
        actives = 0
        for (act, ), _ in result_iterator(res):
            actives += 1

        self.assertEqual(actives, 266)

    def test_bit_values(self):
        pix = Table.from_tag(PIXELS)
        res = select(pix.isActive, where=pix.isActive == 1)
        actives = 0
        for (act, ), _ in result_iterator(res):
            actives += act

        self.assertEqual(actives, 234)



