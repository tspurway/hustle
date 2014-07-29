import unittest
from hustle import select, Table
from setup import IMPS
from hustle.core.settings import Settings, overrides


class TestSimpleQuery(unittest.TestCase):
    def setUp(self):
        overrides['server'] = 'disco://localhost'
        overrides['dump'] = False
        overrides['nest'] = False
        self.settings = Settings()

    def tearDown(self):
        pass

    def test_equality_on_partition(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.date == '2014-01-27')
        results = list(res)
        self.assertEqual(len(results), 10)
        found = next((a, d, c) for a, d, c in results if a == 30018 and d == '2014-01-27' and c == 4506)
        self.assertIsNotNone(found)
        self.assertTrue(all(d == '2014-01-27' for _, d, _ in results))
        res.purge()

    def test_range_on_partition(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.date > '2014-01-27')
        results = list(res)
        self.assertEqual(len(results), 20)
        self.assertTrue(all(d in ('2014-01-28', '2014-01-29') for _, d, _ in results))
        res.purge()

    def test_combo_where_on_partition(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis,
                     where=((imps.date >= '2014-01-20') & (imps.ad_id == 30010)))
        results = list(res)
        self.assertEqual(len(results), 6)
        self.assertTrue(all(d >= '2014-01-20' and a == 30010 for a, d, _ in results))
        res.purge()

    def test_combo_where_on_or_partition(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis,
                     where=((imps.date == '2014-01-21') | (imps.date == '2014-01-25') | (imps.ad_id == 30010)))
        results = list(res)
        self.assertEqual(len(results), 27)
        self.assertTrue(all(d == '2014-01-21' or d == '2014-01-25' or a == 30010 for a, d, _ in results))
        res.purge()

    def test_combo_where_on_or_partition_ex(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis,
                     where=((imps.date << ['2014-01-21', '2014-01-25']) | (imps.ad_id == 30010)))
        results = list(res)
        self.assertEqual(len(results), 27)
        self.assertTrue(all(d == '2014-01-21' or d == '2014-01-25' or a == 30010 for a, d, _ in results))
        res.purge()

    def test_combo_where_on_or_partition_ex1(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis,
                     where=((imps.date << ['2014-01-21', '2014-01-25']) | (imps.ad_id << [30003, 30010])))
        results = list(res)
        self.assertEqual(len(results), 40)
        self.assertTrue(all(d == '2014-01-21' or d == '2014-01-25' or a == 30010 or a == 30003 for a, d, _ in results))
        res.purge()

    def test_combo_where_on_or_partition_ex2(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis,
                     where=((imps.date << ['2014-01-21', '2014-01-25']) & (imps.ad_id << [30003, 30010])))
        results = list(res)
        self.assertEqual(len(results), 1)
        self.assertTrue(all(d == '2014-01-21' and a == 30010 for a, d, _ in results))
        res.purge()

    def test_combo_where_on_and_partition(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis,
                     where=((imps.date >= '2014-01-21') & (imps.date <= '2014-01-23') & (imps.ad_id == 30010)))
        results = list(res)
        self.assertEqual(len(results), 2)
        self.assertTrue(all(d in ('2014-01-21', '2014-01-22', '2014-01-23') and a == 30010 for a, d, _ in results))
        res.purge()

    def test_combo_where_no_partition(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis, where=(imps.time >= 180000))
        results = list(res)
        print results
        self.assertEqual(len(results), 5)
        res.purge()

    def test_combo_where_on_mixed_partition(self):
        imps = Table.from_tag(IMPS)
        res = select(imps.ad_id, imps.date, imps.cpm_millis,
                     where=(((imps.date >= '2014-01-21') & (imps.date <= '2014-01-23') & (imps.time > 170000))))
        results = list(res)
        self.assertEqual(len(results), 2)
        self.assertTrue(all((d in ('2014-01-21', '2014-01-22', '2014-01-23') and a == 30003) for a, d, c in results))
        res.purge()
