from hustle import select, Table
from setup import PIXELS_HLL
from hustle.core.settings import Settings, overrides
from hustle.cardinality import h_cardinality as h_hll

from collections import defaultdict
from operator import itemgetter

import unittest
import ujson


HLL_ESTIMATE_ERROR = .04


class TestCardinalityQuery(unittest.TestCase):
    def setUp(self):
        overrides['server'] = 'disco://localhost'
        overrides['dump'] = False
        overrides['nest'] = False
        self.settings = Settings()

    def tearDown(self):
        pass

    def checkEstimate(self, estimate, expect):
        self.assertAlmostEqual(estimate, expect,
                               delta=int(HLL_ESTIMATE_ERROR * expect))

    def test_cardinality_all(self):
        hll = Table.from_tag(PIXELS_HLL)
        res = select(h_hll(hll.hll), where=hll)
        estimate = next(iter(res))[0]
        tokens = set([])
        with open("./fixtures/pixel.json") as f:
            for line in f:
                record = ujson.loads(line)
                tokens.add(record["token"])
        self.checkEstimate(estimate, len(tokens))
        res.purge()

    def test_cardinality_on_condition(self):
        hll = Table.from_tag(PIXELS_HLL)
        active_tokens = set([])
        inactive_tokens = set([])
        with open("./fixtures/pixel.json") as f:
            for line in f:
                record = ujson.loads(line)
                if record["isActive"]:
                    active_tokens.add(record["token"])
                else:
                    inactive_tokens.add(record["token"])
        res = select(h_hll(hll.hll), where=(hll.isActive == 1))
        estimate = next(iter(res))[0]
        self.checkEstimate(estimate, len(active_tokens))
        res.purge()

        res = select(h_hll(hll.hll), where=(hll.isActive == 0))
        estimate = next(iter(res))[0]
        self.checkEstimate(estimate, len(inactive_tokens))
        res.purge()

    def test_cardinality_with_order_by(self):
        hll = Table.from_tag(PIXELS_HLL)
        tokens_by_date = defaultdict(set)
        with open("./fixtures/pixel.json") as f:
            for line in f:
                record = ujson.loads(line)
                tokens_by_date[record["date"]].add(record["token"])
        result = [(date, len(tokens)) for date, tokens in tokens_by_date.items()]

        # Test order by date
        expects = sorted(result, key=itemgetter(0), reverse=True)
        res = select(hll.date, h_hll(hll.hll), where=hll, order_by=0, desc=True)
        estimates = list(res)
        for i, (date, expected_cardinality) in enumerate(expects):
            self.assertEqual(estimates[i][0], date)
            self.checkEstimate(estimates[i][1], expected_cardinality)
        res.purge()

        # Test order by hll
        res = select(hll.date, h_hll(hll.hll), where=hll, order_by=1, desc=True)
        l = list(res)
        for i in range(len(l) - 1):
            self.assertTrue(l[i][1] >= l[i + 1][1])
        res.purge()
