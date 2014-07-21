import unittest
from hustle import select, Table, h_max, h_min
from hustle.core.column_fn import ip_ntoa
from setup import IPS
from hustle.core.settings import Settings, overrides


class TestSimpleQuery(unittest.TestCase):
    def setUp(self):
        overrides['server'] = 'disco://localhost'
        overrides['dump'] = False
        overrides['nest'] = False
        self.settings = Settings()

    def tearDown(self):
        pass

    def test_column_fn(self):
        ips = Table.from_tag(IPS)
        res = select(ips.exchange_id, ip_ntoa(ips.ip),
                     where=ips.exchange_id == "Adx")
        results = list(res)
        self.assertEqual(len(results), 29)

    def test_column_fn_with_agg(self):
        ips = Table.from_tag(IPS)
        res = select(ips.exchange_id, h_max(ip_ntoa(ips.ip)),
                     where=ips, order_by=(ips.exchange_id,))
        results = list(res)
        exchanges = [ex for ex, _ in results]
        ipss = [ip for _, ip in results]
        self.assertListEqual(['Adx', 'Appnexus', 'OpenX', 'Rubycon'], exchanges)
        self.assertListEqual(['192.168.1.1'] * 4, ipss)

        res = select(ips.exchange_id, h_min(ip_ntoa(ips.ip)),
                     where=ips, order_by=(ips.exchange_id,))
        results = list(res)
        exchanges = [ex for ex, _ in results]
        ipss = [ip for _, ip in results]
        self.assertListEqual(['Adx', 'Appnexus', 'OpenX', 'Rubycon'], exchanges)
        self.assertListEqual(['127.0.0.1'] * 4, ipss)

    def test_column_fn_with_distinct(self):
        ips = Table.from_tag(IPS)
        res = select(ip_ntoa(ips.ip),
                     where=ips.exchange_id == "Adx", order_by=(ip_ntoa(ips.ip),),
                     distinct=True)
        results = list(res)
        ipss = [ip[0] for ip in results]
        self.assertListEqual(['127.0.0.1', '192.1.1.1', '192.1.1.2', '192.168.1.1'],
                             ipss)

    def test_column_fn_with_nest(self):
        ips = Table.from_tag(IPS)
        res = select(ip_ntoa(ips.ip),
                     where=ips.exchange_id == "Adx", order_by=(ip_ntoa(ips.ip),),
                     distinct=True, nest=True)
        ret = select(res.ip, where=res, order_by=(res.ip,))
        results = list(ret)
        ipss = [ip[0] for ip in results]
        self.assertListEqual(['127.0.0.1', '192.1.1.1', '192.1.1.2', '192.168.1.1'],
                             ipss)
