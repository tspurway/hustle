import unittest
from hustle import Table, insert, drop, delete, get_partitions
from hustle.core.settings import Settings, overrides

IMPS = '__test_drop_imps'


def imp_process(data):
    from disco.util import urlsplit

    _, (host, _), _ = urlsplit(data['url'])
    if host.startswith('www.'):
        host = host[4:]
    data['site_id'] = host


def ensure_tables():
    overrides['server'] = 'disco://localhost'
    overrides['dump'] = False
    overrides['nest'] = False
    settings = Settings()
    ddfs = settings['ddfs']

    imps = Table.create(IMPS,
                        fields=['=$token', '%url', '+%site_id', '@cpm_millis', '+#ad_id', '+$date', '+@time'],
                        partition='date',
                        force=True)

    tags = ddfs.list("hustle:%s:" % IMPS)
    if len(tags) == 0:
        # insert the files
        insert(imps, phile='fixtures/imps.json', preprocess=imp_process)
    return imps


class TestDropTable(unittest.TestCase):
    def setUp(self):
        overrides['server'] = 'disco://localhost'
        overrides['dump'] = False
        overrides['nest'] = False
        self.settings = Settings()
        self.ddfs = self.settings['ddfs']
        self.table = ensure_tables()

    def test_delete_all(self):
        delete(self.table)
        self.assertEqual([], get_partitions(self.table))
        tags = self.ddfs.list(Table.base_tag(self.table._name))
        self.assertEqual(len(tags), 1)
        self.assertEqual(tags[0], "hustle:__test_drop_imps")

    def test_delete_partial(self):
        delete(self.table.date >= '2014-01-13')
        self.assertEqual(['hustle:__test_drop_imps:2014-01-10',
                          'hustle:__test_drop_imps:2014-01-11',
                          'hustle:__test_drop_imps:2014-01-12'],
                         get_partitions(self.table))
        tags = self.ddfs.list(Table.base_tag(self.table._name))
        self.assertEqual(len(tags), 4)
        self.assertIn("hustle:__test_drop_imps", tags)
        drop(self.table)
        with self.assertRaises(ValueError):
            delete(self.table.site_id == 'foobar')
            delete(self.tale.url)

    def test_drop(self):
        drop(self.table)
        self.assertEqual([], get_partitions(self.table))
        tags = self.ddfs.list(Table.base_tag(self.table._name))
        self.assertEqual(len(tags), 0)
