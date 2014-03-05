from hustle import Table, insert
from hustle.core.settings import Settings, overrides

IMPS = '__test_imps'
PIXELS = '__test_pixels'


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
                        columns=['wide index string token', 'trie url', 'index trie site_id', 'uint cpm_millis',
                                 'index int ad_id', 'index string date', 'index uint time'],
                        partition='date',
                        force=True)
    pixels = Table.create(PIXELS,
                          columns=['wide index string token', 'index uint8 isActive', 'index trie site_id',
                                   'uint amount', 'index int account_id', 'index trie city', 'index trie16 state',
                                   'index int16 metro', 'string ip', 'lz4 keyword', 'index string date'],
                          partition='date',
                          force=True)

    tags = ddfs.list("hustle:%s:" % IMPS)
    if len(tags) == 0:
        # insert the files
        insert(imps, phile='fixtures/imps.json', preprocess=imp_process)

    tags = ddfs.list("hustle:%s:" % PIXELS)
    if len(tags) == 0:
        # insert the files
        insert(pixels, phile='fixtures/pixel.json')


if __name__ == '__main__':
    ensure_tables()

