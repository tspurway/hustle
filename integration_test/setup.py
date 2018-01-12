from hustle import Table, insert
from hustle.core.settings import Settings, overrides
import ujson


IMPS = '__test_imps'
PIXELS = '__test_pixels'
PIXELS_HLL = '__test_pixels_hll'
IPS = '__test_ips'


def imp_process(data):
    from disco.util import urlsplit

    _, (host, _), _ = urlsplit(data['url'])
    if host.startswith('www.'):
        host = host[4:]
    data['site_id'] = host


def insert_hll(table, file=None, streams=None, preprocess=None,
               maxsize=100 * 1024 * 1024, tmpdir='/tmp', decoder=ujson.decode,
               lru_size=10000, hll_field=None, **kwargs):
    from cardunion import Cardunion
    import os

    settings = Settings(**kwargs)
    ddfs = settings['ddfs']

    def part_tag(name, partition=None):
        rval = "hustle:" + name
        if partition:
            rval += ':' + str(partition)
        return rval

    def hll_iter(strms):
        buf = {}
        fields = table._field_names
        fields.remove('hll')
        #  fields.remove('maxhash')

        for stream in strms:
            for line in stream:
                try:
                    data = decoder(line)
                except Exception as e:
                    print "Exception decoding record (skipping): %s %s" % (e, line)
                else:
                    if preprocess:
                        if not preprocess(data):
                            continue
                    key = ujson.dumps([data[f] for f in fields])
                    if key not in buf:
                        hll = Cardunion(12)
                        buf[key] = hll
                    else:
                        hll = buf[key]

                    hll.add(data[hll_field])

        for key, hll in buf.iteritems():
            data = dict(zip(fields, ujson.loads(key)))
            data['hll'] = hll.dumps()
            yield data

    if file:
        streams = [open(file)]
    lines, partition_files = table._insert([hll_iter(streams)],
                                           maxsize=maxsize, tmpdir=tmpdir,
                                           decoder=lambda x: x, lru_size=lru_size)
    if partition_files is not None:
        for part, pfile in partition_files.iteritems():
            tag = part_tag(table._name, part)
            ddfs.push(tag, [pfile])
            print 'pushed %s, %s' % (part, tag)
            os.unlink(pfile)
    return table._name, lines


def ensure_tables():
    overrides['server'] = 'disco://localhost'
    overrides['dump'] = False
    overrides['nest'] = False
    settings = Settings()
    ddfs = settings['ddfs']

    imps = Table.create(IMPS,
                        columns=['wide index string token', 'trie url', 'index trie site_id', 'uint cpm_millis',
                                 'index int ad_id', 'index string date', 'index uint time', 'bit click',
                                 'index bit impression', 'bit conversion'],
                        partition='date',
                        force=True)
    pixels = Table.create(PIXELS,
                          columns=['wide index string token', 'index bit isActive', 'index trie site_id',
                                   'uint amount', 'index int account_id', 'index trie city', 'index trie16 state',
                                   'index int16 metro', 'string ip', 'lz4 keyword', 'index string date'],
                          partition='date',
                          force=True)
    pixel_hlls = Table.create(PIXELS_HLL,
                              columns=['index bit isActive', 'index trie site_id', 'index int account_id',
                                       'index trie city', 'index trie16 state', 'index string date',
                                       'binary hll'],
                              partition='date',
                              force=True)
    ips = Table.create(IPS,
                       columns=['index trie16 exchange_id', 'index uint32 ip'],
                       force=True)

    tags = ddfs.list("hustle:%s:" % IMPS)
    if len(tags) == 0:
        # insert the files
        insert(imps, File='fixtures/imps.json', preprocess=imp_process)

    tags = ddfs.list("hustle:%s:" % PIXELS)
    if len(tags) == 0:
        # insert the files
        insert(pixels, File='fixtures/pixel.json')

    tags = ddfs.list("hustle:%s:" % IPS)
    if len(tags) == 0:
        # insert the files
        insert(ips, File='fixtures/ip.json')

    tags = ddfs.list("hustle:%s:" % PIXELS_HLL)
    if len(tags) == 0:
        # insert the files
        insert_hll(pixel_hlls, file='./fixtures/pixel.json', hll_field='token')


if __name__ == '__main__':
    ensure_tables()
