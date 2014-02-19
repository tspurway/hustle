from hustle import create, insert
from hustle.core.settings import Settings

IMPS = '__test_imps'
PIXELS = '__test_pixels'


def imp_process(data):
    from disco.util import urlsplit
    _, (host, _), _ = urlsplit(data['url'])
    if host.startswith('www.'):
        host = host[4:]
    data['site_id'] = host


def ensure_tables(settings=Settings()):
    ddfs = settings['ddfs']
    imps = create(IMPS,
                  fields=['=$token', '%url', '+%site_id', '@cpm_millis', '+#ad_id', '+$date', '+@time'],
                  partition='date',
                  force=True)
    pixels = create(PIXELS,
                    fields=['=$token', '+%1isActive', '+%site_id', '@amount', '+#account_id', '+%city', '+%2state', '+#2metro',
                            '$ip', '*keyword', '+$date'],
                    partition='date',
                    force=True)

    tags = ddfs.list("hustle:%s:" % IMPS)
    if len(tags) == 0:
        # insert the files
        insert(imps, file='fixtures/imps.json', preprocess=imp_process)

    tags = ddfs.list("hustle:%s:" % PIXELS)
    if len(tags) == 0:
        # insert the files
        insert(pixels, file='fixtures/pixel.json')

if __name__ == '__main__':
    ensure_tables()

