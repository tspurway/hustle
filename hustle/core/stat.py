from disco.core import Job
from disco.worker.task_io import task_input_stream
from hustle.core.pipeworker import Worker, HustleStage

import hustle
import hustle.core
import hustle.core.marble


def stat_input_stream(fd, size, url, params):
    from disco import util
    from hustle.core.marble import MarbleStream

    try:
        scheme, netloc, rest = util.urlsplit(url)
    except Exception as e:
        print "Error handling hustle_input_stream for %s. %s" % (url, e)
        raise e

    otab = None
    try:
        # print "FLurlG: %s" % url
        fle = util.localize(rest, disco_data=params._task.disco_data,
                            ddfs_data=params._task.ddfs_data)
        # print "FLOGLE: %s" % fle
        otab = MarbleStream(fle)
        rows = otab.number_rows
        frows = float(rows)
        rval = {'_': rows, }
        for field, (subdb, subindexdb, _, column) in otab.dbs.iteritems():
            if subindexdb:
                rval[field] = subindexdb.stat(otab.txn)['ms_entries'] / frows
        yield '', rval
    except Exception as e:
        print "Gibbers: %s" % e
    finally:
        if otab:
            otab.close()


class StatPipe(Job):
    required_modules = [
        ('hustle', hustle.__file__),
        ('hustle.core', hustle.core.__file__),
        ('hustle.core.marble', hustle.core.marble.__file__)]

    def __init__(self, master):

        super(StatPipe, self).__init__(master=master, worker=Worker())
        self.pipeline = [('split',
                          HustleStage('stat',
                                      process=process_stat,
                                      input_chain=[task_input_stream,
                                                   stat_input_stream]))]


def process_stat(interface, state, label, inp, task):
    from disco import util

    # inp contains a set of replicas, let's force local #HACK
    input_processed = False
    for i, inp_url in inp.input.replicas:
        scheme, (netloc, port), rest = util.urlsplit(inp_url)
        if netloc == task.host:
            input_processed = True
            inp.input = inp_url
            break

    if not input_processed:
        raise Exception("Input %s not processed, no LOCAL resource found."
                        % str(inp.input))

    for key, value in inp:
        interface.output(0).add(key, value)
