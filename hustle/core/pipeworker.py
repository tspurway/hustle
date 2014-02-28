#!/usr/bin/env python

from disco.util import shuffled, chainify
from disco.worker.pipeline import worker
from disco import util
from collections import defaultdict
from disco.worker.pipeline.worker import Stage
from heapq import merge
from itertools import islice


# import sys
# sys.path.append('/Library/Python/2.7/site-packages/pycharm-debug.egg')
# import pydevd
# pydevd.settrace('localhost', port=12999, stdoutToServer=True, stderrToServer=True)

def sort_reader(fd, fname, read_buffer_size=8192):
    buf = ""
    while True:
        r = fd.read(read_buffer_size)
        buf += r

        kayvees = buf.split('\n')
        buf = kayvees[-1]

        # the last key/value pair will be truncated except for EOF, in which case it will be empty
        for kayvee in islice(kayvees, len(kayvees) - 1):
            raws = kayvee.split(b"\xff")
            yield islice(raws, len(raws) - 1)

        if not len(r):
            if len(buf):
                print("Couldn't match the last {0} bytes in {1}. "
                      "Some bytes may be missing from input.".format(len(buf), fname))
            break


def disk_sort(input, filename, sort_keys, binaries=(), sort_buffer_size='10%', desc=False):
    import ujson
    from disco.comm import open_local
    from disco.fileutils import AtomicFile
    import base64
    # import sys
    # sys.path.append('/Library/Python/2.7/site-packages/pycharm-debug.egg')
    # import pydevd
    # pydevd.settrace('localhost', port=12999, stdoutToServer=True, stderrToServer=True)
    out_fd = AtomicFile(filename)
    key_types = None
    MPT = ()
    # print "SORTKEY: %s" % repr(sort_keys)
    for key, _ in input:
        if isinstance(key, (str, unicode)):
            raise ValueError("Keys must be sequences", key)

        # determine if the key is numeric
        if key_types is None:
            key_types = []
            for kt in key:
                try:
                    float(kt)
                    key_types.append('n')
                except:
                    key_types.append('')

        #serialize the key - encoded either as NULL, json, or b64 - note that
        for i, each_key in enumerate(key):
            if each_key is None:
                ukey = b'\x00'
            elif i in binaries and key_types[i] != 'n':
                ukey = base64.b64encode(each_key)
            else:
                ukey = ujson.dumps(each_key)
            out_fd.write(ukey)
            out_fd.write(b'\xff')
        out_fd.write('\n')
    out_fd.flush()
    out_fd.close()
    unix_sort(filename,
              [(sk, key_types[sk]) for sk in sort_keys],
              sort_buffer_size=sort_buffer_size,
              desc=desc)
    fd = open_local(filename)
    for k in sort_reader(fd, fd.url):
        # yield [ujson.loads(key) if key != b'\x00' else None for key in k], MPT

        rval = []
        for i, key in enumerate(k):
            if key == b'\x00':
                rkey = None
            elif i in binaries:
                rkey = base64.b64decode(key)
            else:
                rkey = ujson.loads(key)
            rval.append(rkey)
        yield rval, MPT


def sort_cmd(filename, sort_keys, sort_buffer_size, desc=False):
    keys = []
    # print "File %s" % filename
    for index, typ in sort_keys:
        keys.append("-k")
        index += 1
        if desc:
            keys.append("%d,%dr%s" % (index, index, typ))
        else:
            keys.append("%d,%d%s" % (index, index, typ))

    cmd = ["sort", "-t", '\xff']
    return (cmd + keys + ["-T", ".", "-S", sort_buffer_size, "-o", filename, filename],
            False)


def unix_sort(filename, sort_keys, sort_buffer_size='10%', desc=False):
    import subprocess, os.path
    if not os.path.isfile(filename):
        raise Exception("Invalid sort input file {0}".format(filename), filename)
    try:
        env = os.environ.copy()
        env['LC_ALL'] = 'C'
        cmd, shell = sort_cmd(filename, sort_keys, sort_buffer_size, desc=desc)
        subprocess.check_call(cmd, env=env, shell=shell)
    except subprocess.CalledProcessError as e:
        raise Exception("Sorting {0} failed: {1}".format(filename, e), filename)


class _lt_wrapper(tuple):
    def __new__(cls, seq, sort_range):
        return super(_lt_wrapper, cls).__new__(cls, seq)

    def __init__(self, seq, sort_range):
        super(_lt_wrapper, self).__init__(seq)
        self.sort_range = sort_range

    def __lt__(self, other):
        for sr in self.sort_range:
            if self[sr] < other[sr]:
                return True
            elif self[sr] > other[sr]:
                return False
        return False  # they are equal


class _gt_wrapper(tuple):
    def __new__(cls, seq, sort_range):
        return super(_gt_wrapper, cls).__new__(cls, seq)

    def __init__(self, seq, sort_range):
        super(_gt_wrapper, self).__init__(seq)
        self.sort_range = sort_range

    def __lt__(self, other):
        for sr in self.sort_range:
            if self[sr] > other[sr]:
                return True
            elif self[sr] < other[sr]:
                return False
        return False  # they are equal


def merge_wrapper(it, sort_range=(0,), desc=False):
    if desc:
        wrapper = _gt_wrapper
    else:
        wrapper = _lt_wrapper

    for key, value in it:
        yield wrapper(key, sort_range), value


class HustleStage(Stage):
    def __init__(self, name, sort=(), input_sorted=False, desc=False, combine_labels=False, binaries=(), **kwargs):
        super(HustleStage, self).__init__(name, **kwargs)
        self.sort = sort
        self.input_sorted = input_sorted
        self.desc = desc
        self.combine_labels = combine_labels
        self.binaries = binaries


class Worker(worker.Worker):
    def jobenvs(self, job, **jobargs):
        import sys
        envs = {'PYTHONPATH': ':'.join([path.strip('/') for path in sys.path])}
        envs['LD_LIBRARY_PATH'] = 'lib'
        envs['PYTHONPATH'] = ':'.join(('lib', envs.get('PYTHONPATH', '')))
        envs['PATH'] = '/srv/disco/helper/bin:/usr/local/bin:/bin:/usr/bin'
        envs['TZ'] = 'UTC'
        envs['DISCO_WORKER_MAX_MEM'] = "-1"
        return envs

    def start(self, task, job, **jobargs):
        task.makedirs()
        if self.getitem('profile', job, jobargs):
            from cProfile import runctx
            name = 'profile-{0}'.format(task.uid)
            path = task.path(name)
            runctx('self.run(task, job, **jobargs)', globals(), locals(), path)
            task.put(name, open(path, 'rb').read())
        else:
            self.run(task, job, **jobargs)
        self.end(task, job, **jobargs)

    def prepare_input_map(self, task, stage, params):
        # The input map maps a label to a sequence of inputs with that
        # label.
        map = defaultdict(list)

        for l, i in util.chainify(self.labelexpand(task, stage, i, params) for i in self.get_inputs()):
            if stage.combine_labels:
                map[0].append(i)
            else:
                map[l].append(i)

        if stage.sort:
            newmap = {}
            if stage.input_sorted:
                for label, inputs in map.iteritems():
                    input = merge(*(merge_wrapper(inp, sort_range=stage.sort, desc=stage.desc) for inp in inputs))
                    newmap[label] = [input]
            else:
                for label, inputs in map.iteritems():

                    input = chainify(shuffled(inputs))
                    newmap[label] = [disk_sort(input,
                                               task.path('sort.dl'),
                                               sort_keys=stage.sort,
                                               sort_buffer_size='15%',
                                               binaries=stage.binaries,
                                               desc=stage.desc)]
            map = newmap
        #print "OUTSIE: %s" % str(map)
        return map

    def run_stage(self, task, stage, params):
        # Call the various entry points of the stage task in order.
        params._task = task
        interface = self.make_interface(task, stage, params)
        state = stage.init(interface, params) if callable(stage.init) else None
        if callable(stage.process):
            input_map = self.prepare_input_map(task, stage, params)
            for label in stage.input_hook(state, input_map.keys()):
                # for inp in input_map[label]:
                    # stage.process(interface, state, label, inp, task)
                if stage.combine:
                    stage.process(interface, state, label, SerialInput(input_map[label]), task)
                else:
                    for inp in input_map[label]:
                        stage.process(interface, state, label, inp, task)
        if callable(stage.done):
            stage.done(interface, state)


def SerialInput(inputs):
    for inp in inputs:
        for k, v in inp:
            yield k, v


if __name__ == '__main__':
    Worker.main()
