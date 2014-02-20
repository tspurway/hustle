from disco import func
from disco import util
from disco.settings import DiscoSettings

import collections


class Peekable(object):
    def __init__(self, iterable):
        self._iterable = iter(iterable)
        self._cache = collections.deque()

    def __iter__(self):
        return self

    def _fillcache(self, n):
        if n is None:
            n = 1
        while len(self._cache) < n:
            self._cache.append(self._iterable.next())

    def next(self, n=None):
        self._fillcache(n)
        if n is None:
            result = self._cache.popleft()
        else:
            result = [self._cache.popleft() for i in range(n)]
        return result

    def peek(self, n=None):
        self._fillcache(n)
        if n is None:
            result = self._cache[0]
        else:
            result = [self._cache[i] for i in range(n)]
        return result


class SortedIterator(object):

    def __init__(self, inputs):
        ins = [Peekable(input) for input in inputs]
        self.collection = sorted(ins, key=self._key)

    def __iter__(self):
        return self

    def next(self):
        removes = []
        reinsert = None
        rval = None
        for stream in self.collection:
            try:
                rval = stream.next()
                reinsert = stream
                break
            except StopIteration:
                removes.append(stream)

        if rval:
            for remove in removes:
                self.collection.remove(remove)
            if reinsert:
                self.collection.remove(reinsert)
                try:
                    reinsert.peek()
                except:
                    pass
                else:
                    removes = []
                    reinsert_index = 0
                    for stream in self.collection:
                        try:
                            stream.peek()
                            if self._key(reinsert) < self._key(stream):
                                break
                        except:
                            removes.append(stream)
                        reinsert_index += 1
                    self.collection.insert(reinsert_index, reinsert)
                    for remove in removes:
                        self.collection.remove(remove)
            return rval
        raise StopIteration

    def _key(self, stream):
        try:
            key, value = stream.peek()
            return tuple(key)
        except StopIteration:
            return tuple()


def sorted_iterator(urls,
                    reader=func.chain_reader,
                    input_stream=(func.map_input_stream,),
                    notifier=func.notifier,
                    params=None,
                    ddfs=None):

    from disco.worker import Input
    from disco.worker.classic.worker import Worker

    worker = Worker(map_reader=reader, map_input_stream=input_stream)
    settings = DiscoSettings(DISCO_MASTER=ddfs) if ddfs else DiscoSettings()

    inputs = []
    for input in util.inputlist(urls, settings=settings):
        notifier(input)
        instream = Input(input, open=worker.opener('map', 'in', params))
        if instream:
            inputs.append(instream)

    return SortedIterator(inputs)


def ensure_list(val):
    if not isinstance(val, list):
        if isinstance(val, (tuple, set)):
            return list(val)
        return [val]
    return val


