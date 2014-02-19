from collections import OrderedDict


class LRUDict(object):
    def __init__(self, max_size=None, fetch=None, evict=None):
        self.max_size = max_size
        self.fetch = fetch
        self.evict = evict
        self.store = OrderedDict()
        self._validate_size()

    def set(self, key, value):
        self.store[key] = value
        self._validate_size()

    def __setitem__(self, key, value):
        self.set(key, value)

    def get(self, key):
        if key in self.store:
            value = self.store[key]

            # ensure this newly fetched key is re-inserted
            del self.store[key]
            self.store[key] = value
            return value
        else:
            value = self.fetch(key)
            if value is not None:
                self.set(key, value)
                return value
        return None

    def _validate_size(self):
        if self.max_size is not None:
            while len(self.store) > self.max_size:
                key, value = self.store.popitem(last=False)
                # evict - write to backing store
                self.evict(key, value)

    def setdefault(self, key, default=None):
        value = self.get(key)
        if value is None:
            self.set(key, default)
            return default
        return value

    def _iteritems(self):
        return self.store.iteritems()

    def evictAll(self):
        for k, v in self._iteritems():
            self.evict(k, v)


def mdb_fetch(key, txn=None, ixdb=None):
    from pyebset import BitSet
    try:
        bitmaps = ixdb.get(txn, key)
    except:
        bitmaps = None

    if bitmaps is not None:
        bitset = BitSet()
        bitset.loads(bitmaps)
        return bitset
    return None


def mdb_evict(key, bitset, txn=None, ixdb=None):
    ixdb.put(txn, key, bitset.dumps())
