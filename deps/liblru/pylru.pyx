import sys
import traceback
from collections import defaultdict

cdef extern from "clru.h":
    ctypedef void (*CharFetch)(char *, void *)
    ctypedef void (*CharEvict)(char *, void *)

    cdef cppclass CharLRU:
        CharLRU(CharFetch f, CharEvict e, size_t size, void *self)
        void get(char *k)
        void set(char *k)
        void evictall()

cdef void _charfetch(char *key, void *_self):
    self = <object>_self
    try:
        res = self._Fetch(key)
        if res is not None:
          self.kv[key] = res
    except Exception, e:
        print >> sys.stderr, traceback.format_exc()
        print >> sys.stderr, "Exception: %s" % str(e)

cdef void _charevict(char *key, void *_self):
    self = <object>_self
    try:
        try:
            val = self.kv[key]
        except KeyError:
            # key has no value
            return
        if val is None:
            print key, self.kv

        self._Evict(key, val)
    except Exception, e:
        print >> sys.stderr, traceback.format_exc()
        print >> sys.stderr, "Exception: %s" % str(e)
    del self.kv[key]

cdef class CharLRUDict(object):
    cdef CharLRU *store
    cdef dict kv
    cdef object _Fetch, _Evict

    property kv:
        def __get__(self):
            return self.kv

    property _Fetch:
        def __get__(self):
            return self._Fetch

    property _Evict:
        def __get__(self):
            return self._Evict

    def __cinit__(self, max_size=None, fetch=None, evict=None, factory=object):
        self.kv = <dict> defaultdict(factory)
        self._Fetch = fetch
        self._Evict = evict
        self.store = new CharLRU(_charfetch, _charevict, max_size, <void *>self)
        if self.store is NULL:
            raise MemoryError()

    def __dealloc__(self):
        if self.store is not NULL:
            del self.store

    def set(self, char *key, value):
        self.kv[key] = value
        self.store.set(key)

    def __setitem__(self, char *key, value):
        self.set(key, value)

    def get(self, char *key):
        self.store.get(key)
        return self.kv.get(key, None)

    def __getitem__(self, char *key):
        self.store.get(key)
        return self.kv[key]

    def evictAll(self):
        self.store.evictall()

    def _getContents(self):
        from copy import copy
        return copy(self.kv)


cdef extern from "clru.h":
    ctypedef void (*IntFetch)(long, void *)
    ctypedef void (*IntEvict)(long, void *)

    cdef cppclass IntLRU:
        IntLRU(IntFetch f, IntEvict e, size_t size, void *self)
        void get(long k)
        void set(long k)
        void evictall()

cdef void _intfetch(long key, void *_self):
    self = <object>_self
    try:
        res = self._Fetch(key)
        if res is not None:
            self.kv[key] = res
    except Exception, e:
        print >> sys.stderr, traceback.format_exc()
        print >> sys.stderr, "Exception: %s" % str(e)

cdef void _intevict(long key, void *_self):
    self = <object>_self
    try:
        try:
            val = self.kv[key]
        except KeyError:
            # key has no value
            return
        if val is None:
            print key, self.kv
            print >> sys.stderr, key, self.kv
        self._Evict(key, val)
    except Exception, e:
        print >> sys.stderr, traceback.format_exc()
        print >> sys.stderr, "Exception: %s" % str(e)
    del self.kv[key]

cdef class IntLRUDict(object):
    cdef IntLRU *store
    cdef dict kv
    cdef object _Fetch, _Evict

    property kv:
        def __get__(self):
            return self.kv

    property _Fetch:
        def __get__(self):
            return self._Fetch

    property _Evict:
        def __get__(self):
            return self._Evict


    def __cinit__(self, size_t max_size, object fetch=None, object evict=None, factory=object):
        self.kv = <dict> defaultdict(factory)
        self._Fetch = fetch
        self._Evict = evict
        self.store = new IntLRU(_intfetch, _intevict, max_size, <void *>self)
        if self.store is NULL:
            raise MemoryError()

    def __dealloc__(self):
        if self.store is not NULL:
            del self.store

    def set(self, long key, value):
        self.kv[key] = value
        self.store.set(key)

    def __setitem__(self, key, value):
        self.set(key, value)

    def get(self, long key):
        self.store.get(key)
        return self.kv.get(key, None)

    def __getitem__(self, long key):
        self.store.get(key)
        return self.kv[key]

    def evictAll(self):
        self.store.evictall()

    def _getContents(self):
        from copy import copy
        return copy(self.kv)

cdef class LRUDict(object):
    @classmethod
    def getDict(cls, max_size=None, fetch=None, evict=None, isInt=False, factory=object):
        if isInt:
            return IntLRUDict(max_size, fetch, evict, factory)
        else:
            return CharLRUDict(max_size, fetch, evict, factory)
