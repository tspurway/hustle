from libc.stdint cimport uint64_t
from cython.operator cimport dereference as deref, preincrement as inc
from libcpp.vector cimport vector
from libcpp.string cimport string


cdef extern from "<ostream>" namespace "std":
    cdef cppclass ostream[T]:
        pass

cdef extern from "<sstream>" namespace "std":
    cdef cppclass stringstream:
        stringstream() except +
        string str()
        ostream write(char *, size_t)

cdef extern from "<algorithm>" namespace "std":
    cdef bint binary_search(vector[size_t].iterator,
                            vector[size_t].iterator,
                            uint64_t&)

cdef extern from "ewah.h":
    cdef cppclass EWAHBoolArray[T]:
        EWAHBoolArray() nogil except +
        bint set(size_t i) nogil
        bint get(size_t i) nogil
        void logicaland(EWAHBoolArray&, EWAHBoolArray&) nogil
        void logicalor(EWAHBoolArray&, EWAHBoolArray&) nogil
        void logicalnot(EWAHBoolArray&) nogil
        size_t sizeInBytes() nogil
        void write(stringstream &, bint) nogil
        void read(stringstream &, bint) nogil
        vector[size_t] toArray() nogil
        size_t numberOfOnes() nogil
        bint operator==(EWAHBoolArray&) nogil
        bint operator!=(EWAHBoolArray&) nogil
        size_t sizeInBits() nogil
        void reset() nogil
        void inplace_logicalnot() nogil


cdef class BitSet:
    cdef EWAHBoolArray[uint64_t] *thisptr
    cdef vector[size_t] indexes
    cdef bint updated

    def __cinit__(self):
        self.thisptr = new EWAHBoolArray[uint64_t]()
        self.updated = True

    def __dealloc__(self):
        del self.thisptr

    def __setitem__(self, key, value):
        if value:
            self.set(key)

    def __getitem__(self, key):
        return self.thisptr.get(key)

    def set(self, size_t i):
        if self.thisptr.set(i):
            self.updated = True
            return True
        else:
            return False

    def get(self, size_t i):
        return self.thisptr.get(i)

    def dumps(self):
        cdef stringstream s

        self.thisptr.write(s, True)
        return s.str()

    def loads(self, s):
        cdef stringstream ss

        ss.write(s, len(s))
        self.thisptr.read(ss, True)
        self.updated = True
        return

    def size_in_bytes(self):
        return self.thisptr.sizeInBytes()

    def size_in_bits(self):
        return self.thisptr.sizeInBits()

    def reset(self):
        self.thisptr.reset()

    cpdef BitSet land(self, BitSet other):
        cdef BitSet s = BitSet()

        self.thisptr.logicaland(deref(other.thisptr), deref(s.thisptr))
        return s

    def __and__(self, other):
        return self.land(other)

    cpdef BitSet lor(self, BitSet other):
        cdef BitSet s = BitSet()

        self.thisptr.logicalor(deref(other.thisptr), deref(s.thisptr))
        return s

    def __or__(self, other):
        return self.lor(other)

    cpdef BitSet lnot(self):
        cdef BitSet s = BitSet()

        self.thisptr.logicalnot(deref(s.thisptr))
        return s

    def lnot_inplace(self):
        self.thisptr.inplace_logicalnot()
        self.updated = True
        return self

    def __richcmp__(BitSet l, BitSet r, int op):
        cdef bint e

        if op == 2:
            e = (deref(l.thisptr) == deref(r.thisptr))
        elif op == 3:
            e = (deref(l.thisptr) != deref(r.thisptr))
        else:
            raise AttributeError("Unsupported operators.")
        return e

    def __invert__(self):
        return self.lnot()

    def __iter__(self):
        cdef vector[size_t] v = self.thisptr.toArray()
        cdef size_t i

        IF UNAME_SYSNAME == "Linux":
            cdef vector[uint64_t].iterator it = v.begin()
            while it != v.end():
                i = deref(it)
                yield i
                inc(it)
        ELSE:
            # clang compiler on Mac Os x will report error for the code above
            return (i for i in <list>v)

    def __len__(self):
        return self.thisptr.numberOfOnes()

    def __str__(self):
        return self.dumps()

    def __contains__(self, size_t v):
        if self.updated or (self.indexes.size() == 0):
            self.indexes = self.thisptr.toArray()
            self.updated = False

        return binary_search(self.indexes.begin(), self.indexes.end(), v)
