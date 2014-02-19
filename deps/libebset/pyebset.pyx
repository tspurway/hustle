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

    def __cinit__(self):
        self.thisptr = new EWAHBoolArray[uint64_t]()

    def __dealloc__(self):
        del self.thisptr

    def __setitem__(self, key, value):
        if value:
            self.thisptr.set(key)

    def set(self, size_t i):
        return self.thisptr.set(i)

    def dumps(self):
        cdef stringstream s

        self.thisptr.write(s, True)
        return s.str()

    def loads(self, s):
        cdef stringstream ss

        ss.write(s, len(s))
        self.thisptr.read(ss, True)
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
        cdef size_t l = self.thisptr.sizeInBits()
        cdef size_t i

        IF UNAME_SYSNAME == "Linux":
            cdef vector[uint64_t].iterator it = v.begin()
            while it != v.end():
                i = deref(it)
                if i < l:
                    yield i
                else:
                    break
                inc(it)
        ELSE:
            # clang compiler on Mac Os x will report error for the code above
            return (i for i in <list>v if i < l)

    def __len__(self):
        return self.thisptr.numberOfOnes()

    def __str__(self):
        return self.dumps()

    def __contains__(self, size_t v):
        cdef vector[size_t] a = self.thisptr.toArray()

        return binary_search(a.begin(), a.end(), v)
