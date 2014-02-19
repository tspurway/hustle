#!python
#cython: boundscheck=False
#cython: wraparound=False
# #cython: profile=True
from libc.stdint cimport uint64_t
from libc.stdlib cimport malloc, realloc, free
from scamurmur3 import murmur3_x64_64


MAX_MAXHASH = 65535  # Max uint16


cdef inline void _swap(uint64_t *a, uint64_t *b):
    cdef uint64_t t

    t = b[0]
    b[0] = a[0]
    a[0] = t


cdef inline size_t _roundup(size_t v):
    ''' Round up to the nearest power of 2.
    '''
    v -= 1
    v |= v >> 1
    v |= v >> 2
    v |= v >> 4
    v |= v >> 8
    v |= v >> 16
    v += 1
    return v


cdef class MinHeap:
    """ Minimum heap container, a wrapper based on an implementation in C.

    """
    cdef uint64_t *heap
    cdef size_t capacity  # maximum capacity
    cdef size_t size      # current element in the heap
    cdef size_t rest      # rest number of the slots available

    def __cinit__(self, size_t capacity, items=[]):
        cdef int i
        cdef size_t n

        if capacity == 0:
            raise ValueError("Invalid size of heap")
        l = len(items)
        if l >= capacity:
            n = capacity
        elif l == 0:
            n = min(_roundup(int(0.125 * capacity)), capacity)
        else:
            n = min(_roundup(l), capacity)
        self.heap = <uint64_t *>malloc(n * sizeof(uint64_t))
        if self.heap is NULL:
            raise MemoryError()
        self.capacity = capacity
        self.size = 0
        i = 0
        for item in items:
            (self.heap + i)[0] = item
            self.size += 1
            i += 1
            if self.size == self.capacity:
                break
        self.rest = n - self.size
        if self.size > 0:
            self.heapify()

    def __dealloc__(self):
        if self.heap is not NULL:
            free(self.heap)

    cdef void shiftdown(self, int start, int pos):
        cdef uint64_t *bottom, *top, *parent
        cdef uint64_t new_item
        cdef int parent_index

        bottom = self.heap + pos
        new_item = bottom[0]
        while pos > start:
            parent_index = <int>((pos - 1) / 2)
            parent = self.heap + parent_index
            if new_item < parent[0]:
                self.heap[pos] = parent[0]
                pos = parent_index
                continue
            break
        self.heap[pos] = new_item

    cdef void shiftup(self, int start):
        cdef int endpos, startpos, childpos, rightpos
        cdef uint64_t new_item

        endpos = <int>self.size
        startpos = start
        new_item = (self.heap + start)[0]
        childpos = 2 * start + 1
        while childpos < endpos:
            rightpos = childpos + 1
            if rightpos < endpos and self.heap[childpos] > self.heap[rightpos]:
                childpos = rightpos
            self.heap[start] = self.heap[childpos]
            start = childpos
            childpos = 2 * start + 1
        self.heap[start] = new_item
        self.shiftdown(startpos, start)

    cdef void heapify(self):
        cdef int i

        i = <int>(self.size / 2)
        while i >= 0:
            self.shiftup(i)
            i -= 1

    cpdef pop(self):
        if self.size == 0:
            raise IndexError("Failed to pop an empty heap")

        _swap(self.heap + 0, self.heap + self.size - 1)
        self.size -= 1
        self.rest += 1
        self.shiftup(0)
        return (self.heap + self.size)[0]

    cpdef push(self, uint64_t new):
        cdef int smallest
        cdef uint64_t *item
        cdef uint64_t *tmp
        cdef size_t n

        if self.size >= self.capacity:
            raise ValueError("Failed to push, heap is full")
        if self.rest == 0:  # no slots left, need to reallocate memory
            n = min(_roundup(int(1.125 * self.size)), self.capacity)
            tmp = <uint64_t *>realloc(self.heap, n * sizeof(uint64_t))
            if tmp is NULL:
                raise MemoryError()
            self.heap = tmp
            self.rest = n - self.size

        item = self.heap + self.size
        item[0] = new
        self.size += 1
        self.rest -= 1
        self.shiftdown(0, <int>(self.size - 1))

    def __len__(self):
        return self.size

    def __iter__(self):
        cdef int i

        for i in range(self.size):
            yield (self.heap + i)[0]

    cpdef peek(self):
        if self.size == 0:
            raise IndexError("Failed to peek an empty heap")
        return self.heap[0]

    def nlargest(self, size_t n):
        cdef uint64_t *buf
        cdef int i

        if n <= 0:
            raise ValueError("Invalid argument.")

        if n < self.size:
            while self.size > n:
                self.pop()
        for i in range(self.size):
            yield (self.heap + i)[0]

    def nsmallest(self, size_t n):
        cdef uint64_t *buf
        cdef size_t s

        if n <= 0:
            raise ValueError("Invalid argument.")

        s = n if n < self.size else self.size
        while s > 0:
            yield self.pop()
            s -= 1


cdef class MaxHeap:
    """ Maximum heap container, a wrapper based on an implementation in C.

    """
    cdef uint64_t *heap
    cdef size_t capacity
    cdef size_t size
    cdef size_t rest

    def __cinit__(self, size_t capacity, items=[]):
        cdef int i
        cdef size_t n

        if capacity == 0:
            raise ValueError("Invalid size of heap")
        l = len(items)
        if l >= capacity:
            n = capacity
        elif l == 0:
            n = min(_roundup(int(0.125 * capacity)), capacity)
        else:
            n = min(_roundup(l), capacity)
        self.heap = <uint64_t *>malloc(n * sizeof(uint64_t))
        if self.heap is NULL:
            raise MemoryError()
        self.capacity = capacity
        self.size = 0
        i = 0
        for item in items:
            (self.heap + i)[0] = item
            self.size += 1
            i += 1
            if self.size == self.capacity:
                break
        self.rest = n - self.size
        if self.size > 0:
            self.heapify()

    def __dealloc__(self):
        if self.heap is not NULL:
            free(self.heap)

    cdef void shiftdown(self, int start, int pos):
        cdef uint64_t *bottom, *top, *parent
        cdef uint64_t new_item
        cdef int parent_index

        bottom = self.heap + pos
        new_item = bottom[0]
        while pos > start:
            parent_index = <int>((pos - 1) / 2)
            parent = self.heap + parent_index
            if new_item > parent[0]:
                self.heap[pos] = parent[0]
                pos = parent_index
                continue
            break
        self.heap[pos] = new_item

    cdef void shiftup(self, int start):
        cdef int endpos, startpos, childpos, rightpos
        cdef uint64_t new_item

        endpos = <int>self.size
        startpos = start
        new_item = (self.heap + start)[0]
        childpos = 2 * start + 1
        while childpos < endpos:
            rightpos = childpos + 1
            if rightpos < endpos and self.heap[childpos] < self.heap[rightpos]:
                childpos = rightpos
            self.heap[start] = self.heap[childpos]
            start = childpos
            childpos = 2 * start + 1
        self.heap[start] = new_item
        self.shiftdown(startpos, start)

    cdef void heapify(self):
        cdef int i

        i = <int>(self.size / 2)
        while i >= 0:
            self.shiftup(i)
            i -= 1

    cpdef pop(self):
        if self.size == 0:
            raise IndexError("Failed to pop an empty heap")

        _swap(self.heap + 0, self.heap + self.size - 1)
        self.size -= 1
        self.rest += 1
        self.shiftup(0)
        return (self.heap + self.size)[0]

    cpdef push(self, uint64_t new):
        cdef int smallest
        cdef uint64_t *item
        cdef uint64_t *tmp
        cdef size_t n

        if self.size >= self.capacity:
            raise ValueError("Failed to push, heap is full")
        if self.rest == 0:  # no slots left, need to reallocate memory
            n = min(_roundup(int(1.125 * self.size)), self.capacity)
            tmp = <uint64_t *>realloc(self.heap, n * sizeof(uint64_t))
            if tmp is NULL:
                raise MemoryError()
            self.heap = tmp
            self.rest = n - self.size

        item = self.heap + self.size
        item[0] = new
        self.size += 1
        self.rest -= 1
        self.shiftdown(0, <int>(self.size - 1))

    def __len__(self):
        return self.size

    def __iter__(self):
        cdef int i

        for i in range(self.size):
            yield (self.heap + i)[0]

    cpdef peek(self):
        if self.size == 0:
            raise IndexError("Failed to peek an empty heap")
        return self.heap[0]

    def nsmallest(self, size_t n):
        cdef uint64_t *buf
        cdef int i

        if n <= 0:
            raise ValueError("Invalid argument.")

        if n < self.size:
            while self.size > n:
                self.pop()
        for i in range(self.size):
            yield (self.heap + i)[0]

    def nlargest(self, size_t n):
        cdef uint64_t *buf
        cdef size_t s

        if n <= 0:
            raise ValueError("Invalid argument.")

        s = n if n < self.size else self.size
        while s > 0:
            yield self.pop()
            s -= 1


cdef class MaxHash:
    cdef MinHeap heap
    cdef size_t _capacity
    cdef set _uniq

    def __init__(self, size_t capacity = MAX_MAXHASH, items=()):
        self._capacity = capacity
        if isinstance(items, set):
            items = set(MinHeap(capacity * 2, items).nlargest(capacity))
        else:
            items = set(MinHeap(capacity * 2, set(items)).nlargest(capacity))
        self.heap = MinHeap(capacity, items)
        self._uniq = items

    cpdef size_t capacity(self):
        return self._capacity

    cpdef add(self, element):
        self.add_hashed(murmur3_x64_64(element))

    cpdef add_hashed(self, uint64_t h):
        if h not in self._uniq:
            if self.heap.size < self._capacity:
                self._uniq.add(h)
                self.heap.push(h)
            elif h > self.heap.peek():
                p = self.heap.pop()
                self._uniq.remove(p)
                self._uniq.add(h)
                self.heap.push(h)

    cpdef set uniq(self):
        return self._uniq

    cpdef MaxHash union(self, MaxHash maxhash):
        cdef size_t c

        c = min(maxhash._capacity, self._capacity)
        return MaxHash(c, self._uniq.union(maxhash._uniq))

    cpdef merge(self, MaxHash maxhash):
        cdef set diff

        diff = maxhash._uniq - self._uniq
        for h in diff:
            self.add_hashed(h)

    @classmethod
    def get_jaccard_index(cls, maxhashes):
        # Check the capacity of each maxhash. If all of them are
        # under-loaded, calculate the jaccard index directly.
        if len(maxhashes) <= 1: return 1.0

        under_loaded = True
        for maxhash in maxhashes:
            if len(maxhash.uniq()) == maxhash.capacity():
                under_loaded = False
                break
        if under_loaded:
            union = set(maxhashes[0].uniq())
            intersection = set(maxhashes[0].uniq())
            for maxhash in maxhashes[1:]:
                union = union.union(maxhash.uniq())
                intersection = intersection.intersection(maxhash.uniq())
            return len(intersection) / float(len(union))

        # Not all maxhashes are under-loaded, estimate jaccard index.
        cdef MaxHash first = maxhashes[0]
        cdef MaxHash allunion = MaxHash(first._capacity, first._uniq)
        for maxhash in maxhashes:
            allunion.merge(maxhash)

        allinter = allunion._uniq
        for maxhash in maxhashes:
            allinter = allinter.intersection(maxhash.uniq())
        return len(allinter) / float(allunion._capacity)

    def dumps(self):
        """Dump the vector of leading zeros counters as a byte array
        """
        import struct
        import cStringIO

        o = cStringIO.StringIO()
        o.write(struct.pack('H', len(self.heap)))
        o.write(struct.pack('H', self._capacity))
        for val in self.heap:
            o.write(struct.pack('L', val))
        s = o.getvalue()
        o.close()
        return s

    @classmethod
    def loads(cls, data):
        import struct
        size, capacity = struct.unpack('HH', data[:4])
        val_fmt = '%dL' % size
        heap = set(struct.unpack(val_fmt, data[4:]))
        return cls(capacity, heap)


cdef class MinHash:
    cdef MaxHeap heap
    cdef size_t _capacity
    cdef set _uniq

    def __init__(self, size_t capacity = MAX_MAXHASH, items=()):
        self._capacity = capacity
        if isinstance(items, set):
            items = set(MaxHeap(capacity * 2, items).nsmallest(capacity))
        else:
            items = set(MaxHeap(capacity * 2, set(items)).nsmallest(capacity))
        self.heap = MaxHeap(capacity, items)
        self._uniq = items

    cpdef size_t capacity(self):
        return self._capacity

    cpdef add(self, element):
        self.add_hashed(murmur3_x64_64(element))

    cpdef add_hashed(self, uint64_t h):
        if h not in self._uniq:
            if self.heap.size < self._capacity:
                self._uniq.add(h)
                self.heap.push(h)
            elif h < self.heap.peek():
                p = self.heap.pop()
                self._uniq.remove(p)
                self._uniq.add(h)
                self.heap.push(h)

    cpdef set uniq(self):
        return self._uniq

    cpdef MinHash union(self, MinHash minhash):
        cdef size_t c

        c = min(minhash._capacity, self._capacity)
        return MinHash(c, self._uniq.union(minhash._uniq))

    cpdef merge(self, MinHash minhash):
        cdef set diff

        diff = minhash._uniq - self._uniq
        for h in diff:
            self.add_hashed(h)

    @classmethod
    def get_jaccard_index(cls, minhashes):
        # Check the capacity of each minhash. If all of them are
        # under-loaded, calculate the jaccard index directly.
        if len(minhashes) <= 1: return 1.0

        under_loaded = True
        for minhash in minhashes:
            if len(minhash.uniq()) == minhash.capacity():
                under_loaded = False
                break
        if under_loaded:
            union = set(minhashes[0].uniq())
            intersection = set(minhashes[0].uniq())
            for minhash in minhashes[1:]:
                union = union.union(minhash.uniq())
                intersection = intersection.intersection(minhash.uniq())
            return len(intersection) / float(len(union))

        # Not all minhashes are under-loaded, estimate jaccard index.
        cdef MinHash first = minhashes[0]
        cdef MinHash allunion = MinHash(first._capacity, first._uniq)
        for minhash in minhashes:
            allunion.merge(minhash)

        allinter = allunion._uniq
        for minhash in minhashes:
            allinter = allinter.intersection(minhash.uniq())
        return len(allinter) / float(allunion._capacity)

    def dumps(self):
        """Dump the vector of leading zeros counters as a byte array
        """
        import struct
        import cStringIO

        o = cStringIO.StringIO()
        o.write(struct.pack('H', len(self.heap)))
        o.write(struct.pack('H', self._capacity))
        for val in self.heap:
            o.write(struct.pack('L', val))
        s = o.getvalue()
        o.close()
        return s

    @classmethod
    def loads(cls, data):
        import struct
        size, capacity = struct.unpack('HH', data[:4])
        val_fmt = '%dL' % size
        heap = set(struct.unpack(val_fmt, data[4:]))
        return cls(capacity, heap)
