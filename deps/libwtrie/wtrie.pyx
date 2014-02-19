#!python
#cython: boundscheck=False
#cython: wraparound=False
# #cython: profile=True
from array import array
from libc.string cimport strlen, memset, strcmp, strstr, strndup, strdup
from libc.stdlib cimport malloc, free, realloc
from libc.stdint cimport int32_t, uint32_t, uint64_t, uint16_t
from libc.stdio cimport printf
import struct


class TrieFullException(Exception):
    pass


cdef struct Node:
    int parent_index
    size_t kid_size
    int knodes[256]
    char *key

cdef create_node(char *key, int key_len, int parent_index, Node *result):
    result.parent_index = parent_index
    result.key = NULL
    if key != NULL:
        if key_len >= 0:
            result.key = strndup(key, key_len)
        else:
            result.key = strdup(key)
        if result.key == NULL:
            raise MemoryError()
    result.kid_size = 0
    memset(result.knodes, 0, sizeof(result.knodes))

cdef destroy_node(Node *node):
    if node.key != NULL:
        free(node.key)
        node.key = NULL

cdef size_t MAX_KID_SPACE = (2**24) * 4

cdef size_t pad(size_t val):
    return (4 - (val % 4)) % 4

cdef class Trie(object):
    cdef Node *nodes
    cdef size_t width
    cdef size_t kid_space
    cdef size_t size      # current element in the heap
    cdef size_t rest      # rest number of the slots available
    cdef size_t max_size
    cdef int symbol_size

    def __cinit__(self, width=4):
        cdef Node root
        self.nodes = <Node *> malloc(65536 * sizeof(Node))
        if self.nodes == NULL:
            raise MemoryError()
        create_node('', 0, 0, self.nodes)
        self.width = width
        self.symbol_size = 0
        self.kid_space = 12
        self.size = 1
        self.rest = 65535
        self.max_size = 2**(8*self.width)

    def __dealloc__(self):
        cdef int i
        if self.nodes != NULL:
            for i in range(self.size):
                destroy_node(self.nodes + i)
            free(self.nodes)

    def node_at_path(self, *indicies):
        cdef Node *node = self.nodes
        cdef int node_index
        for index in indicies:
            node_index = node.knodes[index]
            if node_index:
                node = self.nodes + node_index
            else:
                return None, None, None
        return node.key, node.kid_size, node.parent_index


    cdef Node *_insert_node(self, char *key, int key_len, int parent_index):
        cdef void *tmp
        # printf("     INSERT: %s %d\n", key, key_len)
        if self.rest <= 0:
            if self.size + 65536 > self.max_size:
                raise TrieFullException("Trie full: nodes=%s kid_space=%s" % (self.size, self.kid_space))
            tmp = realloc(self.nodes, (self.size + 65536) * sizeof(Node))
            if tmp == NULL:
                raise MemoryError()
            self.nodes = <Node *>tmp
            self.rest = 65536
        create_node(key, key_len, parent_index, self.nodes + self.size)
        self.size += 1
        self.rest -= 1
        return self.nodes + (self.size - 1)

    def add(self, key):
        #print "Adding %s" % repr(key)
        return self._add(key)

    cdef int _add(self, char *key):
        "add the key to the trie, return the vid"
        cdef size_t length = strlen(key)
        cdef size_t offset = 0
        cdef int parent_index = 0
        cdef int child_node_index
        cdef int new_child_index
        cdef char *key_from_offset, *radix, *ptmp
        cdef size_t radix_len
        cdef size_t prefix_len
        cdef unsigned char selector
        cdef Node *node, *new_child, *parent
        cdef char *radix_match, *rest_radix
        cdef int i

        # printf("CFONC: %s %d\n", key, length)

        # make sure we aren't full
        if self.kid_space >= MAX_KID_SPACE:
            raise TrieFullException("Trie full: nodes=%s kid_space=%s" % (self.size, self.kid_space))

        while offset < length:
            key_from_offset = key + <int>offset
            parent = self.nodes + parent_index
            selector = <unsigned char>key[offset]
            child_node_index = parent.knodes[selector]
            # printf("   SEL: %c %x\n", selector, selector)


            # if the current node doesn't have a node yet starting with the first character of the key,
            # just insert it
            if child_node_index == 0:
                child_node_index = <int>self.size
                self._insert_node(key_from_offset, -1, parent_index)
                # readjust pointer, may have done realloc
                parent = self.nodes + parent_index
                parent.knodes[selector] = child_node_index
                parent.kid_size += 1
                self.kid_space += 12 + length - offset
                self.kid_space += pad(self.kid_space)
                return child_node_index
            else:
                node = self.nodes + child_node_index
                radix = node.key
                radix_len = strlen(radix)
                if not strcmp(radix, key_from_offset):
                    # printf("     NODE EXISTS: %s\n", radix)
                    return child_node_index
                else:
                    # take care of the case where the node is not split
                    radix_match = strstr(key_from_offset, radix)
                    # printf("GR: '%s' '%s' '%s'\n", radix_match, key_from_offset, radix)

                    # will be true if key_from_offset starts with radix
                    if radix_match == key_from_offset:
                        offset += radix_len
                        parent_index = child_node_index
                    else:
                        # migrate.
                        # this is the case where both the current node and key are split
                        # on their common prefix.  a new node is created to replace the current
                        # node and hold the common prefix, which then becomes the parent of the
                        # current node.  The suffix of the key is then added to this node as well.
                        prefix_len = min(length - offset, radix_len)
                        for i in range(prefix_len):
                            if key_from_offset[i] != radix[i]:
                                prefix_len = i
                                break

                        # printf("prefix_len = %d\n", prefix_len)

                        # create the common prefix node and insert it
                        new_child_index = <int>self.size
                        new_child = self._insert_node(radix, <int>prefix_len, parent_index)

                        # we may have done a realloc, so just update the pointers
                        node = self.nodes + child_node_index
                        parent = self.nodes + parent_index

                        parent.knodes[selector] = new_child_index

                        # add the existing node as a child of the new node
                        rest_radix = radix + prefix_len
                        new_child.knodes[<unsigned char>(rest_radix[0])] = child_node_index
                        new_child.kid_size = 1
                        node.parent_index = new_child_index
                        ptmp = node.key
                        node.key = strdup(rest_radix)
                        if node.key == NULL:
                            raise MemoryError()
                        if ptmp != NULL:
                            free(ptmp)
                        self.kid_space += 12
                        self.kid_space -= pad(radix_len)
                        self.kid_space += pad(prefix_len) + pad(radix_len - prefix_len)

                        # insert the rest of the key
                        offset += prefix_len
                        parent_index = new_child_index
        return parent_index

    def print_it(self):
        self._print_it(0, 0, '<root>')

    def _print_it(self, int index, depth, msg):
        cdef Node *node = self.nodes + index
        cdef int i = 0
        cdef int child_index = 0
        print '    '*depth, "'%s'" % node.key, msg
        for i in range(256):
            child_index = node.knodes[i]
            if child_index != 0:
                self._print_it(child_index, depth + 1, "%s(%s), %s" % (child_index, self.nodes[child_index].parent_index, i))

    def get_csize(self):
        "return a tuple (nodesize, kidsize) for the sizes of the two C arrays"
        cdef Node *node
        cdef int i

        nodesize = self.size * 4
        kidsize = 0

        for i in range(self.size):
            node = self.nodes + i
            kidsize += 6 + 4 * node.kid_size
            kidsize += strlen(node.key)
            kidsize += (4 - (kidsize % 4)) % 4
        return nodesize, kidsize

    def serialize(self):
        """Return a tuple (nodes, kids, size) of arrays packed into C readable format.  This is for the 'rtrie' functions"""

        def _empty_bytes(size):
            for i in range(size):
                yield 0

        def _fill_in_order(int node_index, nodebuf, kidbuf, uint32_t kid_x):
            cdef Node *node = self.nodes + node_index
            cdef Node *kid_node
            cdef uint32_t kid, i, packed_kid, pad, val, kayrock
            cdef uint16_t key_size
            cdef unsigned char selector
            cdef char *nodekey
            cdef uint32_t four = 4
            kayrock = kid_x / four

            # print "node: %d" % node_index,

            # serialize the parent
            struct.pack_into("I", kidbuf, kid_x, node.parent_index)
            kid_x += four
            # print "parent: %d" % node.parent_index,

            #serialize the key string
            nodekey = node.key
            key_size = <uint16_t>strlen(nodekey)
            if key_size:
                struct.pack_into("H%ds" % key_size, kidbuf, kid_x, key_size, nodekey)
                kid_x += key_size
            kid_x += 2
            # print "rlen: %d radix: '%s'" % (key_size, nodekey),

            # add padding to 4 bytes
            pad = (four - (kid_x % four)) % four
            kid_x += pad
            # print "ppad: %d" % kid_x,

            for i in range(256):
                kid = node.knodes[i]
                if kid > 0:
                    # in this case we just store those nodes that have kids
                    # make lookup faster by storing the leading selector character in the high order byte
                    kid_node = self.nodes + kid
                    selector = <unsigned char>kid_node.key[0]
                    packed_kid = ((<uint32_t>selector) << 24) | kid
                    struct.pack_into("I", kidbuf, kid_x, packed_kid)
                    kid_x += four
                    # print "kid: %s,%d" % (chr(selector), kid),
            # print '<>'

            # write out the node buffer
            val = <uint32_t>(((<uint32_t>node.kid_size) << 24) | kayrock)
            #struct.pack_into("BxH", nodebuf, node_index * four, node.kid_size, kayrock)
            struct.pack_into("I", nodebuf, node_index * four, val)
            # print "  %d, %d" % (node.kid_size, kayrock)

            # now, recursively do the children - note that this will result in a sorted kids array
            for i in range(256):
                kid = node.knodes[i]
                if kid:
                    kid_x = _fill_in_order(kid, nodebuf, kidbuf, kid_x)
            return kid_x

        nodesize, kidsize = self.get_csize()
        # make sure we aren't full
        if self.kid_space >= MAX_KID_SPACE:
            raise TrieFullException("Trie full: nodes=%s kid_space=%s" % (self.size, self.kid_space))
        nodes = array('B', _empty_bytes(nodesize))
        kids = array('B', _empty_bytes(kidsize))
        _fill_in_order(0, nodes, kids, 0)
        return nodes, kids, self.size
