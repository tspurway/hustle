"""
:mod:`hustle.core.marble` -- The Hustle Database Core
=====================================================


"""
from functools import partial
from collections import defaultdict
from pyebset import BitSet
import mdb
import ujson
import os
import tempfile
import clz4
import rtrie
import time

COMMIT_THRESHOLD = 50000


class Marble(object):
    """
    The Marble is the smallest unit of distribution and replication in Hustle.  The Marble is a wrapper around a
    standalone `LMDB <http://symas.com/mdb/>`_ key/value store.  An *LMDB* key/value store may have many sub key/value
    stores, which are called DBs in LMDB terminology.  Each Hustle column is represented by one LMDB DB (called the column
    DB), plus another LMDB DB if that column is indexed (called the index DB) (see :ref:`schemadesign`).

    The Marble file is also the unit of insertion and replication of data into the Hustle system.  Marbles can be built
    on remote systems, in a completely distributed manner.  They are then *pushed* into the cluster's DDFS file system,
    which is a relatively inexpensive operation.  This is Hustle's :ref:`distributed insert functionality <insertguide>`.

    In addition each Marble contains several LMDB meta DBs for meta-data for prefix Tries, schema, and table statistics
    used by the query optimizer.

    The column DB is a key/value store that stores data for a particular column.  It has a locally unique row identifier (RID) as
    the key, and the actual value for that column's data as its value, encoded depending on the schema data type of
    the column.  All integer types are directly encoded in LMDB as integers, whereas the Trie compression types are encoded
    as integers (called VIDs), which actually are keys in the two dedicated Trie meta DBs (one for 32 and one for 16
    bit Tries).  Uncompressed strings, as well as lz4 and binary style data is simply encoded as byte string values.

    The index DB for a column is a key/value store that inverts the key and the value of the column DB.  It is used to
    perform the identity and range queries required form Hustle's *where clause*.  The key in the index DB is the
    actual value for that column, but the value is a *bitmap index* of the RIDs where that value is present.  This is
    a very efficient and compact way to store the index in an append-only database like Hustle.

    :type name: basestring
    :param name: the name of the *Marble* to create

    :type fields: sequence of string
    :param name: the schema specification of the columns (see :ref:`Hustle Schema Guide <schemadesign>`

    :type partition: basestring
    :param partition: the column that will serve as the partition for this *Marble*
    """
    def __init__(self, name=None, fields=(), partition=None):
        self._name = name
        self._fields = fields
        self._partition = partition
        self._columns = {}

        for field in fields:
            field, type_indicator, compression_indicator, rtrie_indicator, index_indicator = \
                self._parse_index_type(field)
            part = field == partition
            col = Column(field,
                         self,
                         index_indicator=index_indicator,
                         partition=part,
                         type_indicator=type_indicator,
                         compression_indicator=compression_indicator,
                         rtrie_indicator=rtrie_indicator)
            self._columns[field] = col
            self.__dict__[field] = col

    @property
    def _field_names(self):
        return [self._parse_index_type(field)[0] for field in self._fields]

    @classmethod
    def _parse_index_type(cls, ix):
        """
        We have 9 possible types represented by the type_indicator:
        MDB_STR = 0
        MDB_INT_32 = 1
        MDB_UINT_32 = 2
        MDB_INT_16 = 3
        MDB_UINT_16 = 4
        MDB_INT_8 = 5
        MDB_UINT_8 = 6
        MDB_INT_64 = 7
        MDB_UINT_64 = 8

        The compression_indicator - used only for MDB_STR type_indicators:
        0 - rtrie compression
        1 - no compression
        2 - LZ4 compression
        3 - BINARY

        The rtrie_indicator - used only for 0 (rtrie) compression_indicators:
        MDB_UINT_32 - 32 bit VID
        MDB_UINT_16 - 16 bit VID
        """
        type_indicator = mdb.MDB_STR
        compression_indicator = 0
        rtrie_indicator = mdb.MDB_UINT_32
        index_indicator = 0

        nx = ix
        while True:
            if len(ix) > 1 and ix[0] < 'a':
                ind = ix[0]
                nx = ix[1:]
                if ind == '#':
                    type_indicator = mdb.MDB_INT_32
                    if ix[1] == '2':
                        type_indicator = mdb.MDB_INT_16
                        nx = ix[2:]
                    elif ix[1] == '4':
                        nx = ix[2:]
                    elif ix[1] == '1':
                        type_indicator = mdb.MDB_INT_8
                        nx = ix[2:]
                    elif ix[1] == '8':
                        type_indicator = mdb.MDB_INT_64
                        nx = ix[2:]
                elif ind == '@':
                    type_indicator = mdb.MDB_UINT_32
                    if ix[1] == '2':
                        type_indicator = mdb.MDB_UINT_16
                        nx = ix[2:]
                    elif ix[1] == '4':
                        nx = ix[2:]
                    elif ix[1] == '1':
                        type_indicator = mdb.MDB_UINT_8
                        nx = ix[2:]
                    elif ix[1] == '8':
                        type_indicator = mdb.MDB_UINT_64
                        nx = ix[2:]
                elif ind == '%':
                    if ix[1] == '2':
                        rtrie_indicator = mdb.MDB_UINT_16
                        nx = ix[2:]
                    if ix[1] == '4':
                        nx = ix[2:]
                elif ind == '$':
                    compression_indicator = 1
                elif ind == '*':
                    compression_indicator = 2
                elif ind == '&':
                    compression_indicator = 3
                elif ind == '+':
                    index_indicator = 1
                    ix = nx
                    continue
                elif ind == '=':
                    index_indicator = 2
                    ix = nx
                    continue
            break
        return nx, type_indicator, compression_indicator, rtrie_indicator, index_indicator

    @classmethod
    def from_file(cls, filename):
        """
        Instantiate a :class:`Marble <hustle.core.marble.Marble>` from an *LMDB*
        """
        env, txn, db = mdb.mdb_read_handle(filename, '_meta_', False,
                                           False, False,
                                           mdb.MDB_NOSUBDIR | mdb.MDB_NOLOCK)
        try:
            vals = {k: ujson.loads(v) for k, v in db.items(txn) if not k.startswith('_')}
            return cls(**vals)
        finally:
            txn.commit()
            env.close()

    @classmethod
    def _open_env(cls, filename, maxsize, write):
        # Always open env without locking, since readers and writers never show up
        # togerther.
        if write:
            oflags = mdb.MDB_WRITEMAP | mdb.MDB_NOLOCK
        else:
            oflags = mdb.MDB_RDONLY | mdb.MDB_NOLOCK

        retry = 0
        while retry <= 11:
            try:
                env = mdb.Env(filename,
                              max_dbs=1024,
                              mapsize=maxsize,
                              flags=oflags | mdb.MDB_NORDAHEAD | mdb.MDB_NOSYNC | mdb.MDB_NOSUBDIR)
            except Exception as e:
                print "Error: %s" % e
                retry += 1
                time.sleep(5)
            else:
                return env
        raise SystemError("Failed to open MDB env.")

    def _open(self, filename, maxsize=100 * 1024 * 1024, write=False, lru_size=10000):
        env = self._open_env(filename, maxsize, write)
        env, txn, dbs, meta = self._open_dbs(env, write, lru_size)
        if not write:
            partition = ujson.loads(meta.get(txn, 'partition', 'null'))
            if partition:
                pdata = ujson.loads(meta.get(txn, '_pdata', 'null'))
                if not pdata:
                    raise ValueError("Can't load partition information from meta table.")
                db, _, _, _ = dbs[partition]
                db.echome = pdata
        return env, txn, dbs, meta

    def _open_dbs(self, env, write, lru_size):
        from pylru import LRUDict
        if write:
            txn = env.begin_txn()
        else:
            txn = env.begin_txn(flags=mdb.MDB_RDONLY)
        dbs = {}
        for index, column in self._columns.iteritems():
            subindexdb = None
            bitmap_dict = _dummy
            if column.index_indicator:
                # create an index for this column
                flags = mdb.MDB_CREATE
                if column.is_int:
                    flags |= mdb.MDB_INTEGERKEY
                subindexdb = env.open_db(txn,
                                         name="ix:%s" % index,
                                         flags=flags,
                                         key_inttype=column.get_effective_inttype())
                if write:
                    if column.index_indicator == 2:
                        evict = Victor(mdb_evict, txn, subindexdb)
                        fetch = Victor(mdb_fetch, txn, subindexdb)
                        bitmap_dict = LRUDict.getDict(lru_size,
                                                      fetch,
                                                      evict,
                                                      column.is_int,
                                                      BitSet)
                    else:
                        bitmap_dict = defaultdict(BitSet)

            flags = mdb.MDB_CREATE | mdb.MDB_INTEGERKEY
            if column.partition:
                subdb = PartitionDB(column.name)
            else:
                if column.is_int:
                    flags |= mdb.MDB_INTEGERDUP
                subdb = env.open_db(txn,
                                    name=index,
                                    flags=flags,
                                    key_inttype=mdb.MDB_UINT_32,
                                    value_inttype=column.get_effective_inttype())

            dbs[index] = (subdb, subindexdb, bitmap_dict, column)
        meta = env.open_db(txn, name='_meta_', flags=mdb.MDB_CREATE)
        return env, txn, dbs, meta

    def _insert(self, streams, preprocess=None, maxsize=1024 * 1024 * 1024,
                tmpdir='/tmp', decoder=None, lru_size=10000):
        """insert a file into the hustle table."""
        from wtrie import Trie

        if not decoder:
            decoder = json_decoder

        partitions = {}
        counters = {}
        autoincs = {}
        vid_tries = {}
        vid16_tries = {}
        page_size = 4096
        pdata = None

        try:
            for stream in streams:
                for line in stream:
                    # print "Line: %s" % line
                    try:
                        data = decoder(line)
                    except Exception as e:
                        print "Exception decoding record (skipping): %s %s" % (e, line)
                    else:
                        if preprocess:
                            preprocess(data)

                        newpdata = str(data.get(self._partition, ''))
                        if pdata != newpdata:
                            pdata = newpdata
                            if pdata in partitions:
                                bigfile, env, txn, dbs, meta, pmaxsize = partitions[pdata]
                            else:
                                bigfile = tempfile.mktemp(prefix="hustle", dir=tmpdir) + '.big'
                                env, txn, dbs, meta = self._open(bigfile, maxsize=maxsize, write=True, lru_size=lru_size)
                                page_size = env.stat()['ms_psize']
                                partitions[pdata] = bigfile, env, txn, dbs, meta, maxsize
                                counters[pdata] = 0
                                autoincs[pdata] = 1
                                vid_tries[pdata] = Trie()
                                vid16_tries[pdata] = Trie()
                                pmaxsize = maxsize

                        if counters[pdata] >= COMMIT_THRESHOLD:
                            txn.commit()
                            total_pages = pmaxsize / page_size
                            last_page = env.info()['me_last_pgno']
                            pages_left = total_pages - last_page
                            highwatermark = int(0.75 * total_pages)
                            if pages_left < highwatermark:
                                pmaxsize = int(pmaxsize * 1.5)
                                try:
                                    print "======= attempting to resize mmap ======"
                                    env.set_mapsize(pmaxsize)
                                    env, txn, dbs, meta = self._open_dbs(env, write=True, lru_size=lru_size)
                                except Exception as e:
                                    import traceback
                                    print "Error resizing MDB: %s" % e
                                    print traceback.format_exc(15)
                                    return 0, None
                            else:
                                txn = env.begin_txn()
                            #TODO: a bit a hack - need to reset txns and dbs for all of our indexes
                            #  (iff they are LRUDicts)
                            for index, (_, subindexdb, bitmap_dict, _) in dbs.iteritems():
                                if bitmap_dict is not _dummy and type(bitmap_dict) is not defaultdict:
                                    lru_evict = bitmap_dict._Evict
                                    lru_fetch = bitmap_dict._Fetch
                                    lru_evict.txn = lru_fetch.txn = txn
                                    lru_evict.db = lru_fetch.db = subindexdb
                            partitions[pdata] = bigfile, env, txn, dbs, meta, pmaxsize
                            counters[pdata] = 0

                        _insert_row(data, txn, dbs, autoincs[pdata], vid_tries[pdata], vid16_tries[pdata])
                        autoincs[pdata] += 1
                        counters[pdata] += 1

            files = {}
            total_records = 0
            for pdata, (bigfile, env, txn, dbs, meta, pmaxsize) in partitions.iteritems():
                try:
                    meta.put(txn, '_total_rows', str(autoincs[pdata]))
                    total_records += autoincs[pdata] - 1
                    vid_nodes, vid_kids, _ = vid_tries[pdata].serialize()
                    vid16_nodes, vid16_kids, _ = vid16_tries[pdata].serialize()
                    vn_ptr, vn_len = vid_nodes.buffer_info()
                    vk_ptr, vk_len = vid_kids.buffer_info()
                    vn16_ptr, vn16_len = vid16_nodes.buffer_info()
                    vk16_ptr, vk16_len = vid16_kids.buffer_info()
                    meta.put_raw(txn, '_vid_nodes', vn_ptr, vn_len)
                    meta.put_raw(txn, '_vid_kids', vk_ptr, vk_len)
                    meta.put_raw(txn, '_vid16_nodes', vn16_ptr, vn16_len)
                    meta.put_raw(txn, '_vid16_kids', vk16_ptr, vk16_len)
                    meta.put(txn, 'name', ujson.dumps(self._name))
                    meta.put(txn, 'fields', ujson.dumps(self._fields))
                    meta.put(txn, 'partition', ujson.dumps(self._partition))
                    meta.put(txn, '_pdata', ujson.dumps(pdata))
                    for index, (subdb, subindexdb, bitmap_dict, column) in dbs.iteritems():
                        if subindexdb:
                            # process all values for this bitmap index
                            if column.index_indicator == 2:
                                bitmap_dict.evictAll()
                            else:
                                for val, bitmap in bitmap_dict.iteritems():
                                    subindexdb.put(txn, val, bitmap.dumps())

                    txn.commit()
                except Exception as e:
                    print "Error writing to MDB: %s" % e
                    txn.abort()
                    import traceback
                    trace = traceback.format_exc(15)
                    print trace
                    return 0, None
                else:
                    # close dbs
                    meta.close()
                    for index, (subdb, subindexdb, _, _) in dbs.iteritems():
                        subdb.close()
                        if subindexdb:
                            subindexdb.close()
                    try:
                        outfile = bigfile[:-4]  # drop the '.big'
                        env.copy(outfile)
                        files[pdata] = outfile
                    except Exception as e:
                        print "Copy error: %s" % e
                        raise e
                env.close()
            return total_records, files
        finally:
            for _, (bigfile, _, _, _, _, _) in partitions.iteritems():
                os.unlink(bigfile)


class MarbleStream(object):
    def __init__(self, local_file):
        import ujson
        import socket
        self.marble = Marble.from_file(local_file)
        self.env, self.txn, self.dbs, self.meta = self.marble._open(local_file, write=False)
        self.number_rows = ujson.loads(self.meta.get(self.txn, '_total_rows'))
        self.vid_nodes, _ = self.meta.get_raw(self.txn, '_vid_nodes')
        self.vid_kids, _ = self.meta.get_raw(self.txn, '_vid_kids')
        self.vid16_nodes, _ = self.meta.get_raw(self.txn, '_vid16_nodes', (None, 0))
        self.vid16_kids, _ = self.meta.get_raw(self.txn, '_vid16_kids', (None, 0))
        self.partition = ujson.loads(self.meta.get(self.txn, 'partition', 'null'))
        self.pdata = ujson.loads(self.meta.get(self.txn, '_pdata', 'null'))
        self.host = socket.gethostname()

    def iter_all(self):
        return xrange(1, self.number_rows)

    def mget(self, column_name, keys):
        db, _, _, column = self.dbs[column_name]
        for data in db.mget(self.txn, keys):
            yield column.fetcher(data, self.vid16_nodes, self.vid16_kids, self.vid_nodes, self.vid_kids)

    def get(self, column_name, key):
        db, _, _, column = self.dbs[column_name]
        data = db.get(self.txn, key)
        return column.fetcher(data, self.vid16_nodes, self.vid16_kids, self.vid_nodes, self.vid_kids)

    def get_ix(self, column_name, key):
        _, idb, _, _ = self.dbs[column_name]
        bitset = BitSet()
        try:
            data = idb.get(self.txn, key)
            bitset.loads(data)
        except:
            pass
        return bitset

    def _vid_for_value(self, column, key):
        if column.is_trie:
            if column.rtrie_indicator == mdb.MDB_UINT_16:
                key = rtrie.vid_for_value(self.vid16_nodes, self.vid16_kids, key)
            else:
                key = rtrie.vid_for_value(self.vid_nodes, self.vid_kids, key)
        elif column.is_lz4:
            key = clz4.compress(key)
        return key

    def bit_eq(self, ix, key):
        _, idb, _, column = self.dbs[ix]
        rval = BitSet()
        zkey = self._vid_for_value(column, key)
        if zkey is not None:
            val = idb.get(self.txn, zkey)
            if val is not None:
                rval.loads(val)
        return rval

    def bit_ne(self, ix, key):
        _, idb, _, column = self.dbs[ix]
        rval = BitSet()
        key = self._vid_for_value(column, key)
        if key is not None:
            val = idb.get(self.txn, key)
            if val is not None:
                rval.loads(val)
                rval |= ZERO_BS
                rval.set(self.number_rows)
                rval.lnot_inplace()
        return rval

    def bit_eq_ex(self, ix, keys):
        from collections import Iterable
        _, idb, _, column = self.dbs[ix]
        rval = BitSet()
        for key in keys:
            if isinstance(key, Iterable) and not isinstance(key, (basestring, unicode)):
                # in case the key is a composite object, just grab the first one
                key = key[0]
            zkey = self._vid_for_value(column, key)
            if zkey is not None:
                val = idb.get(self.txn, zkey)
                if val is not None:
                    bitset = BitSet()
                    bitset.loads(val)
                    rval |= bitset
        return rval

    def bit_ne_ex(self, ix, keys):
        from collections import Iterable
        _, idb, _, column = self.dbs[ix]
        rval = BitSet()
        for key in keys:
            if isinstance(key, Iterable) and not isinstance(key, (basestring, unicode)):
                # in case the key is a composite object, just grab the first one
                key = key[0]
            zkey = self._vid_for_value(column, key)
            if zkey is not None:
                val = idb.get(self.txn, zkey)
                if val is not None:
                    bitset = BitSet()
                    bitset.loads(val)
                    rval |= bitset
        rval |= ZERO_BS
        rval.set(self.number_rows)
        rval.lnot_inplace()
        return rval

    def _bit_op(self, val, op):
        rval = BitSet()
        it = op(self.txn, val)
        for _, v in it:
            if v is None:
                continue
            bitset = BitSet()
            bitset.loads(v)
            rval |= bitset
        return rval

    def bit_lt(self, ix, val):
        _, idb, _, _ = self.dbs[ix]
        return self._bit_op(val, idb.get_lt)

    def bit_gt(self, ix, val):
        _, idb, _, _ = self.dbs[ix]
        return self._bit_op(val, idb.get_gt)

    def bit_le(self, ix, val):
        _, idb, _, _ = self.dbs[ix]
        return self._bit_op(val, idb.get_le)

    def bit_ge(self, ix, val):
        _, idb, _, _ = self.dbs[ix]
        return self._bit_op(val, idb.get_ge)

    def close(self):
        try:
            self.txn.commit()
            self.env.close()
        except:
            pass


class Column(object):
    """
    A *Column* is the named, typed field of a :class:`Marble <hustle.core.marble.Marble>`.   *Columns* are typically
    created automatically by parsing the *fields* of a the *Marble* instantiation.

    The *Column* overrides Python's relational operators :code:`> < <= >= ==` which forms the basis for
    the :ref:`Query DSL <queryguide>`.  All of these operators expect a *Python literal* as their second
    (right hand side) argument which should be the same type as the *Column*.  These *Column Expressions* are
    represented by the :class:`Expr <hustle.core.marble.Expr>` class.

    Note that the *Marble* and *Table* classes expose their *Columns* as Python *attributes*::

        # instantiate a table
        imps = Table.from_tag('impressions')

        # access a the date column
        date_column = imps.date

        # create a Column Expression
        site_column_expression = imps.site_id == 'google.com'

        # create another Column Expression
        date_column_expression = date_column > '2014-03-07'

        # query
        select(date_column, where=date_column_expression & )

    """
    def __init__(self, name, table, index_indicator=0, partition=False, type_indicator=0,
                 compression_indicator=0, rtrie_indicator=mdb.MDB_UINT_32, alias=None):
        self.name = name
        self.fullname = "%s.%s" % (table._name, name) if hasattr(table, '_name') else name
        self.table = table
        self.type_indicator = type_indicator
        self.partition = partition
        self.index_indicator = index_indicator
        self.compression_indicator = compression_indicator
        self.rtrie_indicator = rtrie_indicator
        self.alias = alias
        self.is_trie = type_indicator == mdb.MDB_STR and compression_indicator == 0
        self.is_lz4 = type_indicator == mdb.MDB_STR and compression_indicator == 2
        self.is_binary = type_indicator == mdb.MDB_STR and compression_indicator == 3
        self.is_int = self.type_indicator != mdb.MDB_STR or self.compression_indicator == 0
        self.is_numeric = self.type_indicator > 0
        self.is_index = self.index_indicator > 0
        self.is_wide = self.index_indicator == 2

        # use dictionary (trie) compression if required
        if self.is_trie:
            if self.rtrie_indicator == mdb.MDB_UINT_16:
                self.converter = _convert_vid16
                self.fetcher = _fetch_vid16
            else:
                self.converter = _convert_vid
                self.fetcher = _fetch_vid
            self.default_value = ''
        elif self.is_lz4:
            self.converter = _convert_lz4
            self.fetcher = _fetch_lz4
            self.default_value = ''
        elif self.is_int:
            self.converter = _convert_int
            self.fetcher = _fetch_me
            self.default_value = 0
        else:
            self.converter = _convert_str
            self.fetcher = _fetch_me
            self.default_value = ''

    def named(self, alias):
        """
        return a new column that has an alias that will be used in the resulting schema
        :type alias: str
        :param alias: the name of the alias
        """
        newcol = Column(self.name, self.table, self.index_indicator, self.partition, self.type_indicator,
                        self.compression_indicator, self.rtrie_indicator, alias)
        return newcol

    @property
    def column(self):
        return self

    def schema_string(self):
        """
        return the schema for this column.  This is used to build the schema of a query result, so we need to
        use the alias.
        """
        rval = self.alias or self.name
        indexes = ['', '+', '=']
        prefix = indexes[self.index_indicator]
        lookup = ['', '#4', '@4', '#2', '@2', '#1', '@1', '#8', '@8']

        if self.type_indicator == mdb.MDB_STR:
            if self.compression_indicator == 0:
                prefix += '%'
                if self.rtrie_indicator == mdb.MDB_UINT_32:
                    prefix += '4'
                elif self.rtrie_indicator == mdb.MDB_UINT_16:
                    prefix += '2'
            elif self.compression_indicator == 1:
                prefix += '$'
            elif self.compression_indicator == 2:
                prefix += '*'
            elif self.compression_indicator == 3:
                prefix += '&'
        else:
            prefix += lookup[self.type_indicator]
        return prefix + rval

    def description(self):
        """
        Return a human-readable type description for this column.
        """
        type_lookup = ['', 'int32', 'uint32', 'int16', 'uint16', 'int8',
                       'uint8', 'int64', 'uint64']
        dict_lookup = ['', '', '32', '', '16', '', '']
        string_lookup = ['trie', 'string', 'lz4', 'binary']
        rval = type_lookup[self.type_indicator]
        if not self.type_indicator:
            rval += string_lookup[self.compression_indicator]
            if self.compression_indicator == 0:
                rval += dict_lookup[self.rtrie_indicator]
        inds = [rval]
        if self.index_indicator:
            inds.append("IX")
        if self.partition:
            inds.append("PT")
        return "%s (%s)" % (self.alias or self.name, ','.join(inds))

    def get_effective_inttype(self):
        if self.type_indicator == mdb.MDB_STR and self.compression_indicator == 0:
            return self.rtrie_indicator
        return self.type_indicator

    def _get_expr(self, op, part_op, other):
        if not self.partition \
                and op in [in_lt, in_gt, in_ge, in_le] \
                and (self.is_trie or self.is_lz4 or self.is_binary):
            raise TypeError("Column %s doesn't support range query." % self.fullname)
        if not self.is_index:
            raise TypeError("Column %s is not an index, cannot appear in 'where' clause." % self.fullname)
        part_expr = partial(part_op, other=other) if self.partition else part_all
        return Expr(self.table,
                    partial(op, col=self.name, other=other),
                    part_expr,
                    self.partition)

    def __lshift__(self, other):
        return self._get_expr(in_in, part_in, other=other)

    def __rshift__(self, other):
        return self._get_expr(in_not_in, part_not_in, other=other)

    def __eq__(self, other):
        return self._get_expr(in_eq, part_eq, other=other)

    def __ne__(self, other):
        return self._get_expr(in_ne, part_ne, other=other)

    def __lt__(self, other):
        return self._get_expr(in_lt, part_lt, other=other)

    def __gt__(self, other):
        return self._get_expr(in_gt, part_gt, other=other)

    def __ge__(self, other):
        return self._get_expr(in_ge, part_ge, other=other)

    def __le__(self, other):
        return self._get_expr(in_le, part_le, other=other)

    def __str__(self):
        return self.description()


class Aggregation(object):
    """
    An *Aggregation* is a Column Function that represents some aggregating computation over the values of that
    column.  It is exclusively used in the *project* section of the :func:`select() <hustle.select>` function.

    An *Aggregation* object holds onto four distinct function references which are called at specific times
    during the *group_by stage* of the query pipeline.

    :type f: func(accumulator, value)
    :param f: the function called for every value of the *column*, returns a new accumulator (the MAP aggregator)

    :type g: func(accumulator)
    :param g: the function called to produce the final value (the REDUCE aggregator)

    :type h: func(accumulator)
    :param h: the function called to produce intermediate values (the COMBINE aggregator)

    :type default: func()
    :param default:  the function called to produce the initial aggregation accumulator

    :type column: :class:`Column <hustle.core.marble.Column>`
    :param column: the column to aggregate

    :type name: basestring
    :param name: the unique name of the aggregator.  Used to assign a column name to the result

    .. note::

        Here is the actual implementation of the h_avg() *Aggregation* which will give us the average value
        for a numeric column in a :func:`select() <hustle.select>` statement::

            def h_avg(col):
                return Aggregation("avg",
                                   col,
                                   f=lambda (accum, count), val: (accum + val, count + 1),
                                   g=lambda (accum, count): float(accum) / count,
                                   h=lambda (accum, count): (accum, count),
                                   default=lambda: (0, 0))

        First look at the *default()* function which returns the tuple (0, 0).  This sets the *accum* and *count* values
        that we will be tracking both to zero.  Next, let's see what's happening in the *f()* function.  Note that it
        performs two computations, one :code:`accum + val` builds a sum of the values, and the :code:`count + 1` will
        count the total number of values.  The difference between the *g()* and *h()* functions is when they take place.
        The *h()* function is used to summarize results.  It should always return an *accum* that can be further
        inputted into the *f()* function.  The *g()* function is used at the very end of the computation to compute the
        final value to return the client.

    .. seealso::
        :func:`h_sum() <hustle.h_sum>`, :func:`h_count() <hustle.h_count>`, :func:`h_avg() <hustle.h_avg>`
            Some of Hustle's aggregation functions

    """
    def __init__(self, name, column, f=None, g=lambda a: a, h=lambda a: a, default=lambda: None,
                 is_numeric=None, is_binary=None):

        self.column = column
        self.f = f
        self.g = g
        self.h = h
        self.default = default
        self.name = "%s(%s)" % (name, column.name if column else '')
        self.fullname = "%s(%s)" % (name, column.fullname if column else '')

        self.is_numeric = column.is_numeric if column else False
        self.is_binary = column.is_binary if column else False

    @property
    def table(self):
        return self.column.table

    def named(self, alias):
        newag = Aggregation(self.name, self.column.named(alias), self.f, self.g, self.h, self.default)
        return newag


class Expr(object):
    """
    The *Expr* is returned by the overloaded relational operators of the :class:`Column <hustle.core.marble.Column>`
    class.

    The *Expr* is a recursive class, that can be composed of other *Exprs* all connected with the logical
    :code:`& | ~` operators (*and, or, not*).

    Each *Expr* instance must be aware if its sub-expressions *have* partitions or *are* partitions.  This is to
    because the *&* and *|* operators will optimize expressions over patitioned columns differently.  Consider the
    following query::

        select(impressions.site_id, where=(impressions.date == '2014-02-20') & (impressions.amount > 10))

    Let's assume that the *impressions.date* column is a partition column.  It should be clear that we can optimize
    this query by only executing the query on the '2014-02-20' partition, which if we had many dates would vastly
    improve our query execution.

    On the other hand consider the following, almost identical query::

        select(impressions.site_id, where=(impressions.date == '2014-02-20') | (impressions.amount > 10))

    In this case, we cannot optimize according to our partition.  We need to visit *all* partitions in *impressions*
    and execute the *OR* operation across those rows for the *amount* expression.

    .. note::

        It is important to realize that all Column Expressions in an Expr must refer to the same *Table*

    :type table: :class:`Table <hustle.Table>`
    :param table: the *Table* this *Expr* queries

    :type f: func(MarbleStream)
    :param f: the function to execute to actually perform the expression on data in the *Marble*

    :type part_f: func(list of strings)
    :param part_f: the function to execute to perform the expresson on data in the *partition*

    :type is_partition: bool
    :param is_partition: indicates if this Expr only has partition columns

    """
    def __init__(self, table, f=None, part_f=None, is_partition=False):
        if not part_f:
            part_f = part_all
        self.table = table
        self.f = f
        self.part_f = part_f
        self.is_partition = is_partition

    @property
    def _name(self):
        return self.table._name

    @property
    def has_partition(self):
        return self.part_f != part_all

    @property
    def has_no_partition(self):
        return not self.has_partition

    def _assert_unity(self, other_expr):
        if self.table is not None and other_expr.table is not None and self.table._name != other_expr.table._name:
            raise Exception("Error Expression must have a single table: %s != %s" %
                            (self.table._name, other_expr.table._name))

    def __and__(self, other_expr):
        self._assert_unity(other_expr)

        # p & p
        if self.is_partition and other_expr.is_partition:
            return Expr(self.table,
                        None,
                        partial(part_conditional, op='and', l_expr=self.part_f, r_expr=other_expr.part_f),
                        True)
        # n & n
        elif self.has_no_partition and other_expr.has_no_partition:
            return Expr(self.table,
                        partial(in_conditional, op='and', l_expr=self.f, r_expr=other_expr.f),
                        part_all)
        # n & p
        elif (self.is_partition and other_expr.has_no_partition) or \
                (self.has_no_partition and other_expr.is_partition):
            if self.is_partition:
                f_expr = other_expr.f
                pf_expr = self.part_f
            else:
                f_expr = self.f
                pf_expr = other_expr.part_f
            return Expr(self.table, f_expr, pf_expr)
        # n & np
        elif (self.has_no_partition and other_expr.has_partition) or \
                (self.has_partition and other_expr.has_no_partition):
            if self.has_partition:
                pf_expr = self.part_f
            else:
                pf_expr = other_expr.part_f
            return Expr(self.table,
                        partial(in_conditional, op='and', l_expr=self.f, r_expr=other_expr.f),
                        pf_expr)
        # p & np
        elif (self.is_partition and other_expr.has_partition) or (self.has_partition and other_expr.is_partition):
            if self.is_partition:
                f_expr = other_expr.f
            else:
                f_expr = self.f
            return Expr(self.table,
                        f_expr,
                        partial(part_conditional, op='and', l_expr=self.part_f, r_expr=other_expr.part_f))
        # np & np
        elif self.has_partition and other_expr.has_partition:
            return Expr(self.table,
                        partial(in_conditional, op='and', l_expr=self.f, r_expr=other_expr.f),
                        partial(part_conditional, op='and', l_expr=self.part_f, r_expr=other_expr.part_f))
        else:
            raise Exception("Error in partitions (and)")

    def __or__(self, other_expr):
        self._assert_unity(other_expr)

        # p | p
        if self.is_partition and other_expr.is_partition:
            return Expr(self.table,
                        partial(in_conditional, op='or', l_expr=self.f, r_expr=other_expr.f),
                        partial(part_conditional, op='or', l_expr=self.part_f, r_expr=other_expr.part_f),
                        True)
        # p | np
        elif (self.is_partition and other_expr.has_partition) or (self.has_partition and other_expr.is_partition):
            if self.is_partition:
                f_expr = other_expr.f
            else:
                f_expr = self.f
            return Expr(self.table,
                        f_expr,
                        partial(part_conditional, op='or', l_expr=self.part_f, r_expr=other_expr.part_f))
        # np | np
        elif self.has_partition and other_expr.has_partition:
            return Expr(self.table,
                        partial(in_conditional, op='or', l_expr=self.f, r_expr=other_expr.f),
                        partial(part_conditional, op='or', l_expr=self.part_f, r_expr=other_expr.part_f))
        # n | n, n | p, n | np
        elif self.has_no_partition or other_expr.has_no_partition:
            return Expr(self.table,
                        partial(in_conditional, op='or', l_expr=self.f, r_expr=other_expr.f),
                        part_all)
        else:
            raise Exception("Error in partitions (or)")

    def __invert__(self):
        # invert partition function only if it's not part_all
        if self.has_partition:
            return Expr(self.table,
                        partial(in_not, expr=self.f),
                        partial(in_not, expr=self.part_f),
                        self.is_partition)
        else:
            return Expr(self.table,
                        partial(in_not, expr=self.f),
                        self.part_f,
                        self.is_partition)

    def __call__(self, tablet, invert=False):
        return self.f(tablet, invert)

    def partition(self, tags, invert=False):
        return self.part_f(tags, invert)


def _convert_vid16(val, vid_trie=None, vid_trie16=None):
    try:
        val = str(val)
    except:
        val = val.encode('utf-8')

    return vid_trie16.add(val)


def _convert_vid(val, vid_trie=None, vid_trie16=None):
    try:
        val = str(val)
    except:
        val = val.encode('utf-8')
    return vid_trie.add(val)


def _convert_int(val, vid_trie=None, vid_trie16=None):
    if type(val) is int or type(val) is long:
        return val
    return int(val)


def _convert_str(val, vid_trie=None, vid_trie16=None):
    try:
        val = str(val)
    except:
        val = val.encode('utf-8')
    return val


def _convert_lz4(val, vid_trie=None, vid_trie16=None):
    try:
        val = str(val)
    except:
        val = val.encode('utf-8')
    return clz4.compress(val)


def _fetch_vid16(vid, vid16_nodes, vid16_kids, vid_nodes, vid_kids):
    return rtrie.value_for_vid(vid16_nodes, vid16_kids, vid)


def _fetch_vid(vid, vid16_nodes, vid16_kids, vid_nodes, vid_kids):
    return rtrie.value_for_vid(vid_nodes, vid_kids, vid)


def _fetch_lz4(data, vid16_nodes, vid16_kids, vid_nodes, vid_kids):
    return clz4.decompress(data)


def _fetch_me(data, vid16_nodes, vid16_kids, vid_nodes, vid_kids):
    return data


def in_not(obj, invert, expr):
    if expr is None:
        return BitSet()
    return expr(obj, not invert)


def part_all(tags, invert=False):
    if invert:
        return []
    return tags


def part_conditional(obj, invert, op, l_expr, r_expr):
    if (op == 'and' and not invert) or (op == 'or' and invert):
        # and
        l_set = set(l_expr(obj, invert))
        for uid in r_expr(obj, invert):
            if uid in l_set:
                yield uid
    else:
        # or
        l_set = set(l_expr(obj, invert))
        for uid in l_set:
            yield uid
        for uid in r_expr(obj, invert):
            if uid not in l_set:
                yield uid


def in_conditional(tablet, invert, op, l_expr, r_expr):
    if (op == 'and' and not invert) or (op == 'or' and invert):
        # and
        if l_expr is None:
            return r_expr(tablet, invert)
        elif r_expr is None:
            return l_expr(tablet, invert)
        return l_expr(tablet, invert) & r_expr(tablet, invert)
    else:
        # or
        if l_expr is None or r_expr is None:
            return None
        return l_expr(tablet, invert) | r_expr(tablet, invert)


def in_in(tablet, invert, col, other):
    from collections import Iterable
    if isinstance(other, Iterable) and not isinstance(other, (basestring, unicode)):
        other = set(other)
        if invert:
            return tablet.bit_ne_ex(col, other)
        return tablet.bit_eq_ex(col, other)
    else:
        raise ValueError("Item in contains must be an iterable.")


def part_in(tags, invert, other):
    from collections import Iterable
    if isinstance(other, Iterable) and not isinstance(other, (basestring, unicode)):
        other = set(other)
        if invert:
            return part_not_in(tags, False, other)
        return (t for t in tags if t in other)
    else:
        raise ValueError("Item in contains must be an iterable.")


def in_not_in(tablet, invert, col, other):
    from collections import Iterable
    if isinstance(other, Iterable) and not isinstance(other, (basestring, unicode)):
        other = set(other)
        if invert:
            return tablet.bit_eq_ex(col, other)
        return tablet.bit_ne_ex(col, other)
    else:
        raise ValueError("Item in contains must be an iterable.")


def part_not_in(tags, invert, other):
    from collections import Iterable
    if isinstance(other, Iterable) and not isinstance(other, (basestring, unicode)):
        other = set(other)
        if invert:
            return part_in(tags, False, other)
        return (t for t in tags if t not in other)
    else:
        raise ValueError("Item in contains must be an iterable.")


def in_eq(tablet, invert, col, other):
    if invert:
        return tablet.bit_ne(col, other)
    return tablet.bit_eq(col, other)


def part_eq(tags, invert, other):
    if invert:
        return part_ne(tags, False, other)
    return (t for t in tags if t == other)


def in_ne(tablet, invert, col, other):
    if invert:
        return tablet.bit_eq(col, other)
    return tablet.bit_ne(col, other)


def part_ne(tags, invert, other):
    if invert:
        return part_eq(tags, False, other)
    return (t for t in tags if t != other)


def in_lt(tablet, invert, col, other):
    if invert:
        return tablet.bit_ge(col, other)
    return tablet.bit_lt(col, other)


def part_lt(tags, invert, other):
    if invert:
        return part_ge(tags, False, other)
    return (t for t in tags if t < other)


def in_gt(tablet, invert, col, other):
    if invert:
        return tablet.bit_le(col, other)
    return tablet.bit_gt(col, other)


def part_gt(tags, invert, other):
    if invert:
        return part_le(tags, False, other)
    return (t for t in tags if t > other)


def in_ge(tablet, invert, col, other):
    if invert:
        return tablet.bit_lt(col, other)
    return tablet.bit_ge(col, other)


def part_ge(tags, invert, other):
    if invert:
        return part_lt(tags, False, other)
    return (t for t in tags if t >= other)


def in_le(tablet, invert, col, other):
    if invert:
        return tablet.bit_gt(col, other)
    return tablet.bit_le(col, other)


def part_le(tags, invert, other):
    if invert:
        return part_gt(tags, False, other)
    return (t for t in tags if t <= other)


def check_query(select, join, order_by, limit, wheres):
    """Query checker for hustle."""

    if not len(wheres):
        raise ValueError("Where clause must have at least one table. where=%s" % repr(wheres))

    tables = {}
    for where in wheres:
        if isinstance(where, Marble):
            table = where
        else:
            table = where.table
        if table._name in tables:
            raise ValueError("Table %s occurs twice in the where clause."
                             % where._name)
        tables[table._name] = table

    if len(select) == 0:
        raise ValueError("No items in the select clause.")

    selects = set()
    for i, c in enumerate(select):
        name = c.fullname
        if c.table and c.table._name not in tables:
            raise ValueError("Selected column %s is not from the given tables"
                             " in the where clauses." % name)
        if name in selects:
            raise ValueError("Duplicate column %s in the select list." % name)
        selects.add(name)
        selects.add(c.name)
        selects.add(i)

    if join:
        if len(tables) != 2:
            raise ValueError("Query with join takes exact two tables, %d given."
                             % len(tables))
        if len(join) != 2:
            raise ValueError("Join takes exact two columns, %d given."
                             % len(join))
        if join[0].table._name == join[1].table._name:
            raise ValueError("Join columns belong to a same table.")
        if join[0].type_indicator != join[1].type_indicator:
            raise ValueError("Join columns have different types.")
        for c in join:
            if c.table._name not in tables:
                name = '%s.%s' % (c.table._name, c.name)
                raise ValueError("Join column %s is not from the given tables"
                                 " in the where clauses." % name)

    if order_by:
        for c in order_by:
            name = c.fullname if isinstance(c, (Column, Aggregation)) else c
            if name not in selects:
                raise ValueError("Order_by column %s is not in the select list."
                                 % name)

    if limit is not None:
        if limit < 0:
            raise ValueError("Negtive number is not allowed in the limit.")

    return True


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


class DUMMY(object):
    tobj = None

    def __getitem__(self, item):
        return self.tobj

_dummy = DUMMY()
_dummy_bitmap = DUMMY()
_dummy.tobj = _dummy_bitmap

ZERO_BS = BitSet()
ZERO_BS.set(0)

setattr(_dummy_bitmap, 'set', (lambda k: k))
setattr(_dummy, 'get', lambda k: _dummy_bitmap)


def kv_decoder(line, kvs=()):
    key, value = line
    return dict(zip(kvs, key + value))


def json_decoder(line):
    return ujson.loads(line)


class Victor(object):
    def __init__(self, fn, txn, db):
        self.fn = fn
        self.txn = txn
        self.db = db

    def __call__(self, *args):
        if len(args) > 1:
            return self.fn(args[0], args[1], txn=self.txn, ixdb=self.db)
        else:
            return self.fn(args[0], txn=self.txn, ixdb=self.db)


def _insert_row(data, txn, dbs, row_id, vid_trie, vid16_trie):
    column = None
    try:
        for subdb, _, bitmap_dict, column in dbs.itervalues():

            val = column.converter(data.get(column.name, column.default_value) or column.default_value,
                                   vid_trie, vid16_trie)
            subdb.put(txn, row_id, val)
            bitmap_dict[val].set(row_id)
    except Exception as e:
        print "Can't INSERT: %s %s: %s" % (repr(data), column, e)


class PartitionDB(object):
    '''
    A fake mdb-like class just for partition columns.
    '''
    def __init__(self, partition):
        self.echome = partition

    def put(self, txn, row_id, val):
        return

    def mget(self, txn, keys, default=None):
        '''keys should be a bitmap
        '''
        for i in range(len(keys)):
            yield self.echome

    def get(self, txn, key, default=None):
        return self.echome

    def close(self):
        return
