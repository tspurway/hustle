cimport cmdb
from libc.stdint cimport uint8_t, int8_t, uint16_t, int16_t, uint32_t, \
    int32_t, uint64_t, int64_t
from cpython.int cimport PyInt_FromLong
from cpython.long cimport PyLong_FromLongLong, PyLong_FromUnsignedLongLong

# env creation flags
MDB_FIXEDMAP = 0x01
MDB_NOSUBDIR = 0x4000
MDB_NOSYNC = 0x10000
MDB_RDONLY = 0x20000
MDB_NOMETASYNC = 0x40000
MDB_WRITEMAP = 0x80000
MDB_MAPASYNC = 0x100000
MDB_NOTLS = 0x200000
MDB_NOLOCK = 0x400000
MDB_NORDAHEAD = 0x800000

# db open flags
MDB_REVERSEKEY = 0x02
MDB_DUPSORT = 0x04
MDB_INTEGERKEY = 0x08
MDB_DUPFIXED = 0x10
MDB_INTEGERDUP = 0x20
MDB_REVERSEDUP = 0x40
MDB_CREATE = 0x40000

# write flags
MDB_NOOVERWRITE = 0x10
MDB_NODUPDATA = 0x20
MDB_CURRENT = 0x40
MDB_RESERVE = 0x10000
MDB_APPEND = 0x20000
MDB_APPENDDUP = 0x40000
MDB_MULTIPLE = 0x80000

# cursor operations
MDB_FIRST = 0
MDB_FIRST_DUP = 1
MDB_GET_BOTH = 2
MDB_GET_BOTH_RANGE = 3
MDB_GET_CURRENT = 4
MDB_GET_MULTIPLE = 5
MDB_LAST = 6
MDB_LAST_DUP = 7
MDB_NEXT = 8
MDB_NEXT_DUP = 9
MDB_NEXT_MULTIPLE = 10
MDB_NEXT_NODUP = 11
MDB_PREV = 12
MDB_PREV_DUP = 13
MDB_PREV_NODUP = 14
MDB_SET = 15
MDB_SET_KEY = 16
MDB_SET_RANGE = 17


# Custom defs
KB = 1024
MB = 1024 * 1024
GB = 1024 * 1024 * 1024
MDB_MAX_KEYVALUE_SIZE = 511
MDB_COMMIT_THRESHOLD = 300000

# breakdown of mdb data types
MDB_STR = 0
MDB_INT_32 = 1
MDB_UINT_32 = 2
MDB_INT_16 = 3
MDB_UINT_16 = 4
MDB_INT_8 = 5
MDB_UINT_8 = 6
MDB_INT_64 = 7
MDB_UINT_64 = 8

# comparison functions for small integers (1 or 2) bytes.
cdef inline int int16_compare(const cmdb.MDB_val *a, const cmdb.MDB_val *b):
    return (<int16_t*>a.mv_data)[0] - (<int16_t*>b.mv_data)[0]

cdef inline int uint16_compare(const cmdb.MDB_val *a, const cmdb.MDB_val *b):
    return (<uint16_t*>a.mv_data)[0] - (<uint16_t*>b.mv_data)[0]

cdef inline int int8_compare(const cmdb.MDB_val *a, const cmdb.MDB_val *b):
    return (<int8_t*>a.mv_data)[0] - (<int8_t*>b.mv_data)[0]

cdef inline int uint8_compare(const cmdb.MDB_val *a, const cmdb.MDB_val *b):
    return (<uint8_t*>a.mv_data)[0] - (<uint8_t*>b.mv_data)[0]

# set key comparison function for small integers (1 or 2) bytes.
cdef int set_key_cmp(cmdb.MDB_txn* txn, cmdb.MDB_dbi db, uint32_t inttype):
    if inttype == MDB_INT_8:
        return cmdb.mdb_set_compare(txn, db, int8_compare)
    elif inttype == MDB_UINT_8:
        return cmdb.mdb_set_compare(txn, db, uint8_compare)
    if inttype == MDB_INT_16:
        return cmdb.mdb_set_compare(txn, db, int16_compare)
    elif inttype == MDB_UINT_16:
        return cmdb.mdb_set_compare(txn, db, uint16_compare)
    else:
        return 0

# set value comparison function for small integers (1 or 2) bytes.
cdef int set_value_cmp(cmdb.MDB_txn* txn, cmdb.MDB_dbi db, uint32_t inttype):
    if inttype == MDB_INT_8:
        return cmdb.mdb_set_dupsort(txn, db, int8_compare)
    elif inttype == MDB_UINT_8:
        return cmdb.mdb_set_dupsort(txn, db, uint8_compare)
    if inttype == MDB_INT_16:
        return cmdb.mdb_set_dupsort(txn, db, int16_compare)
    elif inttype == MDB_UINT_16:
        return cmdb.mdb_set_dupsort(txn, db, uint16_compare)
    else:
        return 0

# function type for integer casting
ctypedef object (*cast_fn)(void *)

cdef inline object _cast_int8(void *p):
    return PyInt_FromLong((<int8_t *>p)[0])

cdef inline object _cast_uint8(void *p):
    return PyInt_FromLong((<uint8_t *>p)[0])

cdef inline object _cast_int16(void *p):
    return PyInt_FromLong((<int16_t *>p)[0])

cdef inline object _cast_uint16(void *p):
    return PyInt_FromLong((<uint16_t *>p)[0])

cdef inline object _cast_int32(void *p):
    return PyInt_FromLong((<int32_t *>p)[0])

cdef inline object _cast_uint32(void *p):
    return PyLong_FromUnsignedLongLong((<uint32_t *>p)[0])

cdef inline object _cast_int64(void *p):
    return PyLong_FromLongLong((<int64_t *>p)[0])

cdef inline object _cast_uint64(void *p):
    return PyLong_FromUnsignedLongLong((<uint64_t *>p)[0])

cdef cast_fn _setup_caster(uint32_t t):
    if t == MDB_INT_32:
        return _cast_int32
    elif t == MDB_UINT_32:
        return _cast_uint32
    elif t == MDB_INT_16:
        return _cast_int16
    elif t == MDB_UINT_16:
        return _cast_uint16
    elif t == MDB_INT_8:
        return _cast_int8
    elif t == MDB_UINT_8:
        return _cast_uint8
    elif t == MDB_INT_64:
        return _cast_int64
    else:
        return _cast_uint64

cdef size_t _size_int(uint32_t t):
    if t == MDB_INT_32 or t == MDB_UINT_32:
        return sizeof(int32_t)
    elif t == MDB_INT_16 or t == MDB_UINT_16:
        return sizeof(int16_t)
    elif t == MDB_INT_8 or t == MDB_UINT_8:
        return sizeof(int8_t)
    else:
        return sizeof(int64_t)


class KeyExistError(Exception):
    pass


class KeyNotFoundError(Exception):
    pass


class MapFullError(Exception):
    pass


class TxnFullError(Exception):
    pass


cdef class Txn:
    cdef cmdb.MDB_txn *txn

    def __init__(self, Env env, Txn parent=None, uint32_t flags=0):
        cdef cmdb.MDB_txn *parent_txn = NULL
        if parent:
            parent_txn = parent.txn

        err = cmdb.mdb_txn_begin(env.env, parent_txn, flags, &self.txn)
        if err:
            raise Exception("Error creating master transaction: %s"
                            % cmdb.mdb_strerror(err))

    def commit(self):
        err = cmdb.mdb_txn_commit(self.txn)
        if err:
            raise Exception("Error committing transaction: %s"
                            % cmdb.mdb_strerror(err))

    def abort(self):
        cmdb.mdb_txn_abort(self.txn)

    def reset(self):
        '''Both reset and renew work on only readonly transaction.
        '''
        cmdb.mdb_txn_reset(self.txn)

    def renew(self):
        err = cmdb.mdb_txn_renew(self.txn)
        if err:
            raise Exception("Error renewing transaction: %s"
                            % cmdb.mdb_strerror(err))


cdef class Env:
    cdef cmdb.MDB_env *env

    def __init__(self, char *filename,
                 uint32_t flags=MDB_WRITEMAP | MDB_NOSYNC,
                 int permissions=0664, size_t mapsize=0, int max_dbs=8,
                 int max_readers=1024):
        err = cmdb.mdb_env_create(&self.env)
        if err:
            raise Exception("Error creating environment: %s"
                            % cmdb.mdb_strerror(err))
        if mapsize:
            err = cmdb.mdb_env_set_mapsize(self.env, mapsize)
            if err:
                raise Exception("Could not set environment size: %s"
                                % cmdb.mdb_strerror(err))
        if max_readers:
            err = cmdb.mdb_env_set_maxreaders(self.env, max_readers)
            if err:
                raise Exception("Could not set max readers: %s"
                                % cmdb.mdb_strerror(err))
        if max_dbs:  # set max_dbs > 0 to enable multiple named dbs
            err = cmdb.mdb_env_set_maxdbs(self.env, max_dbs)
            if err:
                raise Exception("Could not set max dbs: %s"
                                % cmdb.mdb_strerror(err))

        err = cmdb.mdb_env_open(self.env, filename, flags, permissions)
        if err:
            raise Exception("Error opening environment: %s"
                            % cmdb.mdb_strerror(err))

    def set_mapsize(self, size_t mapsize):
        err = cmdb.mdb_env_set_mapsize(self.env, mapsize)
        if err:
            raise Exception("Could not set environment size, make sure all\
                            txns have been closed: %s" % cmdb.mdb_strerror(err))

    def close(self):
        cmdb.mdb_env_close(self.env)

    def copy(self, char *filename):
        err = cmdb.mdb_env_copy(self.env, filename)
        if err:
            raise Exception("Error copying environment: %s"
                            % cmdb.mdb_strerror(err))

    def stat(self):
        cdef cmdb.MDB_stat stat

        err = cmdb.mdb_env_stat(self.env, &stat)
        if err:
            raise Exception("Error 'stat'ing environment: %s"
                            % cmdb.mdb_strerror(err))
        return dict(ms_psize=stat.ms_psize,
                    ms_depth=stat.ms_depth,
                    ms_branch_pages=stat.ms_branch_pages,
                    ms_leaf_pages=stat.ms_leaf_pages,
                    ms_overflow_pages=stat.ms_overflow_pages,
                    ms_entries=stat.ms_entries)

    def info(self):
        cdef cmdb.MDB_envinfo info

        err = cmdb.mdb_env_info(self.env, &info)
        if err:
            raise Exception("Error 'info'ing environment: %s"
                            % cmdb.mdb_strerror(err))
        return dict(me_mapaddr=<long>info.me_mapaddr,
                    me_mapsize=info.me_mapsize,
                    me_last_pgno=info.me_last_pgno,
                    me_last_txnid=info.me_last_txnid,
                    me_maxreaders=info.me_maxreaders,
                    me_numreaders=info.me_numreaders)

    def sync(self, force=False):
        err = cmdb.mdb_env_sync(self.env, force)
        if err:
            raise Exception("Error sycning environment: %s"
                            % cmdb.mdb_strerror(err))

    def set_flags(self, uint32_t flags, int onoff):
        err = cmdb.mdb_env_set_flags(self.env, flags, onoff)
        if err:
            raise Exception("Error setting flags in environment: %s"
                            % cmdb.mdb_strerror(err))

    def get_flags(self):
        cdef uint32_t flags

        err = cmdb.mdb_env_get_flags(self.env, &flags)
        if err:
            raise Exception("Error getting flags in environment: %s"
                            % cmdb.mdb_strerror(err))
        return flags

    def begin_txn(self, Txn parent=None, uint32_t flags=0):
        cdef Txn txn

        txn = Txn(self, parent, flags)
        return txn

    def open_db(self, Txn txn, name=None,
                uint32_t flags=MDB_CREATE | MDB_DUPSORT,
                key_inttype=MDB_INT_32, value_inttype=MDB_INT_32):
        cdef DB db

        if flags & MDB_INTEGERKEY:
            if flags & MDB_INTEGERDUP:
                db = IntIntDB(self, txn, name, flags, key_inttype, value_inttype)
            else:
                db = IntStrDB(self, txn, name, flags, key_inttype)
        else:
            if flags & MDB_INTEGERDUP:
                db = StrIntDB(self, txn, name, flags, value_inttype)
            else:
                db = DB(self, txn, name, flags)
        return db


cdef class DB:
    cdef cmdb.MDB_dbi dbi
    cdef Env env
    cdef uint32_t flags

    def __init__(self, Env env, Txn txn, name=None,
                 uint32_t flags=MDB_DUPSORT | MDB_CREATE):
        cdef char *cname
        self.env = env
        self.flags = flags
        cname = NULL if name is None else <char*>name
        err = cmdb.mdb_dbi_open(txn.txn, cname, flags, &self.dbi)
        if err:
            raise Exception("Error opening datsabase: %s"
                            % cmdb.mdb_strerror(err))

    def get_flags(self):
        return long(self.flags)

    def close(self):
        cmdb.mdb_dbi_close(self.env.env, self.dbi)

    def drop(self, Txn txn, bint delete = False):
        err = cmdb.mdb_drop(txn.txn, self.dbi, delete)
        if err:
            raise Exception("Error dropping datsabase: %s"
                            % cmdb.mdb_strerror(err))

    def stat(self, Txn txn):
        cdef cmdb.MDB_stat stat

        err = cmdb.mdb_stat(txn.txn, self.dbi, &stat)
        if err:
            raise Exception("Error stating database: %s"
                            % cmdb.mdb_strerror(err))
        return dict(ms_psize=stat.ms_psize,
                    ms_depth=stat.ms_depth,
                    ms_branch_pages=stat.ms_branch_pages,
                    ms_leaf_pages=stat.ms_leaf_pages,
                    ms_overflow_pages=stat.ms_overflow_pages,
                    ms_entries=stat.ms_entries)

    def contains(self, Txn txn, key):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef size_t key_len

        key_len = len(key) + 1
        api_key.mv_size = key_len
        api_key.mv_data = <char*>key

        err = cmdb.mdb_get(txn.txn, self.dbi, &api_key, &api_value)
        if err == 0:
            return True
        else:
            return False

    def get(self, Txn txn, key, default=None):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef size_t key_len

        key_len = len(key) + 1
        api_key.mv_size = key_len
        api_key.mv_data = <char*>key

        err = cmdb.mdb_get(txn.txn, self.dbi, &api_key, &api_value)
        if err == cmdb.MDB_NOTFOUND:
            return default
        elif err:
            raise Exception("Error getting data: %s" % cmdb.mdb_strerror(err))
        cdef char *rval = <char*>api_value.mv_data
        return rval[:api_value.mv_size-1]

    def mget(self, Txn txn, keys, default=None):
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *rval

        api_value.mv_size =  0
        api_value.mv_data =  NULL
        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                            % cmdb.mdb_strerror(err))
        try:
            for key in keys:
                api_key.mv_size = len(key) + 1
                api_key.mv_data = <char*>key
                if cmdb.MDB_SUCCESS == cmdb.mdb_cursor_get(cursor, &api_key,
                                                           &api_value, MDB_SET):
                    rval = <char*>api_value.mv_data
                    yield rval[:api_value.mv_size-1]
                else:
                    yield default
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_raw(self, Txn txn, key, default=None):
        """return the pointer and length of the data"""
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef size_t key_len

        key_len = len(key) + 1
        api_key.mv_size = key_len
        api_key.mv_data = <char*>key

        err = cmdb.mdb_get(txn.txn, self.dbi, &api_key, &api_value)
        if err == cmdb.MDB_NOTFOUND:
            return default
        elif err:
            raise Exception("Error getting data: %s" % cmdb.mdb_strerror(err))
        return <uint64_t>api_value.mv_data, api_value.mv_size

    def put(self, Txn txn, key, value, uint32_t flags=0):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef size_t key_len = len(key)
        cdef size_t value_len = len(value)

        api_key.mv_size = key_len + 1
        api_key.mv_data = <char*>key
        api_value.mv_size = value_len + 1
        api_value.mv_data = <char*>value

        err = cmdb.mdb_put(txn.txn, self.dbi, &api_key, &api_value, flags)
        if err == cmdb.MDB_MAP_FULL:
            raise MapFullError("Error putting data: %s"
                               % cmdb.mdb_strerror(err))
        elif err == cmdb.MDB_TXN_FULL:
            raise TxnFullError("Error putting data: %s"
                               % cmdb.mdb_strerror(err))
        elif err == cmdb.MDB_KEYEXIST:
            raise KeyExistError("Error putting data: %s"
                                % cmdb.mdb_strerror(err))
        elif err:
            raise Exception("Error putting data: %s" % cmdb.mdb_strerror(err))

    def put_raw(self, Txn txn, key, uint64_t val_ptr, uint64_t val_len, uint32_t flags=0):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef size_t key_len = len(key)
        cdef size_t value_len = val_len

        api_key.mv_size = key_len + 1
        api_key.mv_data = <char*>key
        api_value.mv_size = val_len
        api_value.mv_data = <char*>val_ptr

        err = cmdb.mdb_put(txn.txn, self.dbi, &api_key, &api_value, flags)
        if err == cmdb.MDB_MAP_FULL:
            raise MapFullError("Error putting data: %s"
                               % cmdb.mdb_strerror(err))
        elif err == cmdb.MDB_TXN_FULL:
            raise TxnFullError("Error putting data: %s"
                               % cmdb.mdb_strerror(err))
        elif err == cmdb.MDB_KEYEXIST:
            raise KeyExistError("Error putting data: %s"
                                % cmdb.mdb_strerror(err))
        elif err:
            raise Exception("Error putting data: %s" % cmdb.mdb_strerror(err))

    def delete(self, Txn txn, key, value=None):
        """Delete key/value from MDB

        If value is not specified, delete all values with this key. Otherwise,
        delete the specified key/value.
        """
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value

        api_key.mv_size = len(key) + 1
        api_key.mv_data = <char*>key
        if value:
            api_value.mv_size = len(value) + 1
            api_value.mv_data = <char*>value
            err = cmdb.mdb_del(txn.txn, self.dbi, &api_key, &api_value)
            if err:
                raise Exception("Error deleting data: %s"
                                % cmdb.mdb_strerror(err))
        else:
            err = cmdb.mdb_del(txn.txn, self.dbi, &api_key, NULL)
            if err:
                raise Exception("Error deleting data: %s"
                                % cmdb.mdb_strerror(err))

    def items(self, Txn txn):
        '''Return all the unique key values
        '''
        cdef Cursor cursor

        cursor = Cursor(txn, self)
        while True:
            key, value = cursor.get(op=MDB_NEXT_NODUP)
            if key is not None:
                yield key, value
            else:
                break
        cursor.close()

    def dup_items(self, Txn txn):
        '''Return all the key values
        '''
        cdef Cursor cursor

        try:
            cursor = Cursor(txn, self)
            while True:
                key, value = cursor.get(op=MDB_NEXT)
                if key is not None and value is not None:
                    yield key, value
                else:
                    break
        finally:
            cursor.close()

    def get_dup(self, Txn txn, key):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *value_

        api_key.mv_size = len(key) + 1
        api_key.mv_data = <char*>key

        cdef cmdb.MDB_cursor *cursor
        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating cursor: %s"
                            % cmdb.mdb_strerror(err))
        if not cmdb.mdb_cursor_get(cursor, &api_key, &api_value, MDB_SET):
            value_ = <char*>api_value.mv_data
            yield value_[:api_value.mv_size-1]
            if self.flags & MDB_DUPSORT:
                while True:
                    if not cmdb.mdb_cursor_get(cursor, &api_key,
                                               &api_value, MDB_NEXT_DUP):
                        value_ = <char*>api_value.mv_data
                        yield value_[:api_value.mv_size-1]
                    else:
                        break
        cmdb.mdb_cursor_close(cursor)

    def get_eq(self, Txn txn, key_):
        for value in self.get_dup(txn, key_):
            yield key_, value

    def get_ne(self, Txn txn, key_):
        for key, value in self.dup_items(txn):
            if key != key_:
                yield key, value

    def get_lt(self, Txn txn, key):
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *value_
        cdef char *key_

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = 0
        api_key.mv_data = NULL
        api_value.mv_size = 0
        api_value.mv_data = NULL

        try:
            while not cmdb.mdb_cursor_get(cursor, &api_key,
                                          &api_value, MDB_NEXT):
                value_ = <char*>api_value.mv_data
                key_ = <char*>api_key.mv_data
                k = key_[:api_key.mv_size-1]
                if k < key:
                    yield k, value_[:api_value.mv_size-1]
                else:
                    break
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_le(self, Txn txn, key_):
        for key, value in self.get_lt(txn, key_):
            yield key, value
        for key, value in self.get_eq(txn, key_):
            yield key, value


    def get_gt(self, Txn txn, key):
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *value_
        cdef char *key_

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = len(key) + 1
        api_key.mv_data = <char*>key
        api_value.mv_size = 0
        api_value.mv_data = NULL

        try:
            if not cmdb.mdb_cursor_get(cursor, &api_key,
                                       &api_value, MDB_SET_RANGE):
                key_ = <char*>api_key.mv_data
                k = key_[:api_key.mv_size-1]
                if k == key:
                    # if the key is present, skip all the possible dups
                    api_value.mv_size = 0
                    api_value.mv_data = NULL
                    cmdb.mdb_cursor_get(cursor, &api_key,
                                        &api_value, MDB_LAST_DUP)
                else:
                    value_ = <char*>api_value.mv_data
                    key_ = <char*>api_key.mv_data
                    yield key_[:api_key.mv_size-1], value_[:api_value.mv_size-1]

                while not cmdb.mdb_cursor_get(cursor, &api_key,
                                              &api_value, MDB_NEXT):
                    value_ = <char*>api_value.mv_data
                    key_ = <char*>api_key.mv_data
                    yield key_[:api_key.mv_size-1], value_[:api_value.mv_size-1]
            else:
                yield None, None
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_ge(self, Txn txn, key_):
        for key, value in self.get_eq(txn, key_):
            yield key, value
        for key, value in self.get_gt(txn, key_):
            yield key, value

    def get_range(self, Txn txn, key_begin, key_end):
        if key_begin > key_end:
            raise KeyError("Keys are out of order")
        for key, value in self.get_ge(txn, key_begin):
            if key <= key_end:
                yield key, value
            else:
                break


cdef class StrIntDB(DB):
    cdef size_t valuesize
    cdef cast_fn caster
    cdef bint signed
    def __init__(self, Env env, Txn txn, name=None,
                 uint32_t flags=MDB_DUPSORT | MDB_CREATE,
                 uint32_t inttype=MDB_INT_32):
        flags |= (MDB_DUPSORT | MDB_INTEGERDUP | MDB_DUPFIXED)
        if inttype < MDB_INT_32 or inttype > MDB_UINT_64:
            raise ValueError("Invalid integer type.")
        self.valuesize = _size_int(inttype)
        self.caster = _setup_caster(inttype)
        self.signed = True if inttype % 2 else False
        super(StrIntDB, self).__init__(env, txn, name, flags)
        assert not set_value_cmp(txn.txn, self.dbi, inttype)

    def get(self, Txn txn, key, default=None):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef size_t key_len

        key_len = len(key) + 1
        api_key.mv_size = key_len
        api_key.mv_data = <char*>key

        err = cmdb.mdb_get(txn.txn, self.dbi, &api_key, &api_value)
        if err == cmdb.MDB_NOTFOUND:
            return default
        elif err:
            raise Exception("Error getting data: %s" % cmdb.mdb_strerror(err))
        return self.caster(api_value.mv_data)

    def mget(self, Txn txn, keys, default=None):
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value

        api_value.mv_size =  0
        api_value.mv_data =  NULL
        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                            % cmdb.mdb_strerror(err))
        try:
            for key in keys:
                api_key.mv_size = len(key) + 1
                api_key.mv_data = <char*>key
                if cmdb.MDB_SUCCESS == cmdb.mdb_cursor_get(cursor, &api_key,
                                                           &api_value, MDB_SET):
                    yield self.caster(api_value.mv_data)
                else:
                    yield default
        finally:
            cmdb.mdb_cursor_close(cursor)

    def put(self, Txn txn, key, value, uint32_t flags=0):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef size_t key_len = len(key)
        cdef void *data
        cdef int64_t svalue
        cdef uint64_t uvalue
        if self.signed:
            svalue = value
            data = &svalue
        else:
            uvalue = value
            data = &uvalue

        api_key.mv_size = key_len + 1
        api_key.mv_data = <char*>key
        api_value.mv_size = self.valuesize
        api_value.mv_data = data

        err = cmdb.mdb_put(txn.txn, self.dbi, &api_key, &api_value, flags)
        if err == cmdb.MDB_MAP_FULL:
            raise MapFullError("Error putting data: %s"
                               % cmdb.mdb_strerror(err))
        elif err == cmdb.MDB_TXN_FULL:
            raise TxnFullError("Error putting data: %s"
                               % cmdb.mdb_strerror(err))
        elif err == cmdb.MDB_KEYEXIST:
            raise KeyExistError("Error putting data: %s"
                                % cmdb.mdb_strerror(err))
        elif err:
            raise Exception("Error putting data: %s" % cmdb.mdb_strerror(err))

    def delete(self, Txn txn, key, value=None):
        """Delete key/value from MDB

        If value is not specified, delete all values with this key. Otherwise,
        delete the specified key/value.
        """
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef void *data
        cdef int64_t svalue
        cdef uint64_t uvalue

        api_key.mv_size = len(key) + 1
        api_key.mv_data = <char*>key
        if value:
            if self.signed:
                svalue = value
                data = &svalue
            else:
                uvalue = value
                data = &uvalue
            api_value.mv_size = self.valuesize
            api_value.mv_data = data
            err = cmdb.mdb_del(txn.txn, self.dbi, &api_key, &api_value)
            if err:
                raise Exception("Error deleting data: %s"
                                % cmdb.mdb_strerror(err))
        else:
            err = cmdb.mdb_del(txn.txn, self.dbi, &api_key, NULL)
            if err:
                raise Exception("Error deleting data: %s"
                                % cmdb.mdb_strerror(err))

    def items(self, Txn txn):
        '''Return all the unique key values
        '''
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *key_

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = 0
        api_key.mv_data = NULL
        api_value.mv_size = 0
        api_value.mv_data =  NULL

        try:
            while not cmdb.mdb_cursor_get(cursor, &api_key,
                                          &api_value, MDB_NEXT_NODUP):
                key_ = <char *>api_key.mv_data
                value_ = self.caster(api_value.mv_data)
                yield key_[:api_key.mv_size-1], value_
        finally:
            cmdb.mdb_cursor_close(cursor)

    def dup_items(self, Txn txn):
        '''Return all the key values
        '''
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *key_

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = 0
        api_key.mv_data = NULL
        api_value.mv_size =  0
        api_value.mv_data =  NULL

        try:
            while not cmdb.mdb_cursor_get(cursor, &api_key,
                                          &api_value, MDB_NEXT):
                key_ = <char *>api_key.mv_data
                value_ = self.caster(api_value.mv_data)
                yield key_[:api_key.mv_size-1], value_
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_dup(self, Txn txn, key):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value

        api_key.mv_size = len(key) + 1
        api_key.mv_data = <char *>key

        cdef cmdb.MDB_cursor *cursor
        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating cursor: %s"
                            % cmdb.mdb_strerror(err))
        if not cmdb.mdb_cursor_get(cursor, &api_key, &api_value, MDB_SET):
            yield self.caster(api_value.mv_data)
            if self.flags & MDB_DUPSORT:
                while True:
                    if not cmdb.mdb_cursor_get(cursor, &api_key,
                                               &api_value, MDB_NEXT_DUP):
                        yield self.caster(api_value.mv_data)
                    else:
                        break
        cmdb.mdb_cursor_close(cursor)

    def get_lt(self, Txn txn, key):
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *key_

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = 0
        api_key.mv_data = NULL
        api_value.mv_size = 0
        api_value.mv_data = NULL

        try:
            while not cmdb.mdb_cursor_get(cursor, &api_key,
                                          &api_value, MDB_NEXT):
                key_ = <char*>api_key.mv_data
                k = key_[:api_key.mv_size-1]
                if k < key:
                    yield k, self.caster(api_value.mv_data)
                else:
                    break
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_le(self, Txn txn, key_):
        for key, value in self.get_lt(txn, key_):
            yield key, value
        for key, value in self.get_eq(txn, key_):
            yield key, value


    def get_gt(self, Txn txn, key):
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *key_

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = len(key) + 1
        api_key.mv_data = <char*>key
        api_value.mv_size = 0
        api_value.mv_data = NULL

        try:
            if not cmdb.mdb_cursor_get(cursor, &api_key,
                                       &api_value, MDB_SET_RANGE):
                key_ = <char*>api_key.mv_data
                k = key_[:api_key.mv_size-1]
                if k == key:
                    # if the key is present, skip all the possible dups
                    api_value.mv_size = 0
                    api_value.mv_data = NULL
                    cmdb.mdb_cursor_get(cursor, &api_key,
                                        &api_value, MDB_LAST_DUP)
                else:
                    key_ = <char*>api_key.mv_data
                    yield key_[:api_key.mv_size-1], self.caster(api_value.mv_data)
                while not cmdb.mdb_cursor_get(cursor, &api_key,
                                              &api_value, MDB_NEXT):
                    key_ = <char*>api_key.mv_data
                    yield key_[:api_key.mv_size-1], self.caster(api_value.mv_data)
            else:
                yield None, None
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_ge(self, Txn txn, key_):
        for key, value in self.get_eq(txn, key_):
            yield key, value
        for key, value in self.get_gt(txn, key_):
            yield key, value

    def get_range(self, Txn txn, key_begin, key_end):
        if key_begin > key_end:
            raise KeyError("Keys are out of order")
        for key, value in self.get_ge(txn, key_begin):
            if key <= key_end:
                yield key, value
            else:
                break


cdef class IntStrDB(DB):
    cdef size_t keysize
    cdef cast_fn caster
    cdef bint signed
    def __init__(self, Env env, Txn txn, name=None,
                 uint32_t flags=MDB_DUPSORT | MDB_CREATE,
                 uint32_t inttype=MDB_INT_32):
        flags |= MDB_INTEGERKEY
        if inttype < MDB_INT_32 or inttype > MDB_UINT_64:
            raise ValueError("Invalid integer type.")
        self.keysize = _size_int(inttype)
        self.caster = _setup_caster(inttype)
        self.signed = True if inttype % 2 else False
        super(IntStrDB, self).__init__(env, txn, name, flags)
        assert not set_key_cmp(txn.txn, self.dbi, inttype)

    def contains(self, Txn txn, key):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey
        if self.signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        api_key.mv_size = self.keysize
        api_key.mv_data = data
        api_value.mv_size =  0
        api_value.mv_data =  NULL

        err = cmdb.mdb_get(txn.txn, self.dbi, &api_key, &api_value)
        if err == 0:
            return True
        else:
            return False

    def get(self, Txn txn, key, default=None):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey
        if self.signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        api_key.mv_size = self.keysize
        api_key.mv_data = <void *>data
        api_value.mv_size =  0
        api_value.mv_data =  NULL

        err = cmdb.mdb_get(txn.txn, self.dbi, &api_key, &api_value)
        if err == cmdb.MDB_NOTFOUND:
            return default
        elif err:
            raise Exception("Error getting data: %s" % cmdb.mdb_strerror(err))
        cdef char *rval = <char*>api_value.mv_data
        return rval[:api_value.mv_size-1]

    def mget(self, Txn txn, keys, default=None):
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *value_
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey

        api_key.mv_size = self.keysize
        api_value.mv_size =  0
        api_value.mv_data =  NULL
        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                            % cmdb.mdb_strerror(err))
        try:
            for key in keys:
                if self.signed:
                    skey = key
                    data = &skey
                else:
                    ukey = key
                    data = &ukey
                api_key.mv_data = <void *>data
                if cmdb.MDB_SUCCESS == cmdb.mdb_cursor_get(cursor, &api_key,
                                                           &api_value, MDB_SET):
                    value_ = <char*>api_value.mv_data
                    yield value_[:api_value.mv_size-1]
                else:
                    yield default
        finally:
            cmdb.mdb_cursor_close(cursor)

    def put(self, Txn txn, key, value, uint32_t flags=0):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef size_t value_len = len(value)
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey
        if self.signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        api_key.mv_size = self.keysize
        api_key.mv_data = <void *>data
        api_value.mv_size = value_len + 1
        api_value.mv_data = <char *>value

        err = cmdb.mdb_put(txn.txn, self.dbi, &api_key, &api_value, flags)
        if err == cmdb.MDB_MAP_FULL:
            raise MapFullError("Error putting data: %s"
                               % cmdb.mdb_strerror(err))
        elif err == cmdb.MDB_TXN_FULL:
            raise TxnFullError("Error putting data: %s"
                               % cmdb.mdb_strerror(err))
        elif err == cmdb.MDB_KEYEXIST:
            raise KeyExistError("Error putting data: %s"
                                % cmdb.mdb_strerror(err))
        elif err:
            raise Exception("Error putting data: %s" % cmdb.mdb_strerror(err))


    def delete(self, Txn txn, key, value=None):
        """Delete key/value from MDB

        If value is not specified, delete all values with this key. Otherwise,
        delete the specified key/value.
        """
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey
        if self.signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        api_key.mv_size = self.keysize
        api_key.mv_data = data
        api_value.mv_size =  0
        api_value.mv_data =  NULL
        if value:
            api_value.mv_size = len(value) + 1
            api_value.mv_data = <char*>value
            err = cmdb.mdb_del(txn.txn, self.dbi, &api_key, &api_value)
            if err:
                raise Exception("Error deleting data: %s"
                                % cmdb.mdb_strerror(err))
        else:
            err = cmdb.mdb_del(txn.txn, self.dbi, &api_key, NULL)
            if err:
                raise Exception("Error deleting data: %s"
                                % cmdb.mdb_strerror(err))

    def items(self, Txn txn):
        '''Return all the unique key values
        '''
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *value_

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = 0
        api_key.mv_data = NULL
        api_value.mv_size = 0
        api_value.mv_data = NULL

        try:
            while not cmdb.mdb_cursor_get(cursor, &api_key,
                                          &api_value, MDB_NEXT_NODUP):
                value_ = <char*>api_value.mv_data
                key_ = self.caster(api_key.mv_data)
                yield key_, value_[:api_value.mv_size-1]
        finally:
            cmdb.mdb_cursor_close(cursor)

    def dup_items(self, Txn txn):
        '''Return all the key values
        '''
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *value_

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = 0
        api_key.mv_data = NULL
        api_value.mv_size = 0
        api_value.mv_data = NULL

        try:
            while not cmdb.mdb_cursor_get(cursor, &api_key,
                                          &api_value, MDB_NEXT):
                value_ = <char*>api_value.mv_data
                key_ = self.caster(api_key.mv_data)
                yield key_, value_[:api_value.mv_size-1]
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_dup(self, Txn txn, key):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *value_
        cdef cmdb.MDB_cursor *cursor
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey
        if self.signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = self.keysize
        api_key.mv_data = data
        api_value.mv_size =  0
        api_value.mv_data =  NULL

        try:
            if not cmdb.mdb_cursor_get(cursor, &api_key, &api_value, MDB_SET):
                value_ = <char*>api_value.mv_data
                yield value_[:api_value.mv_size-1]
                if self.flags & MDB_DUPSORT:
                    while True:
                        if not cmdb.mdb_cursor_get(cursor, &api_key,
                                                   &api_value, MDB_NEXT_DUP):
                            value_ = <char*>api_value.mv_data
                            yield value_[:api_value.mv_size-1]
                        else:
                            break
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_lt(self, Txn txn, key):
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *value_
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey
        if self.signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = self.keysize
        api_key.mv_data = data
        api_value.mv_size = 0
        api_value.mv_data = NULL

        try:
            while not cmdb.mdb_cursor_get(cursor, &api_key,
                                          &api_value, MDB_NEXT):
                value_ = <char*>api_value.mv_data
                key_ = self.caster(api_key.mv_data)
                if key_ < key:
                    yield key_, value_[:api_value.mv_size-1]
                else:
                    break
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_le(self, Txn txn, int64_t key_):
        for key, value in self.get_lt(txn, key_):
            yield key, value
        for key, value in self.get_eq(txn, key_):
            yield key, value


    def get_gt(self, Txn txn, key):
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *value_
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey
        if self.signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = self.keysize
        api_key.mv_data = data
        api_value.mv_size = 0
        api_value.mv_data = NULL

        try:
            if not cmdb.mdb_cursor_get(cursor, &api_key,
                                    &api_value, MDB_SET_RANGE):
                key_ = self.caster(api_key.mv_data)
                if key == key_:
                    # if the key is present, skip all the possible dups
                    api_value.mv_size = 0
                    api_value.mv_data = NULL
                    cmdb.mdb_cursor_get(cursor, &api_key,
                                        &api_value, MDB_LAST_DUP)
                else:
                    value_ = <char*>api_value.mv_data
                    key_ = self.caster(api_key.mv_data)
                    yield key_, value_[:api_value.mv_size-1]

                while not cmdb.mdb_cursor_get(cursor, &api_key,
                                                &api_value, MDB_NEXT):
                    value_ = <char*>api_value.mv_data
                    key_ = self.caster(api_key.mv_data)
                    yield key_, value_[:api_value.mv_size-1]
            else:
                yield None, None
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_ge(self, Txn txn, int64_t key_):
        for key, value in self.get_eq(txn, key_):
            yield key, value
        for key, value in self.get_gt(txn, key_):
            yield key, value

    def get_range(self, Txn txn, int64_t key_begin, int64_t key_end):
        if key_begin > key_end:
            raise KeyError("Keys are out of order")
        for key, value in self.get_ge(txn, key_begin):
            if key <= key_end:
                yield key, value
            else:
                break


cdef class IntIntDB(DB):
    cdef size_t keysize, valuesize
    cdef cast_fn key_caster, value_caster
    cdef bint key_signed, value_signed
    def __init__(self, Env env, Txn txn, name=None,
                 uint32_t flags=MDB_DUPSORT | MDB_CREATE,
                 uint32_t key_inttype=MDB_INT_32,
                 uint32_t value_inttype=MDB_INT_32):
        flags |= (MDB_DUPSORT | MDB_INTEGERKEY | MDB_INTEGERDUP | MDB_DUPFIXED)
        if key_inttype < MDB_INT_32 or key_inttype > MDB_UINT_64:
            raise ValueError("Invalid integer type.")
        if value_inttype < MDB_INT_32 or value_inttype > MDB_UINT_64:
            raise ValueError("Invalid integer type.")
        self.keysize = _size_int(key_inttype)
        self.key_caster = _setup_caster(key_inttype)
        self.key_signed = True if key_inttype % 2 else False
        self.valuesize = _size_int(value_inttype)
        self.value_caster = _setup_caster(value_inttype)
        self.value_signed = True if value_inttype % 2 else False
        super(IntIntDB, self).__init__(env, txn, name, flags)
        assert not set_key_cmp(txn.txn, self.dbi, key_inttype)
        assert not set_value_cmp(txn.txn, self.dbi, value_inttype)

    def contains(self, Txn txn, key):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey
        if self.key_signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        api_key.mv_size = self.keysize
        api_key.mv_data = data

        err = cmdb.mdb_get(txn.txn, self.dbi, &api_key, &api_value)
        if err == 0:
            return True
        else:
            return False

    def get(self, Txn txn, key, default=None):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey
        if self.key_signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        api_key.mv_size = self.keysize
        api_key.mv_data = data

        err = cmdb.mdb_get(txn.txn, self.dbi, &api_key, &api_value)
        if err == cmdb.MDB_NOTFOUND:
            return default
        elif err:
            raise Exception("Error getting data: %s" % cmdb.mdb_strerror(err))
        return self.value_caster(api_value.mv_data)

    def mget(self, Txn txn, keys, default=None):
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey

        api_key.mv_size = self.keysize
        api_value.mv_size =  0
        api_value.mv_data =  NULL
        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                            % cmdb.mdb_strerror(err))
        try:
            for key in keys:
                if self.key_signed:
                    skey = key
                    data = &skey
                else:
                    ukey = key
                    data = &ukey
                api_key.mv_data = <void *>data
                if cmdb.MDB_SUCCESS == cmdb.mdb_cursor_get(cursor, &api_key,
                                                           &api_value, MDB_SET):
                    yield self.value_caster(api_value.mv_data)
                else:
                    yield default
        finally:
            cmdb.mdb_cursor_close(cursor)

    def put(self, Txn txn, key, value, uint32_t flags=0):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef void *data, *val
        cdef int64_t skey, svalue
        cdef uint64_t ukey, uvalue
        if self.key_signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey
        if self.value_signed:
            svalue = value
            val = &svalue
        else:
            uvalue = value
            val = &uvalue

        api_key.mv_size = self.keysize
        api_key.mv_data = data
        api_value.mv_size = self.valuesize
        api_value.mv_data = val

        err = cmdb.mdb_put(txn.txn, self.dbi, &api_key, &api_value, flags)
        if err == cmdb.MDB_MAP_FULL:
            raise MapFullError("Error putting data: %s"
                               % cmdb.mdb_strerror(err))
        elif err == cmdb.MDB_TXN_FULL:
            raise TxnFullError("Error putting data: %s"
                               % cmdb.mdb_strerror(err))
        elif err == cmdb.MDB_KEYEXIST:
            raise KeyExistError("Error putting data: %s"
                                % cmdb.mdb_strerror(err))
        elif err:
            raise Exception("Error putting data: %s" % cmdb.mdb_strerror(err))


    def delete(self, Txn txn, key, value=None):
        """Delete key/value from MDB

        If value is not specified, delete all values with this key. Otherwise,
        delete the specified key/value.
        """
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef void *data, *val
        cdef int64_t skey, svalue
        cdef uint64_t ukey, uvalue
        if self.key_signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        api_key.mv_size = self.keysize
        api_key.mv_data = data
        if value:
            if self.value_signed:
                svalue = value
                val = &svalue
            else:
                uvalue = value
                val = &uvalue
            api_value.mv_size = self.valuesize
            api_value.mv_data = val
            err = cmdb.mdb_del(txn.txn, self.dbi, &api_key, &api_value)
            if err:
                raise Exception("Error deleting data: %s"
                                % cmdb.mdb_strerror(err))
        else:
            err = cmdb.mdb_del(txn.txn, self.dbi, &api_key, NULL)
            if err:
                raise Exception("Error deleting data: %s"
                                % cmdb.mdb_strerror(err))

    def items(self, Txn txn):
        '''Return all the unique key values
        '''
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = 0
        api_key.mv_data = NULL
        api_value.mv_size = 0
        api_value.mv_data = NULL

        try:
            while not cmdb.mdb_cursor_get(cursor, &api_key,
                                          &api_value, MDB_NEXT_NODUP):
                yield self.key_caster(api_key.mv_data), self.value_caster(api_value.mv_data)
        finally:
            cmdb.mdb_cursor_close(cursor)

    def dup_items(self, Txn txn):
        '''Return all the key values
        '''
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = 0
        api_key.mv_data = NULL
        api_value.mv_size = 0
        api_value.mv_data = NULL

        try:
            while not cmdb.mdb_cursor_get(cursor, &api_key,
                                          &api_value, MDB_NEXT):
                yield self.key_caster(api_key.mv_data), self.value_caster(api_value.mv_data)
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_dup(self, Txn txn, key):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef cmdb.MDB_cursor *cursor
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey
        if self.key_signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = self.keysize
        api_key.mv_data = data

        try:
            if not cmdb.mdb_cursor_get(cursor, &api_key, &api_value, MDB_SET):
                yield self.value_caster(api_value.mv_data)
                if self.flags & MDB_DUPSORT:
                    while True:
                        if not cmdb.mdb_cursor_get(cursor, &api_key,
                                                   &api_value, MDB_NEXT_DUP):
                            yield self.value_caster(api_value.mv_data)
                        else:
                            break
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_lt(self, Txn txn, key):
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey
        if self.key_signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = 0
        api_key.mv_data = NULL
        api_value.mv_size = 0
        api_value.mv_data = NULL

        try:
            while not cmdb.mdb_cursor_get(cursor, &api_key,
                                          &api_value, MDB_NEXT):
                key_ = self.key_caster(api_key.mv_data)
                if key_ < key:
                    yield key_, self.value_caster(api_value.mv_data)
                else:
                    break
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_le(self, Txn txn, key_):
        for key, value in self.get_lt(txn, key_):
            yield key, value
        for key, value in self.get_eq(txn, key_):
            yield key, value


    def get_gt(self, Txn txn, key):
        cdef cmdb.MDB_cursor *cursor
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef void *data
        cdef int64_t skey
        cdef uint64_t ukey
        if self.key_signed:
            skey = key
            data = &skey
        else:
            ukey = key
            data = &ukey

        err = cmdb.mdb_cursor_open(txn.txn, self.dbi, &cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                    % cmdb.mdb_strerror(err))
        api_key.mv_size = self.keysize
        api_key.mv_data = data
        api_value.mv_size = 0
        api_value.mv_data = NULL

        try:
            if not cmdb.mdb_cursor_get(cursor, &api_key,
                                    &api_value, MDB_SET_RANGE):
                key_ = self.key_caster(api_key.mv_data)
                if key == key_:
                    # if the key is present, skip all the possible dups
                    api_value.mv_size = 0
                    api_value.mv_data = NULL
                    cmdb.mdb_cursor_get(cursor, &api_key,
                                        &api_value, MDB_LAST_DUP)
                else:
                    yield self.key_caster(api_key.mv_data), self.value_caster(api_value.mv_data)
                while not cmdb.mdb_cursor_get(cursor, &api_key,
                                                &api_value, MDB_NEXT):
                    yield self.key_caster(api_key.mv_data), self.value_caster(api_value.mv_data)
            else:
                yield None, None
        finally:
            cmdb.mdb_cursor_close(cursor)

    def get_ge(self, Txn txn, key_):
        for key, value in self.get_eq(txn, key_):
            yield key, value
        for key, value in self.get_gt(txn, key_):
            yield key, value

    def get_range(self, Txn txn, key_begin, key_end):
        if key_begin > key_end:
            raise KeyError("Keys are out of order")
        for key, value in self.get_ge(txn, key_begin):
            if key <= key_end:
                yield key, value
            else:
                break


cdef class Cursor:
    cdef cmdb.MDB_cursor *cursor

    def __init__(self, Txn txn, DB dbi):
        err = cmdb.mdb_cursor_open(txn.txn, dbi.dbi, &self.cursor)
        if err:
            raise Exception("Error creating Cursor: %s"
                            % cmdb.mdb_strerror(err))

    def close(self):
        cmdb.mdb_cursor_close(self.cursor)

    def renew(self, Txn txn):
        err = cmdb.mdb_cursor_renew(txn.txn, self.cursor)
        if err:
            raise Exception("Error renewing Cursor: %s"
                            % cmdb.mdb_strerror(err))

    def get(self, key=None, value=None, uint32_t op=MDB_NEXT):
        """Move the cursor to specified key value.

        If key is None, then move cursor to the first element of current DB.
        Otherwise, flip op from MDB_NEXT to MDB_SET automatically to avoid error.
        """
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value
        cdef char *key_, *value_

        if key:
            api_key.mv_size = len(key) + 1
            api_key.mv_data = <char*>key
            op = MDB_SET if op == MDB_NEXT else op
        else:
            api_key.mv_size = 0
            api_key.mv_data = NULL
        if value:
            api_value.mv_size = len(value) + 1
            api_value.mv_data = <char*>value
        else:
            api_value.mv_size = 0
            api_value.mv_data = NULL

        if not cmdb.mdb_cursor_get(self.cursor, &api_key, &api_value, op):
            value_ = <char*>api_value.mv_data
            if key is not None:
                return key, value_[:api_value.mv_size-1]
            else:
                key_ = <char*>api_key.mv_data
                return key_[:api_key.mv_size-1], value_[:api_value.mv_size-1]
        else:
            return None, None

    def put(self, key, value, uint32_t flags=0):
        cdef cmdb.MDB_val api_key
        cdef cmdb.MDB_val api_value

        api_key.mv_size = len(key) + 1
        api_key.mv_data = <char*>key
        api_value.mv_size = len(value) + 1
        api_value.mv_data = <char*>value

        err = cmdb.mdb_cursor_put(self.cursor, &api_key, &api_value, flags)
        if err == cmdb.MDB_MAP_FULL:
            raise MapFullError("Error putting data: %s"
                               % cmdb.mdb_strerror(err))
        elif err == cmdb.MDB_TXN_FULL:
            raise TxnFullError("Error putting data: %s"
                               % cmdb.mdb_strerror(err))
        elif err:
            raise Exception("Error putting data: %s"
                            % cmdb.mdb_strerror(err))


    def delete(self, uint32_t flags=0):
        err = cmdb.mdb_cursor_del(self.cursor, flags)
        if err:
            raise Exception("Error deleting Cursor: %s"
                            % cmdb.mdb_strerror(err))

    def count_dups(self):
        cdef size_t rval = 0

        err = cmdb.mdb_cursor_count(self.cursor, &rval)
        if err:
            raise Exception("Error counting Cursor: %s"
                            % cmdb.mdb_strerror(err))
        return rval


DEFAULT_DB_NAME = '_default'
IDENTITY_FN = lambda val: val


def mdb_write_handle(path,                          # the path of mdb
                     size,                          # the size of mdb in byte
                     db_name=DEFAULT_DB_NAME,       # the name of database
                     dup=True,                      # duplicate values
                     key_type=MDB_STR,              # key type [MDB_STR|MDB_INT_*]
                     value_type=MDB_STR,            # value type [MDB_STR|MDB_INT_*]
                     open_flags=0                   # additional flags for open
                     ):
    env = Env(path, flags=MDB_NOSYNC | MDB_WRITEMAP | open_flags, mapsize=size)
    txn = env.begin_txn()
    flags = MDB_CREATE
    flags |= MDB_DUPSORT if dup else 0
    flags |= MDB_INTEGERKEY if key_type else 0
    flags |= MDB_INTEGERDUP if value_type else 0
    db = env.open_db(txn, name=db_name, flags=flags,
            key_inttype=key_type, value_inttype=value_type)

    return env, txn, db


def mdb_read_handle(path,                           # the path of mdb
                    db_name=DEFAULT_DB_NAME,        # the name of database
                    dup=True,                       # duplicate values
                    key_type=MDB_STR,              # key type [MDB_STR|MDB_INT_*]
                    value_type=MDB_STR,            # value type [MDB_STR|MDB_INT_*]
                    open_flags=0                    # additional flags for open
                    ):
    env = Env(path, flags=MDB_RDONLY|open_flags)
    txn = env.begin_txn(flags=MDB_RDONLY)
    flags = 0
    flags |= MDB_DUPSORT if dup else 0
    flags |= MDB_INTEGERKEY if key_type else 0
    flags |= MDB_INTEGERDUP if value_type else 0
    db = env.open_db(txn, name=db_name, flags=flags,
            key_inttype=key_type, value_inttype=value_type)

    return env, txn, db


class DupReader(object):
    '''Class to read duplicate mdb database. Note txn in __init__
    aborts immediately to avoid the long-lived read txn. The mdb would
    be closed automatically once it's no longer existed.

    Note that do NOT use txn.abort here, since it will close db handle
    automatically. For readonly txn, call commit is exactly the same as abort,
    except that it remains database handles open. Reader Will close that
    mannually.
    '''
    def __init__(self, path, db_name=DEFAULT_DB_NAME,
                 int_key=MDB_STR, int_val=MDB_STR, decode_fn=None):
        self.path = path
        self.db_name = db_name
        self.env = Env(path, flags=MDB_RDONLY|MDB_NOTLS)
        txn = self.env.begin_txn(flags=MDB_RDONLY)
        flags = MDB_DUPSORT
        flags |= MDB_INTEGERKEY if int_key else 0
        flags |= MDB_INTEGERDUP if int_val else 0
        self.db = self.env.open_db(txn, name=db_name, flags=flags)
        self.decode_fn = decode_fn or IDENTITY_FN
        txn.commit()

    def get(self, key):
        try:
            txn = self.env.begin_txn(flags=MDB_RDONLY)
        except Exception as e:
            print "MDB Error: %s" % e
            yield None
        else:
            values = self.db.get_dup(txn, key)
            try:
                for value in values:
                    yield self.decode_fn(value)
            finally:
                txn.commit()

    def contains(self, key):
        try:
            txn = self.env.begin_txn(flags=MDB_RDONLY)
        except Exception as e:
            print "MDB Error: %s" % e
            return False
        else:
            ret = self.db.contains(txn, key)
            txn.commit()
            return ret

    def get_first(self, key, default=None):
        try:
            txn = self.env.begin_txn(flags=MDB_RDONLY)
        except Exception as e:
            print "MDB Error: %s" % e
            return default
        else:
            value = self.db.get(txn, key)
            txn.commit()
            return self.decode_fn(value) if value is not default else value

    def iteritems(self):
        try:
            txn = self.env.begin_txn(flags=MDB_RDONLY)
        except Exception as e:
            print "MDB Error: %s" % e
            yield None, None
        else:
            try:
                for key, value in self.db.dup_items(txn):
                    yield key, self.decode_fn(value)
            finally:
                txn.commit()

    def close(self):
        self.db.close()
        self.env.close()
        self.db = None
        self.env = None

    def __len__(self):
        txn = self.env.begin_txn(flags=MDB_RDONLY)
        nlen = self.db.stat(txn).get('ms_entries', 0)
        txn.commit()
        return nlen

    def __del__(self):
        if self.db is not None:
            self.db.close()
        if self.env is not None:
            self.env.close()


class Reader(object):
    def __init__(self, path, db_name=DEFAULT_DB_NAME,
                 int_key=MDB_STR, int_val=MDB_STR, decode_fn=None):
        self.path = path
        self.db_name = db_name
        self.env = Env(path, flags=MDB_RDONLY|MDB_NOTLS)
        txn = self.env.begin_txn(flags=MDB_RDONLY)
        flags= 0
        flags |= MDB_INTEGERKEY if int_key else 0
        flags |= MDB_INTEGERDUP if int_val else 0
        self.db = self.env.open_db(txn, name=db_name, flags=flags)
        self.decode_fn = decode_fn or IDENTITY_FN
        txn.commit()

    def contains(self, key):
        try:
            txn = self.env.begin_txn(flags=MDB_RDONLY)
        except Exception as e:
            print "MDB Error: %s" % e
            return False
        else:
            ret = self.db.contains(txn, key)
            txn.commit()
            return ret

    def get(self, key, default=None):
        try:
            txn = self.env.begin_txn(flags=MDB_RDONLY)
        except Exception as e:
            print "MDB Error: %s" % e
            return default
        else:
            value = self.db.get(txn, key, default)
            txn.commit()
            return self.decode_fn(value) if value is not default else value

    def iteritems(self):
        try:
            txn = self.env.begin_txn(flags=MDB_RDONLY)
        except Exception as e:
            print "MDB Error: %s" % e
            yield None, None
        else:
            try:
                for key, value in self.db.items(txn):
                    yield key, self.decode_fn(value)
            finally:
                txn.commit()

    def close(self):
        self.db.close()
        self.env.close()
        self.db = None
        self.env = None

    def __len__(self):
        txn = self.env.begin_txn(flags=MDB_RDONLY)
        nlen = self.db.stat(txn).get('ms_entries', 0)
        txn.commit()
        return nlen

    def __del__(self):
        if self.db is not None:
            self.db.close()
        if self.env is not None:
            self.env.close()


class Writer(object):
    def __init__(self, path, mapsize=10*MB, db_name=DEFAULT_DB_NAME,
                 dup=False, int_key=MDB_STR, int_val=MDB_STR,
                 encode_fn=None, drop_on_mput=False):
        # Check directory exists
        self.db_name = db_name
        self._check_mdb_dir(path)
        self.env = Env(path, flags=MDB_NOSYNC | MDB_WRITEMAP, mapsize=mapsize)
        txn = self.env.begin_txn()
        flags = MDB_CREATE
        flags |= MDB_DUPSORT if dup else 0
        flags |= MDB_INTEGERKEY if int_key else 0
        flags |= MDB_INTEGERDUP if int_val else 0
        self.flags = flags
        self.db = self.env.open_db(txn, name=db_name, flags=flags)
        self.encode_fn = encode_fn or IDENTITY_FN
        self.drop_on_mput = drop_on_mput
        txn.commit()

    def _check_mdb_dir(self, path):
        import os
        import errno
        try:
            os.makedirs(path)
        except OSError as e:
            if e.errno == errno.EEXIST and os.path.isdir(path):
                pass
            else:
                raise

    def put(self, key, value):
        txn = self.env.begin_txn()
        try:
            self.db.put(txn, key, self.encode_fn(value))
        except Exception as e:
            txn.abort()
            print e
        else:
            txn.commit()

    def mput(self, data):
        if hasattr(data, "iteritems"):
            data = data.iteritems()

        if self.drop_on_mput:
            self.drop()

        txn = self.env.begin_txn()
        try:
            ncount = 0
            for key, value in data:
                self.db.put(txn, key, self.encode_fn(value))
                ncount += 1
                if ncount > MDB_COMMIT_THRESHOLD:
                    txn.commit()
                    txn = self.env.begin_txn()
                    ncount = 0
        except Exception as e:
            print e
        else:
            txn.commit()

    def drop(self):
        txn = self.env.begin_txn()
        self.db.drop(txn)
        txn.commit()

    def close(self):
        self.db.close()
        self.env.close()
        self.db = None
        self.env = None

    def __del__(self):
        if self.db is not None:
            self.db.close()
        if self.env is not None:
            self.env.close()
