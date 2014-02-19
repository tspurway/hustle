cdef extern from 'lmdb.h':
    cdef enum:
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

        # MDB Return Values
        MDB_SUCCESS	= 0
        MDB_KEYEXIST = -30799
        MDB_NOTFOUND = -30798
        MDB_PAGE_NOTFOUND = -30797
        MDB_CORRUPTED = -30796
        MDB_PANIC = -30795
        MDB_VERSION_MISMATCH = -30794
        MDB_INVALID = -30793
        MDB_MAP_FULL = -30792
        MDB_DBS_FULL = -30791
        MDB_READERS_FULL = -30790
        MDB_TLS_FULL = -30789
        MDB_TXN_FULL = -30788
        MDB_CURSOR_FULL = -30787
        MDB_PAGE_FULL = -30786
        MDB_MAP_RESIZED = -30785
        MDB_INCOMPATIBLE = -30784
        MDB_BAD_RSLOT = -30783
        MDB_BAD_TXN = -30782
        MDB_BAD_VALSIZE = -30781
        MDB_LAST_ERRCODE = MDB_BAD_VALSIZE

    cdef enum Cursor_Op:
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

    ctypedef struct MDB_txn:
        pass

    ctypedef struct MDB_env:
        pass

    ctypedef struct MDB_cursor:
        pass

    ctypedef unsigned int    MDB_dbi

    ctypedef struct MDB_val:
        size_t   mv_size
        void    *mv_data

    ctypedef   struct MDB_stat:
        unsigned int    ms_psize
        unsigned int    ms_depth
        size_t          ms_branch_pages
        size_t          ms_leaf_pages
        size_t          ms_overflow_pages
        size_t          ms_entries

    ctypedef struct MDB_envinfo:
        void            *me_mapaddr
        size_t          me_mapsize
        size_t          me_last_pgno
        size_t          me_last_txnid
        unsigned int    me_maxreaders
        unsigned int    me_numreaders

    ctypedef int (*MDB_cmp_func)(const MDB_val *a, const MDB_val *b)

    char *mdb_strerror(int err)
    int  mdb_env_create(MDB_env **env)
    int  mdb_env_open(MDB_env *env, char *path, unsigned int flags, unsigned int mode)
    int  mdb_env_copy(MDB_env *env, char *path)
    int  mdb_env_stat(MDB_env *env, MDB_stat *stat)
    int  mdb_env_info(MDB_env *env, MDB_envinfo *stat)
    int  mdb_env_sync(MDB_env *env, int force)
    int  mdb_env_set_flags(MDB_env *env, unsigned int flags, int onoff)
    int  mdb_env_get_flags(MDB_env *env, unsigned int *flags)
    int  mdb_env_get_path(MDB_env *env, char **path)
    int  mdb_env_set_mapsize(MDB_env *env, size_t size)
    int  mdb_env_set_maxdbs(MDB_env *env, MDB_dbi dbs)
    int  mdb_env_set_maxreaders(MDB_env *env, unsigned int readers)
    void mdb_env_close(MDB_env *env)

    int  mdb_txn_begin(MDB_env *env, MDB_txn *parent, unsigned int flags, MDB_txn **txn)
    int  mdb_txn_commit(MDB_txn *txn)
    void mdb_txn_abort(MDB_txn *txn)
    void mdb_txn_reset(MDB_txn *txn)
    int  mdb_txn_renew(MDB_txn *txn)

    int  mdb_set_compare(MDB_txn *txn, MDB_dbi dbi, MDB_cmp_func cmp)
    int  mdb_set_dupsort(MDB_txn *txn, MDB_dbi dbi, MDB_cmp_func cmp)
    int  mdb_dbi_open(MDB_txn *txn, char *name, unsigned int flags, MDB_dbi *dbi)
    void mdb_dbi_close(MDB_env *env, MDB_dbi dbi)
    int  mdb_stat(MDB_txn *txn, MDB_dbi dbi, MDB_stat *stat)
    int  mdb_drop(MDB_txn *txn, MDB_dbi dbi, int delete)
    int  mdb_get(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data)
    int  mdb_put(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data, unsigned int flags)
    int  mdb_del(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data)

    int  mdb_cursor_open(MDB_txn *txn, MDB_dbi dbi, MDB_cursor **cursor)
    void mdb_cursor_close(MDB_cursor *cursor)
    int  mdb_cursor_renew(MDB_txn *txn, MDB_cursor *cursor)
    int  mdb_cursor_get(MDB_cursor *cursor, MDB_val *key, MDB_val *data, unsigned int op)
    int  mdb_cursor_put(MDB_cursor *cursor, MDB_val *key, MDB_val *data, unsigned int flags)
    int  mdb_cursor_del(MDB_cursor *cursor, unsigned int flags)
    int  mdb_cursor_count(MDB_cursor *cursor, size_t *countp)
