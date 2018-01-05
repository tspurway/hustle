from hustle.core.marble import Aggregation, Column

import mdb


def h_cardinality(col):
    """
    """

    def _inner_deault():
        from cardunion import Cardunion
        return Cardunion(12)

    def _inner_hll_accumulate(a, v):
        a.bunion([v])
        return a

    return Aggregation("cardinality",
                       col,
                       f=_inner_hll_accumulate,
                       g=lambda a: a.count(),
                       h=lambda a: a.dumps(),
                       default=_inner_deault,
                       result_spec=Column('_cardinality_type', type_indicator=mdb.MDB_UINT_32))


def h_union(col):
    def _inner_deault():
        from cardunion import Cardunion
        return Cardunion(12)

    def _inner_hll_accumulate(a, v):
        a.bunion([v])
        return a

    return Aggregation("union",
                       col,
                       f=_inner_hll_accumulate,
                       g=lambda a, c: a.dumps(),
                       h=lambda a: a.dumps(),
                       default=_inner_deault,
                       result_spec=Column('_union_type', type_indicator=mdb.MDB_STR, compression_indicator=3))


def h_minhash_merge(col):
    def _inner_deault():
        from maxhash import MaxHash
        return MaxHash()

    def _inner_hll_accumulate(a, v):
        from maxhash import MaxHash
        a.merge(MaxHash.loads(v))
        return a

    return Aggregation("minhash_merge",
                       col,
                       f=_inner_hll_accumulate,
                       g=lambda a, c: a.dumps(),
                       h=lambda a: a.dumps(),
                       default=_inner_deault,
                       result_spec=Column('_minhash_merge_type', type_indicator=mdb.MDB_STR, compression_indicator=3))
