from hustle.core.marble import Column

import mdb


class ColumnFn(object):
    """
    Decorator for column functions.

    Note that the decorating column will inherit all properties from the
    decorated one by default. If the column function will change the data type
    of the original column, remember to overwrite its corresponding types,
    i.e. type_indicator, index_indicator, rtrie_indicator, compression_indicator,
    and boolean. This matters when you want to store the query result back to
    the database. All the specific indicators show as follows:

    ==============             ==================
    type_indicator             Description
    ==============             ==================
    mdb.MDB_STR                String
    mdb.MDB_INT_8/16/32/64     Integer
    mdb.MDB_UINT_8/16/32/64    Unsigned Integer
    ==============             ==================

    ==============             ==================
    compression_indicator      Description
    ==============             ==================
          0                    Trie
          1                    String
          2                    LZ4
          3                    Binary
    ==============             ==================

    ==============             ==================
    trie_indicator             Description
    ==============             ==================
    mdb.MDB_UINT_16            16 bit Trie
    mdb.MDB_UINT_32            32 bit Trie(default)
    ==============             ==================

    ==============             ==================
    boolean                    Description
    ==============             ==================
        True                   Boolean Type
    ==============             ==================

    ==============             ==================
    index_indicator            Description
    ==============             ==================
           1                   index(default)
           2                   wide index
    ==============             ==================
    """
    def __init__(self,
                 type_indicator=None,
                 index_indicator=None,
                 compression_indicator=None,
                 rtrie_indicator=None,
                 boolean=None):
        self.type_indicator = type_indicator
        self.index_indicator = index_indicator
        self.compression_indicator = compression_indicator
        self.rtrie_indicator = rtrie_indicator
        self.boolean = boolean

    def __call__(self, fn):
        def wrap(column):
            index_indicator = self.index_indicator if self.index_indicator is \
                not None else column.index_indicator
            type_indicator = self.type_indicator if self.type_indicator is \
                not None else column.type_indicator
            rtrie_indicator = self.rtrie_indicator if self.rtrie_indicator is \
                not None else column.rtrie_indicator
            compression_indicator = self.compression_indicator if \
                self.compression_indicator is not None else column.compression_indicator
            is_boolean = self.boolean if self.boolean is not None else column.is_boolean

            new_column = Column(column.name, column.table, index_indicator,
                                column.partition, type_indicator, compression_indicator,
                                rtrie_indicator, alias=column.alias, boolean=is_boolean,
                                column_fn=fn)
            return new_column
        return wrap


# column functions defined as follows:

@ColumnFn(type_indicator=mdb.MDB_STR)
def ip_ntoa(val):
    import socket
    import struct
    try:
        ip = socket.inet_ntoa(struct.pack(">L", val))
    except:
        ip = "0.0.0.0"
    return ip


@ColumnFn(type_indicator=mdb.MDB_UINT_32)
def ip_aton(val):
    import socket
    import struct
    try:
        ip = struct.unpack(">L", socket.inet_aton(val))[0]
    except:
        ip = 0
    return ip


@ColumnFn(type_indicator=mdb.MDB_INT_16)
def year(val):
    """
    extract YEAR from "YYYY-MM-DD". Return 0 if failed
    """
    try:
        year = int(val[:4])
    except:
        year = -1
    return year


@ColumnFn(type_indicator=mdb.MDB_INT_8)
def month(val):
    """
    extract MONTH from "YYYY-MM-DD". Return 0 if failed
    """
    try:
        month = int(val[5:7])
    except:
        month = -1
    return month


@ColumnFn(type_indicator=mdb.MDB_INT_8)
def day(val):
    """
    extract DAY from "YYYY-MM-DD". Return 0 if failed
    """
    try:
        day = int(val[-2:])
    except:
        day = -1
    return day


# the old school way to write a column function
# def ip_ntoa(column):
    # "column function for converting an integer IPv4 to a string"
    # import mdb
    # if column.type_indicator != mdb.MDB_UINT_32:
        # raise TypeError("Specified column should be of the uint_32 type")
    # new_column = Column(column.name, column.table, column.index_indicator,
                        # column.partition, mdb.MDB_STR, compression_indicator=0,
                        # rtrie_indicator=column.rtrie_indicator, alias=column.alias,
                        # boolean = False, column_fn=_ip_ntoa)
    # return new_column
