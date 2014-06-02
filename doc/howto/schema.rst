.. _schemadesign:

Hustle Schema Design Guide
==========================

Columns
-------
The syntax for the *columns*  is a sequence of strings, where each string specifies a
column using the following syntax::

    [[wide] index][string | uint[8|16|32|64] | int[8|16|32|64] | trie[16|32] | lz4 | binary] column-name

========        ==========      ========================================
Modifier        Sizes           Description
========        ==========      ========================================
wide                            Use LRU cache when inserting this index
index                           Create index for this column
string                          String Type
bit                             1-bit integer / boolean type
uint            8 16 32 64      Unsigned Integer Type
int             8 16 32 64      Signed Integer Type
trie            16 32           Prefix Trie Compressed String type
lz4                             LZ4 Compressed String type
binary                          Unencoded, uncompressed string type
========        ==========      ========================================


If modifiers are omitted, the **DEFAULT** type is :code:`trie32` (un-indexed)

If sizes are omitted for :code:`uint int trie`, the **DEFAULT** is 32 bits

Example::

    pixels = Table.create('pixels',
          columns=['wide index string token', 'index uint8 isActive', 'index site_id', 'uint32 amount',
                   'index int32 account_id', 'index city', 'index trie16 state', 'index int16 metro',
                   'string ip', 'lz4 keyword', 'index string date'],
          partition='date',
          force=True)

Accessing Fields
----------------

Consider the following code::

    imps = Table.from_tag('impressions')
    select(imps.date, imps.site_id, where=imps)

This is a simple Hustle query written in Python.  Note that the column names *date* and *site_id* are accessed
using standard Python *dot* notation.  All columns are accessed as though they were members of the Table class.

Indexes
-------
By default, columns in Hustle are unindexed.  By indexing a column you make it available for use as a key in
*where clause* and *join clauses* in the :func:`hustle.select` statement.  Unindexed columns can still
be in the list of selected columns or in aggregation function.  The question of whether to index a column or not is a
consideration of overall memory/disk space used by that column in your database.  An indexed column will take up
to twice the amount of memory as an unindexed column.

*Wide indexes* (the '=' indicator) are used simply as a hint to Hustle to expect the number of unique values for
the specified column to be very high with respect to the overall number of rows.  The Hustle query optimizer and
:func:`hustle.insert` function use this information to better manage memory usage when dealing with these columns.

Integer Data
------------

Integers can be 1, 2, 4 or 8 bytes and are either signed or unsigned.

Bit Data
--------

Bits are one bit unsigned integers.  They can represent the number 0 or 1, or the boolean values True and False.

*Bit* typed columns are stored very efficiently and utilize the same bitmap compression that indexed columns
use.  Similarly, it is very efficient to execute aggregating functions over *bit* type data.

String Data and Compression
---------------------------

One of the fundamental design goals of Hustle was to allow for the highest level of compression possible.
String data is one area that we can maximize compression.  Hustle has a total of five types of string
representations: uncompressed, lz4 compressed, two flavours of `Prefix Trie <http://en.wikipedia.org/wiki/Trie>`_
compression, and a binary/blob format.

The first choice for string compression should be the trie compression.  This offers the best performance and can
offer dramatic compression ratios for string data that has many duplicates or many shared prefixes (consider the
strings beginning with "http://www.", for example).  The Hustle trie compression comes in either 2 or 4 byte
flavours.  The two byte flavour can encode up to 65,536 unique strings, and the 4 byte version can encode over
4 billion strings.  Pick the two byte flavour for those columns that have a high degree of full-word repetition,
like 'department', 'sex', 'state', 'country' - whose overall bounds are known.  For strings that have a larger
range, but still have common prefixes and whose overall length is generally less than 256 bytes, like 'url',
'last_name', 'city', 'user_agent',

We investigated many algorithms and implementations of compression algorithms for compressing intermediate sized
string data, strings that are more than 256 bytes.  We found our implementation of lz4 to be both faster and
have much higher compression ratios than `Snappy <https://code.google.com/p/snappy/>`_.  Use LZ4 for fields like
'page_content', 'bio', 'except', 'abstract'.

Some data doesn't like to be compressed.  UIDs and many other hash based data fields are designed to be evenly
distributed, and therefore defeat most (all of our) compression schemes.  In this case, it is more efficient to
simply store the uncompressed string.

Binary Data
-----------

In Hustle, binary data is an attribute that doesn't affect how a string is compressed, but rather, it affects how
the value is treated in our query pipeline.  Normally, result sets are sorted and grouped to execute
*group by clause* and *distinct clause* elements of :func:`hustle.select`.  If you have a column that
contains binary data, such as a .png image or sound file, it doesn't make any sense to sort or group it.

Partitions
----------
Hustle employs a technique for splitting up data into distinct partitions based on a column in the target table.
This allows us to significantly increase query performance by only considering the data that matches the partition
specified in the query.  Typically a partition column has the following attributes:
- the same column is in most Tables
- the number of unique values for the column is low
- the column is often in *where clauses*, often as ranges

The DATE column usually fits the bill for the partition in most LOG type applications.

Hustle currently supports a single column partition per table.  All partitions must also be indexed.  Partitions
must currently be uncompressed string types ('$' indicator).

Partitions are implemented both as regular columns in the database and with a DDFS tagging convention.  All Hustle
tables have DDFS tags that look like::

    hustle:employees

where the *name* of the Table is employees.  Tables that have partitions will never actually store data under this
*root tag* name, rather they will store it under tags that look like::

    hustle:employees:2014-02-21

this is assuming that the *employee* table has the *date* field as a partition.  All of the data marbles for the
date 2014-02-22 for the *employees* table is guaranteed to be stored under this DDFS tag.  When Hustle sees a query
with a where clause identifying this exact date (or a range including this date), we will be able to directly
and quickly access the correct data, thereby increasing the speed of the query.

