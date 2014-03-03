Hustle Documentation
====================

Hustle is a distributed, column oriented, relational
`OLAP Database <http://en.wikipedia.org/wiki/Online_analytical_processing>`_.  Hustle supports parallel insertions
and queries over large data sets, stored on an unreliable cluster of computers.  It is meant to load and query the
enormous data sets typical of ad-tech, high volume web services, and other large-scale analytics applications.

Hustle is a distributed database.  When data is inserted into Hustle, it is replicated across a cluster to enhance
availability, horizontal scalability and enable parallel query execution.  When data is replicated on multiple nodes,
your database becomes resistant to node failure because there is always multiple copies of it on the cluster.  This
allows you to simply add more machines to increase both overall storage and to decrease query time by performing
more operations in parallel.

Hustle is a relational database, so, unlike other NoSQL databases, it stores its data in rows and columns in a fixed
schema.  This means that you must *create* Tables with a fixed number of Columns of specific data types, before
*inserting* data into the database.  The advantage of this is that both storage and query execution can be
fine tuned to minimize both the data footprint and the query execution time.

Hustle uses a `column oriented format <http://en.wikipedia.org/wiki/Column-oriented_DBMS>`_ for storing data.  This
scheme is often used for very large databases, as it is more efficient for aggregation operations such as sum() and
average() functions over a particular column as well as relational *joins* across tables.

Although Hustle has a relational data model, it is not a SQL database.  Hustle extends the Python language for
its relational query facility.  Let's take a look at a typical Hustle query in Python::

    select(impressions.ad_id, h_sum(pixels.amount), h_count(),
           where=(impressions.date < '2014-01-13', pixels.date < '2014-01-13'),
           join=(impressions.site_id, pixels.site_id),
           order_by='ad_id', desc=True)

which would be equivalent to the SQL query::

    SELECT i.ad_id, i.site_id, sum(p.amount), count(*)
    FROM impressions i
    JOIN pixels p on p.site_id = p.site_id
    WHERE i.date < '2014-01-13' and p.date < '2014-01-13'
    ORDER BY i.ad_id DESC
    GROUP BY i.ad_id, i.site_id

The two approaches seem equivalent, however, Python is extensible, whereas SQL is not.  You can do much more
with Hustle than just query data.  Hustle was designed to express distributed computation over indexed data which
includes, but is not limited to the classic relational *select* statement.  SQL is good at queries, not as an ecosystem
for general purpose data-centric distributed computation.

Hustle is meant for large, distributed inserts, and has *append only* semantics.  It is suited to very large *log*
file style inputs, and once data is inserted, it cannot be changed.  This scheme is typically suitable for
distributed applications that generate large log files, with many (possibly hundreds of) thousands of events
per second.  Hustle has been streamlined to accept structured JSON log files as its primary input format, and to
perform *distributed* inserts.  A distributed insert delegates most of the database creation work to the *client*,
thereby freeing up the cluster's resources and avoiding a central computational pinch point like in other *write bound*
relational OLAP databases.  Hustle can easily handle almost unlimited write load using this scheme.

Hustle utilizes modern compression and indexing data structures and algorithms to minimize overall memory footprint
and to maximize query performance.  It utilizes bitmap indexes, prefix trie (dictionary) and lz4 compression, and has a
very rich set of string and numeric data types of various sizes.  Typically, Hustle data sets are 25% to 50% than
their equivalent GZIPed JSON sources.

Hustle has several auxiliary tools:

* a command line interface (CLI) Python shell with auto-completion of Hustle tables and functions
* a client side insert script

Features
--------

* column oriented - super fast queries
* distributed insert - Hustle is designed for petabyte scale datasets in a distributed environment with massive write loads
* compressed - bitmap indexes, lz4, and prefix trie compression
* relational - join gigantic data sets
* partitioned - smart shards
* embarrassingly distributed (`based on Disco <http://discoproject.org/>`_)
* embarrassingly fast (`uses LMDB <http://symas.com/mdb/>`_)
* NoSQL - Python DSL
* bulk append only semantics
* highly available, horizontally scalable
* REPL/CLI query interface

Getting started
---------------

.. toctree::
   :titlesonly:

   start/install
..   start/tutorial

Hustle In Depth
---------------

.. toctree::
   :titlesonly:

   howto/integration_tests
   howto/configure
   howto/cli
   howto/schema
   howto/query
   howto/insert
   howto/delete

Reference
---------

.. toctree::
   :titlesonly:

   api/hustle
   api/core
