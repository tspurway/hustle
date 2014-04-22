.. _queryguide:

Hustle Query Guide
==================

Let's take a quick gander at a Hustle query, remember, the query language is Python.  We do some DSL tricks to 
make the 'where' clause especially useful::

    select(impressions.ad_id, impressions.date, impressions.cpm_millis,
           where=(impressions.date > '2014-01-22') & (impressions.ad_id == 30010))

or::

    select(impressions.ad_id, impressions.date, h_sum(pix.amount), h_count(),
           where=((impressions.date < '2014-01-13') & (impressions.ad_id == 30010), pix.date < '2014-01-13'),
           join=(impressions.site_id, pix.site_id),
           order_by=(impressions.date, 'amount'),
           desc=True,
           limit=15)

Hustle is a relational database, but we reject the SQL language.  The queries are similar to SQL with some
defining differences:

* no 'group_by' clause - if there is an aggregating column, then all other non-aggregating columns are assumed to be 'group_by' columns
* no 'from' clause - the 'where' clause lists all of the tables data is queried from
* where clauses use binary operators ``&, | and ~``.  Note that you will need to parenthesize everything...
* joins are not comparisons - unlike SQL, you need to list the join columns, and it uses the equality operator
* joins are against exactly two tables - if you need more you can nest queries in the 'where' clause
* extensible, if you don't like the built-in aggregating functions, add your own, no worries
* limit, desc, distinct, h_sum(), h_avg(), h_count() work as expected

Projections
-----------

The *projections* are the first list of columns or aggregations in the :func:`select <hustle.select>` function.  The
valid objects here are :class:`Columns <hustle.core.marble.Column>` and
:class:`Aggregations <hustle.core.marble.Aggregation>`.  If a column is specified, then the Table that it belongs to
**must** appear in the *where* parameter.  Similarly, if it is an aggregation function, then the table of the column
being aggregated **must** appear in the *where* parameter.

As a shortcut, Hustle also provides the :func:`star() <hustle.star>` function that returns a list of all of the
columns of a table.  It is used like::

    select(*star(impressions), where=impressions.date == '2014-02-20')

Here is a list of built-in Hustle functions that return *Aggregation* objects:

* :func:`hustle.h_sum`
* :func:`hustle.h_count`
* :func:`hustle.h_avg`
* :func:`hustle.h_min`
* :func:`hustle.h_max`

Grouping / Group By
-------------------

Hustle has no explicit *group by* statement.  Instead, if the projection list contains one or more aggregations,
then the *rest* of the non-aggregating columns are used as the grouping.  Consider the following query::

    select(impressions.date, impressions.site_id, h_sum(impressions.cpm_millis),
           where=impressions.date > '2014-01-15',
           order_by='date')

if you ran this in the :ref:`cliguide`, you might get something like this::

    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    date                       site_id                               sum(cpm_millis)
    - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    2014-01-16                 commodo.com                                       673
    2014-01-16                 culpa.com                                       2,522
    2014-01-16                 cupidatat.com                                   1,483
    2014-01-16                 fugiat.com                                      4,183
    2014-01-16                 irure.com                                       3,862
    2014-01-16                 minim.com                                       1,652
    2014-01-16                 nisi.com                                          136
    2014-01-16                 nulla.com                                       1,999
    2014-01-16                 tempor.com                                      3,288
    2014-01-16                 voluptate.com                                   1,238
    2014-01-17                 Lorem.com                                       1,542
    2014-01-17                 cillum.com                                      1,115
    2014-01-17                 consequat.com                                   1,365
    2014-01-17                 dolore.com                                      4,456
    2014-01-17                 fugiat.com                                      4,173
    2014-01-17                 sint.com                                        3,766
    2014-01-17                 sit.com                                         4,376
    2014-01-17                 sunt.com                                        2,088
    ...


Where Clause
------------

The *where* parameter specifies two important pieces of information:  which tables to fetch data from, and how to
restrict the rows returned from those tables.  If a table is referenced in an expression, then the data from that
table is included in the query.  In this way, Hustle has no need of a redundant *from* clause, like in SQL.

The *where* clause is where Hustle's column expression DSL comes into play.  The *where* parameter may be as simple
as a Table instance, or as complex as a list of deeply nested :class:`column expressions <hustle.core.marble.Expr>`.
Consider the following queries::

    q1 = select(impressions.date, where=impressions)
    q2 = select(impressions.date, where=impressions.site_id == 'google.com')
    q3 = select(impressions.date, where=(impressions.site_id == 'google.com') & (impressions.cpm_micros > 100))
    q4 = select(impressions.date, pixels.site_id, where=(impressions.site_id == 'google.com', pixels))

The first three queries are pretty straightforward, the fourth query is selecting from multiple tables.  Note that this
isn't a join, it is simply fetching all *dates* from *impressions* for *google.com* **PLUS** all *site_ids* from
the *pixels* table.  The results are simply concatenated, and *None* values are used where the column doesn't exist for
that table.

Where clause also supports *in* and *not in* statements by using special operators "<<" and ">>" respectively::

    select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.ad_id << [1000, 1005])
    select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.ad_id >> [1000, 1005])

Note that the right value of "<<" and ">>" could be any type of iterable with each element must be a valid single right value.


Partitions
----------

In Hustle, *partitions* are special columns that allow us to split our data into pieces that group together the same
value for that partition.  When we perform a *where* expression on the *partition*, we are able to optimize the
amount of data we consider for the query, thereby vastly improving out query performance.  Currently, Hustle allows
a single partition column per table.

Join Clause
-----------

*Joins* in Hustle are performed by first specifying which tables are to be joined in the *where* clause (as above),
and by listing the columns to be joined in the *join* parameter.  Consider the following query::

    select(impressions.date, pixels.site_id,
           where=(impressions.site_id == 'google.com', pixels.site_id == 'yahoo.com'),
           join=(impressions.token, pixels.token))

Here we are selecting one column from each table, restricting both table's *site_ids*, then joining them on their
respective *token* column.  Joins in Hustle have the following constraints:

* join operations are between exactly two tables - to do more, you must *nest* the queries
* both the *where* and *join* parameters must be sequences of exactly two elements
* currently all joins in Hustle are `inner joins <http://en.wikipedia.org/wiki/Join_(SQL)#Inner_join>`_

Note that the join argument can also take the name of the column to join on if both tables have the same column name.
The above query could be equivalently written::

    select(impressions.date, pixels.site_id,
           where=(impressions.site_id == 'google.com', pixels.site_id == 'yahoo.com'),
           join='token')

Column Cardinality
------------------

Joins are potentially very expensive operations.  It is very important to understand how a join is performed to
ensure that the operation actually completes without stealing all of the resources of your cluster.  The most important
consideration when joining two tables **isn't** the size of the tables, it's the *cardinality* of the column you
are joining on.  A column's cardinality is the number of unique values it holds.  A column like *sex* or *date* or
*age*, which have very few unique values are said to have *low cardinality*.  Columns like *url* or *cookie* or *uid*
are said to have *high cardinality*.

Here are some rules:

* join on high cardinality columns
* restrict (where clause) on low cardinality columns
* always list the table with fewer row first in the *join* clause
* if you need to join on low cardinality tables, try to restrict one table to as few rows as possible, then list that one first in the join

Order By / Desc Clauses
-----------------------

The *order_by* parameter allows you to sort by any number of columns.  You can control the *direction* of the sort
using the *desc* parameter.  The *order_by* allows many different types of input.  It accepts scalars or sequences,
with any combination of the following types:

* a :class:`column <hustle.core.marble.Column>` means to sort by that column (which should obviously appear in the *project* list)
* a :class:`string <basestring>` means to find the first occurrence of the column with that name and sort by it
* an :class:`int` is an index into the *project* list of columns

Here are some examples::

    select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, order_by=imps.date)
    select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, order_by=(imps.date, imps.ad_id))
    select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, order_by='date')
    select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, order_by='imps.date')
    select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, order_by=('date', imps.ad_id))
    select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, order_by=('date', 2))
    select(imps.ad_id, imps.date, h_sum(imps.cpm_millis), where=imps, order_by=2)

Note that for *string* style columns, you can use either just the *name* of the column, or its *table.column*
notation.

Limit / Distinct Clauses
------------------------

The *limit* and *distinct* parameters behave much like their SQL counterparts.  Here's a few examples::

    select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, distinct=True)
    select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, limit=50)

Nested Queries
--------------

Hustle allows for arbitrarily nested level of queries, and for the intermediate results to be saved and reused many
times in a session.  This can be useful for getting around Hustle's maximum join limit (which is two tables), but also
to perform expensive joins, then reuse them many times.  The *nest* parameter is used to enable this functionality.

Consider the following queries::

    late_jan_imps = select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.date > '2014-01-15', nest=True)
    select(late_jan_imps.ad_id, where=late_jan_imps, distinct=True)
    select(late_jan_imps.date, h_sum(late_jan_imps.cpm_millis), where=imps.ad_id == 15)

Note how we can query once into a temporary table, then query multiple times from this table.  This supports an
exploratory style, where (possibly) expensive queries can be saved and then arbitrarily queried again.


select() Return Values
----------------------

To facilitate a nice :ref:`cliguide` and to have usable results when *nesting* queries or just processing row oriented
results of a 'normal' query, the :func:`select <hustle.select>` function will return a number of different results
depending on it's parameters.

* *nest=True* - will return a :class:`Table <hustle.Table>`
* *dump=True* - this is the default in the CLI - will return None, but will dump the result to stdout
* *nest=False, dump=False* - this is the default when writing Python programs, and will return an iterator of tuples representing the result
* *nest=True, dump=True* - same as *nest=True* above

The idea here is that when in the CLI, you really just want to see the output of your query, while building a program,
you would like to just process the results like a normal
`map/reduce disco job <http://disco.readthedocs.org/en/latest/lib/core.html#disco.core.result_iterator>`_.  Here's
an example of processing the results of a query in Python::

    from hustle import select, Table
    imps = Table.from_tag('impressions')
    result = select(imps.date, h_sum(imps.cpm_mills), where=imps)
    for date, total in result:
        print date, total

Also note that it is possible to iterate over all rows in a `Table <hustle.Table>` directly::

    from hustle import select, Table
    imps = Table.from_tag('impressions')
    for date, clicks, impressions, site_id, cpm_millis in imps:
      print date, clicks

.. seealso::

    :func:`hustle.select`
        Hustle's select statement


Non-blocking select()
---------------------
If you want to run multiple queries at the same time, consider using non-blocking select - :func:`select_nb <hustle.select_nb>`. It's exactly the same as :func:`select <hustle.select>` except that it returns immediately after submitting query to the cluster. The user can use the return value - a :class:`Future <hustle.Future>` object to check the query's status and fetch its resutls.

For example::

    >>> future = select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.date > '2014-01-15', nest=True)
    >>> future.status()
    >>> 'active'
    ... run more queries here ...
    >>> future.done
    >>> False
    ... do some other things ...
    >>> future.status()
    >>> 'ready'
    >>> future.done
    >>> True
    ... wait() function gives your results like the select() does ...
    >>> cat(future.wait())
    ... Note that if you call wait() on a ongoing query, it'll block until it's done ...

.. seealso::

    :func:`hustle.select_nb`
    :class:`hustle.Future`
