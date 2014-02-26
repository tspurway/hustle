.. _queryguide:

Hustle Query Guide
==================

Let's take a quick gander at a Hustle query, remember, the query language is Python.  We do some DSL tricks to 
make the 'where' clause especially crunchy::

    select(impressions.ad_id, impressions.date, impressions.cpm_millis,
           where=(impressions.date > '2014-01-22') & (impressions.ad_id == 30010))

or::

    select(impressions.ad_id, impressions.date, h_sum(pix.amount), h_count(),
           where=((impressions.date < '2014-01-13') & (impressions.ad_id == 30010), pix.date < '2014-01-13'),
           join=(impressions.site_id, pix.site_id),
           order_by='amount',
           desc=True,
           limit=15)

Hustle is a relational database, but we reject the SQL language.  The queries are similar to SQL with some
defining differences:

* no 'group_by' clause - if there is an aggregating column, then all other non-aggregating columns are assumed to
be 'group_by' columns
* no 'from' clause - the 'where' clause lists all of the tables data is queried from
* where clauses use binary operators &, | and ~.  Note that you will need to parenthesize everything...
* joins are not comparisons - unlike SQL, you need to list the join columns, and it uses the equality operator
* joins are against exactly two tables - if you need more you can nest queries in the 'where' clause
* extensible, if you don't like the built-in aggregating functions, add your own, no worries
* limit, desc, distinct, h_sum(), h_avg(), h_count() work as expected

.. seealso::

    :func:`hustle.select`
        Hustle's select statment