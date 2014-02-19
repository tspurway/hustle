Hustle
======

A column oriented, embarrassingly distributed relational NoSQL database.  Good for logs.

There are a lot of databases, so let's just bulletpoint what Hustle is and does:

* column oriented - super fast queries
* compressed - uses bitmap indexes, lz4, and prefix trie compression
* relational - joins across gigantic datasets
* partitioned - helps manage your data, typically by 'date'
* embarrassingly distributed (uses Disco: http://discoproject.org/)
* embarassingly fast (uses lmdb:  http://symas.com/mdb/)
* NoSQL - uses a Python DSL, which is frankly faster than interpretting ol' SQL
* bulk append only (it's for 'log'ish data)
* distributed inserts - solves common write-bound issues with column orineted DBs
* arguably ACID
* definitely consistent
* REPL/CLI query interface

Let's take a quick gander at a Hustle query, remeber, the query langauge is Python.  We do some DSL tricks to make the 'where' clase especially munchie:

```
imps = Table.from_tag('impressions')

select(imps.ad_id, imps.date, imps.cpm_millis,
       where=(imps.date > '2014-01-22') & (imps.ad_id == 30010))
```

that's cool.  check this one out:

```
imps = Table.from_tag('impressions')
pix = Table.from_tag('pixels')

select(imps.ad_id, imps.date, h_sum(pix.amount), h_count(),
       # please note the entirely necessary parentheses in the 'where' clause
       where=((imps.date < '2014-01-13') & (imps.ad_id == 30010),
               pix.date < '2014-01-13'),
       join=(imps.site_id, pix.site_id),
       order_by='amount',
       desc=True,
       limit=15)
```

Hustle is a relational database, but we reject the SQL language.  The queries are similar to SQL with some defining differences:

* no sql - it's damn python!
* no 'group_by' clause - if there is an aggregating column, then all other non-aggregating columns are assumed to be 'group_by' columns
* no 'from' clause - the 'where' clause lists all of the tables data is queried from
* where clauses use binary operators &, | and ~.  Note that you will need to parenthesize everything...
* joins are not comparisons - unlike SQL, you need to list the join columns, and it uses the equality operator
* joins are against exactly two tables - if you need more you can nest queries in the 'where' clause
* extensible, if you don't like the built-in aggregating functions, add your own, no worries



