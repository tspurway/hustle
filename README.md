Hustle (beta)
=============

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

BETA / EAP
==========

Please note that this software is beta/early access.  We intend that you thoroughly enjoy this software, but really have no idea how it will perform in your particular installation.  Be nice and drop us a GitHub 'issue' or just email me at tspurway@gmail.com if there are issues and you want to throttle something.

Installation
============

After cloning this repo and plunging into install land, here are some considerations:

* you need to install Disco 0.5 and it's dependencies - get that working first
* you need to install Hustle and it's 'deps'

```
cd hustle
sudo ./bootstrap.sh
```

That should do it.  Now, there is a config file in /etc/hustle you should take a look at, and another in /etc/disco.  I will need to write more about these...

Tests
=====

Before wandering too far down various rabbit holes, please find the hustle/integration_test and hustle/test directories.

These contain 'nose' tests (https://nose.readthedocs.org/en/latest/) that should be run before you do anything else.  The hustle/test tests should be attempted first, which should rout out any installation problems with hustle/deps, then you should get Disco running and give the hustle/integration_test tests a go.  If all this passes, you will be good to go with your own data!


The CLI
=======

```
cd hustle/bin
./hustle
```

This will open up the Hustle command line REPL/query tool.  It has auto-completion of hustle defined columns and tables, and also defines and 'auto-dump' feature that allows you to simply query and get results printed nicely to your screen.


Queries
=======

Let's take a quick gander at a Hustle query, remeber, the query langauge is Python.  We do some DSL tricks to make the 'where' clase especially crunchie:

```
imps = Table.from_tag('impressions')

select(imps.ad_id, imps.date, imps.cpm_millis,
       where=(imps.date > '2014-01-22') & (imps.ad_id == 30010))
```

see?  check this one out:

```
imps = Table.from_tag('impressions')
pix = Table.from_tag('pixels')

select(imps.ad_id, imps.date, h_sum(pix.amount), h_count(),
       # please note the entirely necessary parentheses in the 'where' clause, and the multiple tables...
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
* limit, desc, distinct, h_sum(), h_avg(), h_count() work as expected




