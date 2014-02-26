Hustle (beta)
=============

A column oriented, embarrassingly distributed relational NoSQL database.

Features
--------

* column oriented - super fast queries
* distributed insert - Hustle is designed for petabyte scale datasets in a distributed environment with massive write loads
* compressed - bitmap indexes, lz4, and prefix trie compression
* relational - join gigantic datasets
* partitioned - smart shards
* embarrassingly distributed (Disco: http://discoproject.org/)
* embarrassingly fast (lmdb:  http://symas.com/mdb/)
* NoSQL - Python DSL
* bulk append only (it's for 'log' data)
* definitely consistent
* REPL/CLI query interface

BETA / EAP
==========

Please note that this software is beta/early access.  We intend that you thoroughly enjoy wrangling unimaginably large datasets with this software, but really have no idea how it will perform in your particular installation.  Be nice and drop us a GitHub 'issue' or just email me at tspurway@gmail.com for help.

Installation
============

After cloning this repo, here are some considerations:

* you will need Python 2.7 or higher - note that it *probably* won't work on 2.6 (has to do with pickling lambdas...)
* you need to install Disco 0.5 and its dependencies - get that working first
* you need to install Hustle and its 'deps' thusly:

```
cd hustle
sudo ./bootstrap.sh
```

That should do it.  Now, there is a config file in /etc/hustle you should take a look at, and another in /etc/disco.  More on this soon...

Tests
=====

Get the tests working - please find the hustle/integration_test and hustle/test directories.  Both of these  contain 'nose' tests (https://nose.readthedocs.org/en/latest/) that should be run before you do anything else.

The hustle/test tests should be attempted first, which should rout out any installation problems with hustle/deps.

The test in integration/test will actually create tables in your Disco/DDFS installation and run real queries against them.  Check out the readme in the integration_test directory for more info on running these.  If you get these passing, you are surely good to go with your own data.


Inserting Data
==============

Currently, Hustle supports inserting JSON log files.  These are defined as a file with a single JSON dict on every line.  There is a tool at hustle/bin/insert that helps in inserting data into Hustle.

One important consideration is that Hustle is designed to deal with large log files.  Each 'insert' will actually crate a new database on the 'local' machine, which will then be 'pushed' into DDFS.  This is Hustle's 'distributed insert' functionality.  It has several considerations:

*  the bigger your files, the faster your queries will perform (shoot for 1GB or larger insert files)
*  you cannot insert a single record (or just a few records) into Hustle - it is designed to have gigantic, distributed inserts
*  you cannot update data once it is inserted


The CLI
=======

```
cd hustle/bin
./hustle
```

This will open up the Hustle command line REPL/query tool.  It has auto-completion of hustle defined columns and tables, help features, knows Hustle defined tables, and also auto-dumps query results in a very nice format.


Queries
=======

Let's take a quick gander at a Hustle query, remember, the query language is Python.  We do some DSL tricks to make the 'where' clause especially crunchy:

```
select(imps.ad_id, imps.date, imps.cpm_millis,
       where=(imps.date > '2014-01-22') & (imps.ad_id == 30010))
```

nice?  check this one out:

```
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

* no 'group_by' clause - if there is an aggregating column, then all other non-aggregating columns are assumed to be 'group_by' columns
* no 'from' clause - the 'where' clause lists all of the tables data is queried from
* where clauses use binary operators &, | and ~.  Note that you will need to parenthesize everything...
* joins are not comparisons - unlike SQL, you need to list the join columns, and it uses the equality operator
* joins are against exactly two tables - if you need more you can nest queries in the 'where' clause
* extensible, if you don't like the built-in aggregating functions, add your own, no worries
* limit, desc, distinct, h_sum(), h_avg(), h_count() work as expected

