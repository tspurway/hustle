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

Let's take a quick gander at a Hustle query, remeber, the query langauge is Python.  We do some DSL tricks to make the 'where' clase especially munchie:

```
imps = Table.from_tag('impressions')\

res = select(
    imps.ad_id, imps.date, imps.cpm_millis,
    where=(imps.date > '2014-01-22') & (imps.ad_id == 30010))

```