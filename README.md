![Hustle](doc/_static/hustle.png)

A column oriented, embarrassingly distributed, relational OLAP database.

Features
--------

* column oriented - super fast queries
* distributed insert - Hustle is designed for petabyte scale datasets in a distributed environment with massive write loads
* compressed - bitmap indexes, lz4, and prefix trie compression
* relational - join gigantic data sets
* partitioned - smart shards
* embarrassingly distributed ([based on Disco](http://discoproject.org/))
* embarrassingly fast ([uses LMDB](http://symas.com/mdb/))
* NoSQL - Python DSL
* bulk append only semantics
* highly available, horizontally scalable
* REPL/CLI query interface

Example Query
-------------

```
select(impressions.ad_id, impressions.date, h_sum(pix.amount), h_count(),
       where=((impressions.date < '2014-01-13') & (impressions.ad_id == 30010),
               pix.date < '2014-01-13'),
       join=(impressions.site_id, pix.site_id),
       order_by=impressions.date)
```


BETA / EAP
----------

Please note that this software is beta/early access.  We intend that you thoroughly enjoy wrangling unimaginably large datasets with this software, but really have no idea how it will perform in your particular installation.  Be nice and drop us a GitHub 'issue' or just email me at tspurway@gmail.com for help.

Installation
------------

After cloning this repo, here are some considerations:

* you will need Python 2.7 or higher - note that it *probably* won't work on 2.6 (has to do with pickling lambdas...)
* you need to install Disco 0.5 and its dependencies - get that working first
* you need to install Hustle and its 'deps' thusly:

```
cd hustle
sudo ./bootstrap.sh
```

Please refer to the [Installation Guide](http://chango.github.io/hustle/start/install.html) for more details

Documentation
=============

http://chango.github.io/hustle/

[Hustle Mailing List](http://groups.google.com/group/hustle-users)


Credits
=======

Special thanks to following open-source projects:

* [EWAHBoolArray](https://github.com/lemire/EWAHBoolArray)
* [disco](http://discoproject.org/)
* [liblmdb](http://symas.com/mdb/)
* [lz4](https://code.google.com/p/lz4/)
* [ultrajson](https://github.com/esnme/ultrajson)
