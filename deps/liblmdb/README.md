Requires:
=======
 - liblmdb
 - Python 2.7 (that is all I have tested with)
 - Cython

Install:
=======
```
    $ sudo python setup.py install
```

Usage
=====

Using Writer and Reader
-----------------------

    >>> import mdb
    >>> writer = mdb.Writer('/tmp/mdbtest')
    >>> writer.put('foo', 'bar')
    >>> writer.mput({"key": "value", "egg": "spam"})
    >>> writer.close()
    >>> reader = mdb.Reader('/tmp/mdbtest')
    >>> reader.get('foo')
    >>> for key, value in reader.iteritems():
    ...   print key, value
    >>> reader.close()

Using Integer Key
-----------------
    >>> writer = mdb.Writer('/tmp/mdbtest', dup=True, int_key=True)
    >>> writer = writer.put(1, 'foo')
    >>> writer = writer.put(1, 'bar')  # append a duplicate key
    >>> writer.close()
    >>> reader = mdb.DupReader('/tmp/mdbtest', int_key=True)
    >>> for v in reader.get(1):
    ...   print v
    >>> reader.close()
    
Using Low-level Stuff
---------------------
    >>> env = mdb.Env('/tmp/mdbtest')
    >>> txn = env.begin_txn()
    >>> db = env.open_db(txn)
    >>> db.put(txn, 'hi', 'assinine')
    >>> txn.commit()
    >>> txn = env.begin_txn()
    >>> print '"%s"' % db.get(txn, 'hi')  # --> assinine
    >>> txn.close()
    >>> db.close()
    >>> env.close()
