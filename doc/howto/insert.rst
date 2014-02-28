.. _insertguide:

Inserting Data To Hustle
========================

The process of inserting data into a Hustle cluster is referred to as a *distributed* insert.  It is
distributed because the client machine does the heavy lifting of creating a
:class:`Marble <hustle.core.marble.Marble>`, which is a self-contained large grained database fragment, which
is then `pushed into the distributed file system DDFS <http://disco.readthedocs.org/en/latest/howto/ddfs.html#ddfs>`_,
which is a relatively inexpensive HTTP operation.  The write throughput to the Hustle cluster, then, is only
bound by the number of machines inserting into it.

Hustle currently supports `one JSON object per line <http://json.org>`_ style input, as well as
`Disco's native results format <http://disco.readthedocs.org/en/latest/faq.html#chaining>`_.

Here is an example insert::

    from hustle import Table, insert
    impressions = Table.from_tag('impressions')
    insert(impressions, './imprsions-june-8.json', server='disco://hustle')

Hustle provides a command-line tool for inserting data located at :code:`bin/insert`.  Here is the *--help* for
it::

    ➜ hustle/bin > ./insert --help
    usage: insert [-h] [-s SERVER] [-f INFILE] [-m MAXSIZE] [-t TMPDIR]
              [-p PROCESSOR] [--disco-chunk]
              TABLE FILES [FILES ...]

    Hustle bulk load

    positional arguments:
      TABLE                 The Hustle table to insert to
      FILES                 The JSON formated files to insert

    optional arguments:
      -h, --help            show this help message and exit
      -s SERVER, --server SERVER
                            DDFS server destination
      -f INFILE             A file containing a list of all files to be inserted
      -m MAXSIZE, --maxsize MAXSIZE
                            Initial size of Hustle marble
      -t TMPDIR, --tmpdir TMPDIR
                            Temporary directory for Hustle marble creation
      -p PROCESSOR          a module.function for the Hustle import preprocessor
      --disco-chunk         Indicated if the input files are in Disco CHUNK format

.. seealso::

    Page :ref:`integrationtests`
        Hustle's Integration Test Suite for creating and inserting to partitioned Tables.

    :func:`insert() function <hustle.insert>`

