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

.. seealso::

    Page :ref:`integrationtests`
        Hustle's Integration Test Suite for creating and inserting to partitioned Tables.

    :func:`insert() function <hustle.insert>`

