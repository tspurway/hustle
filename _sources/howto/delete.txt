.. _deleteguide:

Deleting Data in Hustle
=======================
Deleting data in Hustle is partition-oriented, which means you *can't* remove specific rows as conventional database systems dose. There are two functions to do this with different granularities.

Delete
------
:func:`delete() <hustle.delete>` function *only* deletes data but keeps the table definition. If a :class:`Table <hustle.Table>` object specified, all data in that table will be deleted. To delete a particular range of partitions, pass it an :class:`Expr <hustle.core.marble.Expr>`, for example, "impressions.date < '2014-01-01'".

.. seealso::

    :func:`hustle.delete`
        Hustle's delete statement

    :ref:`schemadesign`
        Details of the Hustle Partition

Drop
----
Use :func:`drop() <hustle.drop>` function to delete the whole table, including data, all partitions, and table definition. Unlike :func:`delete() <hustle.delete>`, it *only* takes a :class:`Table <hustle.Table>` object to specify the table you want to drop.

.. seealso::

    :func:`hustle.drop`
        Hustle's drop statement
