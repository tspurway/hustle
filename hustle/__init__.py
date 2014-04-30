"""
:mod:`hustle` -- Hustle Distributed OLAP Database
=================================================

The :mod:`hustle` module contains everything a client application will
typically need to create hustle :class:`Tables <hustle.Table>` and to insert and query
data to/from them.

Hustle :class:`Tables <hustle.Table>` are stored in multiple, `LMDB <http://symas.com/mdb/>`_ memory
mapped files, which are replicated into `Disco's <http://discoproject.org>`_ Distributed File System (*DDFS*).
Queries are run using a :ref:`Python DSL <queryguide>` which is translated dynamically into a Disco
*pipelined map/reduce*  job.

Hustle is ideal for low latency OLAP queries over massive data sets.
"""
__version__ = '0.2.2'


from disco.core import Job, Disco
from disco.error import CommError
from hustle.core.marble import Aggregation, Marble, json_decoder, Column, Expr, check_query

import os
import sys
import ujson


_TAG_PREFIX = 'hustle:'
_ALG_RIGHT, _ALG_LEFT, _ALG_CENTER = 0x01, 0x10, 0x20


class Table(Marble):
    """
    The fundamental data type to support Hustle's relational model.  A Table contains a number of named
    :class:`Columns <hustle.core.marble.Column>`, each of which is decorated with schema information.
    Note that the table is stored in Disco's
    `DDFS <http://disco.readthedocs.org/en/latest/lib/ddfs.html>`_ distributed file system as a series of replicated
    sub-database files encapsulated by `LMDB <http://symas.com/mdb/>`_ memory mapped b+ tree files called
    :class:`Marbles <hustle.core.marble.Marble>`.  Each *Marble* contains the data for the rows and columns
    of the Table.

    Normally, a table is created using :meth:`create() <hustle.Table.create>`, which creates the appropriately
    named DDFS tag and attributes.  To instantiate an existing Table (to use in a query, for example), the
    :meth:`from_tag() <hustle.Table.from_tag>` method is used.

    see :ref:`schemadesign` for a detailed look at Hustle's schema design language and features.
    """
    def __init__(self, name=None, fields=(), partition=None, **kwargs):
        """
        Internal - create a new Hustle table.  Use :meth:`hustle.Table.create` to create Tables.
        """
        super(Table, self).__init__(name, fields, partition)
        self._blobs = None

    @classmethod
    def from_tag(cls, name, **kwargs):
        """
        Instantiate a named :class:`Table <hustle.Table>` based on meta data from a *DDFS* tag.

        :type  name: string
        :param name: the name of the table
        """
        from hustle.core.settings import Settings
        settings = Settings(**kwargs)
        ddfs = settings['ddfs']

        partition = ujson.loads(ddfs.getattr(cls.base_tag(name), '_partition_'))
        fields = ujson.loads(ddfs.getattr(cls.base_tag(name), '_fields_'))
        return cls(name=name, fields=fields, partition=partition)

    @classmethod
    def create(cls, name, columns=(), fields=(), partition=None, force=False, **kwargs):
        """
        Create a new :class:`Table <hustle.Table>`, replace existing table if force=True.

        :type  name: string
        :param name: the name of the table to create

        :type  columns: sequence of string
        :param columns: the list of *columns* and their extended index/type information

        :type  fields: sequence of string
        :param fields: the list of *columns* and their encoded index/type information

        :type  partition: string
        :param partition: the name of the column to act as the partition for this table

        :type  force: bool
        :param force: overwrite the existing DDFS base tag with this schema

        If *columns* is set, the *fields* parameter is ignored.

        Example::

            pixels = Table.create('pixels',
                  columns=['index string token', 'index uint8 isActive', 'index site_id', 'uint32 amount',
                           'index int32 account_id', 'index city', 'index trie16 state', 'index int16 metro',
                           'string ip', 'lz4 keyword', 'index string date'],
                  partition='date',
                  force=True)

        .. warning::
            This function will not delete or update existing data in any way.  If you use :code:`force=True` to change
            the schema, make sure you either make the change backward compatible (by only adding new columns), or
            by deleting and reloading your data.

        .. seealso::
            For a good example of creating a partitioned Hustle database see :ref:`integrationtests`
            For detailed schema design docs look no further than :ref:`schemadesign`
        """
        from hustle.core.settings import Settings
        settings = Settings(**kwargs)
        ddfs = settings['ddfs']

        if ddfs.exists(cls.base_tag(name)):
            print "Table already exists..."
            if force:
                print "   Overwriting schema..."
            else:
                return None

        if len(columns):
            fields = cls.parse_column_specs(columns)

        ddfs.setattr(cls.base_tag(name), '_fields_', ujson.dumps(fields))
        ddfs.setattr(cls.base_tag(name), '_partition_', ujson.dumps(partition))
        return cls(name=name, fields=fields, partition=partition)

    @classmethod
    def parse_column_specs(cls, columns):
        sizes = {'': '4', '8': '1', '16': '2', '32': '4', '64': '8'}
        fields = []
        for column in columns:
            words = column.split(' ')
            column_name = words[-1]
            index = ''
            typ = ''
            size = ''
            for word in words[:-1]:
                if word == 'wide':
                    if index:
                        raise ValueError("Index already specified on: %s" % column)
                    else:
                        index = '='

                elif word == 'index':
                    if not len(index):
                        index = '+'

                elif len(typ):
                    raise ValueError("Too many types specified on: %s" % column)

                elif word.startswith('uint'):
                    typ = '@'
                    size = word[4:]

                elif word.startswith('int'):
                    typ = '#'
                    size = word[3:]

                elif word.startswith('trie'):
                    typ = '%'
                    size = word[4:]

                elif word == 'lz4':
                    typ = '*'

                elif word == 'binary':
                    typ = '&'

                elif word == 'string':
                    typ = '$'

                elif word == 'bit':
                    typ = '!'

                else:
                    raise ValueError("Illegal type modifier on: %s mod=%s" % (column, word))

                if size not in sizes:
                    raise ValueError("Illegal size on: %s size=%s" % (column, size))
                elif len(size):
                    size = sizes[size]

            newtype = index + typ + size + column_name
            fields.append(newtype)
        return fields

    @classmethod
    def base_tag(cls, name, partition=None):
        """
        return the *DDFS* tag name for a given hustle table name

        :type  name: string
        :param name: the name of the table

        :type  partition: string
        :param partition: the value of the partition
        """
        rval = "hustle:" + name
        if partition:
            rval += ':' + str(partition)
        return rval

    def __iter__(self):
        """
        return a generator of all columns from a table

        """
        return select(*star(self), where=self, dump=False, nest=False)

    def dumps(self):
        dump = {}
        dump['name'] = self._name
        dump['fields'] = self._fields
        dump['blobs'] = self._blobs
        dump['partition'] = self._partition
        return ujson.dumps(dump)

    @classmethod
    def loads(cls, s):
        dump = ujson.decode(s)
        t = cls(dump['name'], dump['fields'], dump['partition'])
        t._blobs = dump['blobs']
        return t


def insert(table, phile=None, streams=None, preprocess=None,
           maxsize=100 * 1024 * 1024, tmpdir='/tmp', decoder=None,
           lru_size=10000, header=False, **kwargs):
    """
    Insert data into a Hustle :class:`Table <hustle.Table>`.

    Create a  :class:`Marble <hustle.core.marble.Marble>` file given the input file or streams according to the
    schema of the table.  Push this (these) file(s) into *DDFS* under the appropriated (possibly) partitioned *DDFS*
    tags.

    Note that a call to :func:`insert() <hustle.insert>` may actually create and push more than one file,
    depending on how many partition values exist in the input.  Be careful.

    For a good example of inserting into a partitioned Hustle database see :ref:`insertguide`

    :type  table: :class:`Table <hustle.Table>`
    :param table: the table to perform the insert on

    :type  phile: string
    :param phile: the file path to open

    :type  streams: sequence of iterable
    :param streams: as an alternative to the *phile* argument, you can specify a list of generators as input

    :type  preprocess: function
    :param preprocess: a function that accepts and returns a dict()

        The input is transformed into a :class:`dict` by the *decoder* param, then the *preprocess* function is
        called for every record.  This gives you the opportunity to transform, filter or otherwise clean your
        data before it is inserted into the :class:`Marble <hustle.core.marble.Marble>`

    :type  maxsize: int
    :param maxsize: the initial size in bytes of the *LMDB* memory mapped file

        Note that the actual underlying LMDB file will grow as data is added to it - this setting is just for its
        initial size.

    :type  tmpdir: string
    :param tmpdir: the temporary directory to write the LMDB memory mapped file

        Note that choosing a directory on an SSD drive will nicely increase throughput.

    :type  decoder: function
    :param decoder: accepts a line of raw input from the input and returns a :class:`dict`

        The dict is expected to have keys that correspond to the column names in the table you are inserting to.  There
        are two built-in decoders in Hustle: :func:`json_decoder() <hustle.core.marble.json_decoder>` (default) and
        :func:`kv_decoder() <hustle.core.marble.kv_decoder>` for processing JSON and Disco *chain* input files,
        respectively.

    :type  lru_size: int
    :param lru_size: the size in records of the LRU cache for holding bitmapped indexes

        You probably won't have to worry about this unless you find your insert is running out of memory or is too
        slow when inserting gigantic files or on nodes with limited memory resources.
    """
    from hustle.core.settings import Settings
    settings = Settings(**kwargs)
    ddfs = settings['ddfs']

    if not decoder:
        decoder = json_decoder

    def part_tag(name, partition=None):
        rval = "hustle:" + name
        if partition:
            rval += ':' + str(partition)
        return rval
    if phile:
        streams = [open(phile)]
    lines, partition_files = table._insert(streams, preprocess=preprocess,
                                           maxsize=maxsize, tmpdir=tmpdir,
                                           decoder=decoder, lru_size=lru_size,
                                           header=header)
    if partition_files is not None:
        for part, pfile in partition_files.iteritems():
            tag = part_tag(table._name, part)
            st = os.stat(pfile)
            ddfs.push(tag, [pfile])
            print 'pushed %s(%.2fG), %s to %s' % \
                (part, st.st_size * 1.0 / 1073741824, tag, ddfs)
            os.unlink(pfile)
    return table._name, lines


def select(*project, **kwargs):
    """
    Perform a relational query, by selecting rows and columns from one or more tables.

    The return value is either::

    * an iterator over the resulting tuples when :code:`nest==False`
    * a :class:`Table <hustle.Table>` instance when :code:`nest==True`
    * in the case of :code:`nest==False and dump==True` return None (this is the default CLI interaction)

    For all of the examples below, *imps* and *pix* are instances of :class:`Table <hustle.Table>`.

    :type project: list of :class:`Column <hustle.core.marble.Column>` | :class:`Aggregation <hustle.core.marble.Aggregation>`
    :param project: a positional argument list of columns and aggregate expressions to return in the result

        A simple projection::

            select(imps.ad_id, imps.date, imps.cpm_millis, where=imps)

        Selects three columns from the *imps* table.

        Hustle also allows for *aggregation functions* such as :func:`h_sum() <hustle.h_sum>`,
        :func:`h_count <hustle.h_count>`, :func:`h_min() <hustle.h_min>`, :func:`h_max() <hustle.h_max>`,
        :func:`h_avg <hustle.h_avg>` as in this example which sums the :code:`imps.cpm_millis`
        column::

            select(imps.ad_id, h_sum(imps.cpm_millis), h_count(), where=imps.date == '2014-01-27')

        Note that Hustle doesn't have a *group by* clause.  In this query, the output will be *grouped* by the
        :code:`imps.ad_id` column implicitly.  Note that in Hustle, if there is an aggregation function present in the
        :code:`project` param, the query results will be *grouped* by all non-aggregation present.

    :type where: (optional) sequence of :class:`Table <hustle.Table>` | :class:`Expr <hustle.core.marble.Expr>`
    :param where: the Tables to fetch data from, as well as the conditions in the *where clause*

        This two purposes: to specify the tables that are to be queried and to allow for the
        selection of data under specific criteria with our Python DSL selection syntax, much the like SQL's *where
        clause*::

            # simple projection with restriction
            select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.date == '2014-01-27')

        Note the :code:`==` operation between the :code:`imps.date` column and the date string.
        The :class:`Column <hustle.core.marble.Column>`
        class overrides all of Python's comparison operators, which, along with the *&*, *|* and *~* logical
        operators allows you to build arbitrarily complex column selection expressions like this::

            select(imps.ad_id, imps.date, imps.cpm_millis,
                    where=((imps.date >= '2014-01-21') & (imps.date <= '2014-01-23')) |
                          ~(imps.site_id == 'google.com))

        Note that for these expressions, the column must come first.  This means that the following expression is
        **illegal**::

            select(imps.ad_id, imps.date, imps.cpm_millis, where='2014-01-27' == imps.date)

        Where clause also supports *in* and *not in* statements by using special operators "<<" and ">>" respectively::

            select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.ad_id << [1000, 1005])
            select(imps.ad_id, imps.date, imps.cpm_millis, where=imps.ad_id >> [1000, 1005])

        Note that the right value "<<" and ">>" could be any type of iterable with each element must be
        a valid single right value.

        In addition, multiple tables can be specified in the where clause like this::

            select(imps.ad_id, pix.amount, where=(imps.date < '2014-01-13', pix))

        which specifies an expression, :code:`imps.date < '2014-01-13'` and a :class:`Table <hustle.Table>` tuple.
        This query will simply return all of the *ad_id* values in *imps* for dates less than January 13th followed
        by all of the *amount* values in the *pix* table.

        Using multiple columns is typically reserved for when you use a *join clause*

    :type join: string | sequence of exactly length 2 of :class:`Column <hustle.core.marble.Column>`
    :param join: specified the columns to perform a relational join operation on for the query

        The join columns can be specified either as a list of 2 columns, or a list of 2 strings. In particular, if
        two columns have the same names, a single string is valid as well.

        Here's an example of a Hustle join::

            select(imps.ad_id, imps.site_id, h_sum(pix.amount), h_count(),
                   where=(imps.date < '2014-01-13', pix.date < '2014-01-13'),
                   join=(imps.site_id, pix.site_id))

        or equivalently::

            select(imps.ad_id, imps.site_id, h_sum(pix.amount), h_count(),
                   where=(imps.date < '2014-01-13', pix.date < '2014-01-13'),
                   join='site_id')

        which joins the *imps* and *pix* tables on their common *site_id* column, then returns the sum of the
        *pix.amount* columns and a count, grouped by the *ad_id* and the *site_id*.  The equivalent query in SQL
        is::

            select i.ad_id, i.site_id, sum(p.amount), count(*)
            from imps i
            join pix p on p.site_id = p.site_id
            where i.date < '2014-01-13' and i.date < '2014-01-13'
            group by i.ad_id, i.site_id

    :type full_join: bool
    :param full_join:

        if True, specifies that a full join between the specified tables in the *where clause* should
        be joined in a full cross-product.  Note that if both *full_join* and *join* are specified, *join* will
        be ignored.

    :type order_by: string | :class:`Column <hustle.core.marble.Column>` | int |
        (sequence of string | :class:`Column <hustle.core.marble.Column>` | int)
    :param order_by: the column(s) to sort the result by

        The sort columns can be specified either as a Column or a list of Columns.  Alternatively, you can specify
        a column by using a string with either the name of the column or the *table.column* string notation.
        Furthermore, you can also represent the column using a zero based index of the *projected* columns.  This
        last case would be used for *Aggregations*.  Here are a few examples::

            select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, order_by=imps.date)
            select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, order_by=(imps.date, imps.ad_id))
            select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, order_by='date')
            select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, order_by='imps.date')
            select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, order_by=('date', imps.ad_id))
            select(imps.ad_id, imps.date, imps.cpm_millis, where=imps, order_by=('date', 2))
            select(imps.ad_id, imps.date, h_sum(imps.cpm_millis), where=imps, order_by=2)

    :type desc: boolean
    :param desc: affects sort order of the *order_by clause* to descending (default ascending)

    :type distinct: boolean
    :param distinct: indicates whether to remove duplicates in results

    :type limit: int
    :param limit: limits the total number of records in the output

    :type block: boolean
    :param block: make select call either blocking (default) or non-blocking.  If True, causes select() to return
    a :class:`Future <hustle.Future>` object

    :type nest: boolean (default = False)
    :param nest: specify that the return value is a :class:`Table <hustle.Table>` to be used in another query

        This allows us to build nested queries.  You may want to do this to join more than two tables, or to reuse
        the results of a query in more than one subsequent query.  For example::

            active_pix = select(*star(pix), where=pix.isActive > 0, nest=True)
            select(h_sum(active_pix.amount), where=active_pix)

    :type kwargs: dict
    :param kwargs: custom settings for this query see :mod:`hustle.core.settings`

    """

    from hustle.core.settings import Settings
    from hustle.core.pipeline import SelectPipe
    from hustle.core.util import ensure_list

    settings = Settings(**kwargs)
    wheres = ensure_list(settings.pop('where', ()))
    order_by = ensure_list(settings.pop('order_by', ()))
    join = settings.pop('join', ())
    full_join = settings.pop('full_join', False)
    distinct = settings.pop('distinct', False)
    desc = settings.pop('desc', False)
    limit = settings.pop('limit', None)
    wide = settings.pop('wide', False)
    nest = settings.pop('nest', False)
    block = settings.pop('block', True)
    autodump = settings.pop('dump', False)
    pre_order_stage = settings.pop('pre_order_stage', ())
    ddfs = settings['ddfs']
    partition = settings.pop('partition', 0)
    if partition < 0:
        partition = 0

    try:
        # if join is a string, extract the actual join columns.
        # do it here to make the query checker happy.
        join = _resolve_join(wheres, join)
        check_query(project, join, order_by, limit, wheres)
    except ValueError as e:
        print "  Invalid query:\n    %s" % e
        return None

    name = '-'.join([where._name for where in wheres])[:64]
    job_blobs = set()
    for where in wheres:
        job_blobs.update(tuple(sorted(w)) for w in _get_blobs(where, ddfs))

    job = SelectPipe(settings['server'],
                     wheres=wheres,
                     project=project,
                     order_by=order_by,
                     join=join,
                     full_join=full_join,
                     distinct=distinct,
                     desc=desc,
                     limit=limit,
                     partition=partition,
                     wide=wide,
                     nest=nest,
                     pre_order_stage=pre_order_stage)

    job.run(name='select_from_%s' % name, input=job_blobs, **settings)
    if block:
        blobs = job.wait()
        if nest:
            rtab = job.get_result_schema(project)
            rtab._blobs = blobs
            return rtab
        elif autodump:
            # the result will be just dumped to stdout
            cols = [c.name for c in project]
            _print_separator(80)
            _print_line(cols, width=80, cols=len(cols),
                        alignments=[_ALG_RIGHT if c.is_numeric else _ALG_LEFT
                                    for c in project])
            _print_separator(80)
            cat(_query_iterator(blobs), 80)
            return
        return _query_iterator(blobs)
    else:
        return Future(job.name, job, settings['server'], nest, *project)


def stat(where, limit=16, **kwargs):
    from hustle.core.settings import Settings
    from hustle.core.stat import StatPipe
    from disco.core import result_iterator
    from collections import defaultdict

    settings = Settings(**kwargs)
    ddfs = settings['ddfs']
    job_blobs = set(tuple(sorted(w)) for w in _get_blobs(where, ddfs, limit))
    # print job_blobs
    job = StatPipe(settings['server'])
    job.run(name="stat_" + where._name, input=job_blobs, **settings)
    res = job.wait()

    # first we need the total, so that we can calculate weighted average
    total = float(sum(v['_'] for _, v in result_iterator(res)))
    final = defaultdict(int)
    for _, cols in result_iterator(res):
        weight = cols.pop('_') / total
        for col, card in cols.iteritems():
            final[col] += card * weight

    # round everything up to a number between 0 .. 100
    really_final = {}
    for key in final:
        card = int(final[key] * 100)
        if card > 0:
            really_final[key] = card
    really_final['_'] = int(total)

    return really_final


def h_sum(col):
    """
    Return an aggregation for the sum of the given column.  Like SQL sum() function.
    This is used in :func:`hustle.select` calls to specify the sum aggregation over a column in a query::

        select(h_sum(employee.salary), employee.department, where=employee.age > 25)

    returns the total salaries for each departments employees over 25 years old

    :type col: :class:`hustle.core.marble.Column`
    :param col: the column to aggregate
    """
    import mdb
    return Aggregation("sum",
                       col,
                       f=lambda a, v: a + v,
                       default=lambda: 0,
                       result_spec=Column('_sum_type', type_indicator=mdb.MDB_INT_32))


def h_count():
    """
    Return an aggregation for the count of each grouped key in a query.  Like SQL count() function::

        select(h_count(), employee.department, where=employee)

    returns a count of the number of employees in each department.
    """
    import mdb
    return Aggregation("count",
                       Column(name='_count', type_indicator=1),
                       f=lambda a, v: a + v,
                       default=lambda: 0,
                       result_spec=Column('_count_type', type_indicator=mdb.MDB_UINT_32))


def h_max(col):
    """
    Return an aggregation for the maximum of the given column.  Like the SQL max() function::

       select(h_max(employee.salary), employee.department, where=employee)

    returns the highest salary for each department.

    :type col: :class:`hustle.core.marble.Column`
    :param col: the column to aggregate
    """
    import mdb

    if col.is_numeric:
        return Aggregation("max",
                           col,
                           f=lambda a, v: a if a > v else v,
                           default=lambda: -9223372036854775808,
                           result_spec=Column('_max_type', type_indicator=mdb.MDB_INT_32))
    else:
        return Aggregation("max",
                           col,
                           f=lambda a, v: a if a > v else v,
                           default=lambda: unichr(0x00))


def h_min(col):
    """
    Return an aggregation for the minimum of the given column.  Like the SQL min() function::

        select(h_min(employee.salary), employee.department, where=employee)

    returns the lowest salary in each department.

    :type col: :class:`hustle.core.marble.Column`
    :param col: the column to aggregate
    """
    import mdb

    if col.is_numeric:
        return Aggregation("min",
                           col,
                           f=lambda a, v: a if a < v else v,
                           default=lambda: 9223372036854775807,
                           result_spec=Column('_min_type', type_indicator=mdb.MDB_INT_32))
    else:
        return Aggregation("min",
                           col,
                           f=lambda a, v: a if a < v else v,
                           default=lambda: unichr(0xFFFF))


def h_avg(col):
    """
    Return an aggregation for the average of the given column.  Like the SQL avg() function::

        select(h_avg(employee.salary), employee.department, where=employee)

    returns the average salary in each department

    :type col: :class:`hustle.core.marble.Column`
    :param col: the column to aggregate
   """
    import mdb

    return Aggregation("avg",
                       col,
                       f=lambda (a, c), v: (a + v, c + 1),
                       g=lambda (a, c): float(a) / c,
                       default=lambda: (0, 0),
                       result_spec=Column('_avg_type', type_indicator=mdb.MDB_INT_32))


def star(table):
    """
    Return the list of all columns in a table.  This is used much like the ``*`` notation in SQL::

        ``select(*star(employee), where=employee.department == 'Finance')``

    returns all of the columns from the *employee* table for the Finance department.

    :type table: :class:`hustle.Table`
    :param col: the table to extract the column names from
    """
    return [table._columns[col] for col in table._field_names]


def cat(result, width=80):
    """
    Pretty print the results of a query or table.

    :type result: iterator over tuples
    :param result: result of a query or a table

    :type width: int
    :param width: the number of columns to constrain output to
    """
    alignments = None
    for columns in result:
        if not alignments:
            alignments = []
            for column in columns:
                try:
                    float(column)
                    alignments.append(_ALG_RIGHT)
                except:
                    alignments.append(_ALG_LEFT)

        _print_line(columns, width=width, cols=len(alignments),
                    alignments=alignments)


def get_tables(**kwargs):
    """
    return the visible Hustle tables in the currently configured DDFS server.  Hustle finds tables by looking
    for DDFS tags that have a *hustle:* prefix.

    :type kwargs: dict
    :param kwargs: custom settings for this query see :mod:`hustle.core.settings`
    """
    from hustle.core.settings import Settings
    settings = Settings(**kwargs)
    tags = settings["ddfs"].list(_TAG_PREFIX)
    uniqs = set()
    for tag in tags:
        l = tag.find(':')
        if l > 0:
            ctag = tag[l + 1:]
            r = ctag.find(':')
            if r > 0:
                uniqs.add(ctag[:r])
            else:
                uniqs.add(ctag)

    return sorted(uniqs)


def tables(**kwargs):
    """
    Print all available tables.

    :type kwargs: dict
    :param kwargs: custom settings for this query see :mod:`hustle.core.settings`
    """
    uniqs = get_tables(**kwargs)
    _print_line(uniqs, alignments=[_ALG_LEFT] * len(uniqs))


def schema(tab, index_only=False, **kwargs):
    """
    Print the schema for a given table

    :type kwargs: dict
    :param kwargs: custom settings for this query see :mod:`hustle.core.settings`
    """
    if not isinstance(tab, Marble):
        table = Table.from_tag(tab, **kwargs)
    else:
        table = tab
    if kwargs.get('verbose'):
        _print_title("Columns of (IX=index, PT=partition) " + table._name)

    # decorate the fields with index and partition information
    cols = sorted([c.description() for c in table._columns.values()
                   if c.index_indicator or not index_only])
    _print_line(cols, width=100, alignments=[_ALG_LEFT] * len(cols))
    if kwargs.get('verbose'):
        _print_separator(100)


def get_partitions(table, **kwargs):
    """
    Get partitions for a given table.

    :type kwargs: dict
    :param kwargs: custom settings for this query see :mod:`hustle.core.settings`
    """
    from hustle.core.settings import Settings
    settings = Settings(**kwargs)
    ddfs = settings["ddfs"]

    if isinstance(table, Marble):
        tablename = table._name
    else:
        tablename = table

    tags = ddfs.list(Table.base_tag(tablename) + ":")
    uniqs = set()
    for tag in tags:
        l = tag.find(':')
        r = tag.rfind(':')
        if r != l:
            uniqs.add(tag)
    return sorted(uniqs)


def partitions(table, **kwargs):
    """
    Print the partitions for a given table.

    :type kwargs: dict
    :param kwargs: custom settings for this query see :mod:`hustle.core.settings`
    """
    partitions = get_partitions(table)
    _print_line(partitions,
                width=132,
                alignments=[_ALG_LEFT] * len(partitions))


def _get_tags(table_or_expr, ddfs):
    # assume a table is being passed in
    table = table_or_expr
    where = None
    if hasattr(table_or_expr, 'table'):
        # nope, its a where clause (ie. and expr)
        table = table_or_expr.table
        where = table_or_expr

    basetag = table.base_tag(table._name) + ':'
    if where and table._partition:
        tags = [tag[len(basetag):] for tag in ddfs.list(basetag)]
        seltags = [table.base_tag(table._name, part)
                   for part in where.partition(tags)]
    else:
        seltags = ddfs.list(basetag)
    return seltags


def delete(table_or_expr, **kwargs):
    """
    Delete data and partitions for a given table, keep the table definition.

    :type table_or_expr: :class:`Table <hustle.Table>` | :class:`Expr <hustle.core.marble.Expr>`
    :param table_or_expr: A table object or an expression with only a partition column

    :type kwargs: dict
    :param kwargs: custom settings for this query see :mod:`hustle.core.settings`

    .. warning::
        Given a table object, all partitions will be deleted. Use a Hustle expression to delete
        a specific range of partitions, e.g. 'impression.date < 2014-01-01'.
    """
    from hustle.core.settings import Settings
    settings = Settings(**kwargs)
    ddfs = settings["ddfs"]

    if not isinstance(table_or_expr, (Expr, Table)):
        raise ValueError("The first argument must be a table or an exprssion.")

    if isinstance(table_or_expr, Expr) and not table_or_expr.is_partition:
        raise ValueError("Column in the expression must be a partition column.")

    tags = _get_tags(table_or_expr, ddfs)
    for tag in tags:
        ddfs.delete(tag)


def drop(table, **kwargs):
    """
    Drop all data, partitions, and table definition for a given table.

    :type table_or_expr: :class:`Table <hustle.Table>`
    :param table_or_expr: A table object

    :type kwargs: dict
    :param kwargs: custom settings for this query see :mod:`hustle.core.settings`
    """
    from hustle.core.settings import Settings
    settings = Settings(**kwargs)
    ddfs = settings["ddfs"]

    if not isinstance(table, Table):
        raise ValueError("Only table is allowed here.")

    delete(table, **kwargs)
    ddfs.delete(Table.base_tag(table._name))


def _query_iterator(blobs):
    from disco.core import result_iterator

    for columns, _ in result_iterator(blobs):
        yield columns


def _print_separator(width=80):
    print "- - " * (width / 4)


def _print_title(title, width=80):
    _print_separator(width)
    print title.rjust((width / 2) + len(title) / 2)
    _print_separator(width)


def _safe_str(s):
    try:
        return str(s)
    except UnicodeEncodeError:
        return unicode(s).encode("utf-8")


def _print_line(items, width=80, cols=3, alignments=None):
    i = 0
    line = ""
    if alignments is None:
        alignments = [_ALG_CENTER] * max(cols, len(items))
    for item, algn in zip(items, alignments):
        if type(item) in (int, long):
            item = "{:,}".format(item)
        else:
            item = _safe_str(item)

        if algn == _ALG_RIGHT:
            item = item.rjust(width / cols)
        elif algn == _ALG_LEFT:
            item = item.ljust(width / cols)
        else:
            item = item.center(width / cols)

        if i < cols:
            line = "%s %s" % (line, item) if line else item
            i += 1
        else:
            print line
            i, line = 1, item
    print line


def _get_blobs(table_or_expr, ddfs, limit=sys.maxint):
    # assume a table is being passed in
    table = table_or_expr
    where = None
    if hasattr(table_or_expr, 'table'):
        # nope, its a where clause (ie. an expr)
        table = table_or_expr.table
        where = table_or_expr

    if table._blobs:
        return table._blobs
    elif where and table._partition:
        # collect the blobs
        basetag = table.base_tag(table._name) + ':'
        tags = [tag[len(basetag):] for tag in ddfs.list(basetag)]
        seltags = [table.base_tag(table._name, part)
                   for part in where.partition(tags)]
        rval = list()
        for tag in seltags:
            rval.extend(ddfs.blobs(tag))
            if len(rval) >= limit:
                return rval[:limit]
        return rval
    else:
        tags = ddfs.list(table.base_tag(table._name))
        blobs = []
        for tag in tags:
            replicas = list(ddfs.blobs(tag))
            blobs.extend(replicas)
            if len(blobs) >= limit:
                return blobs[:limit]
        return blobs


def _resolve_join(wheres, joins):
    if not joins:
        return joins
    elif isinstance(joins, (basestring, unicode)):
        # A single string indicates two columns with the same name
        joins = [joins, joins]

    if len(wheres) != 2 and len(joins) != 2:
        raise ValueError("Must have exact two items in the where/join clause.")

    join_cols = []
    for join, where in zip(joins, wheres):
        if isinstance(join, Column):
            join_cols.append(join)
            continue
        if isinstance(where, Marble):
            table = where
        elif isinstance(where, Expr):
            table = where.table
        else:
            raise ValueError("Invalid item in the where clause,"
                             "expect a table or an expression.")
        try:
            col = table._columns[join]
            join_cols.append(col)
        except KeyError:
            raise ValueError("Table %s doesn't have column %s."
                             % (table._name, join))
    return join_cols


class Future(object):
    """
    Return value of non-blocking function :func:`select_nb() <hustle.select_nb>`.

    It has a series of functions to check query's status and fetch query's results.
    """
    def __init__(self, name, job, disco, nest, *project):
        if not isinstance(job, Job):
            raise ValueError("Invalid argument, expect a Disco job.")
        if not isinstance(disco, Disco):
            raise ValueError("Invalid argument, expect a Disco server.")
        self.name = name
        self._job = job
        self._disco = disco
        self._blobs = None
        self._project = project
        self._status = None
        self._table = None
        self._nest = nest

    def status(self):
        """
        Return the status of the attached query.

        The status of a query might be one of following strings:
            'dead': query is dead
            'active': query is still being executed
            'ready': query is finished, all results are available
            'unknown': status is unknown, e.g. connection to server is busy
        """
        if self._status == 'ready':
            return self._status

        try:
            status, result = self._disco.results(self._job, 500)
        except CommError:
            # to deal with the long time jobs(e.g waiting for shuffling)
            return 'unknown'
        else:
            if status == 'ready':
                self._status = status
                self._blobs = result
            return status

    def wait(self):
        """
        Block and wait the query to finish. If the query is already finished,
        it returns the result at once.

        The return value is the same as :func:`select() <hustle.select>`.
        If query is nested, it returns a table. Otherwise, a list of urls.
        """
        if self.done:
            if self._nest:
                return self.table
            else:
                return self._blobs
        else:
            self._blobs = self._job.wait()
            self._status = 'ready'
            if self._nest:
                return self.table
            else:
                return _query_iterator(self._blobs)

    @property
    def done(self):
        """
        A helper function that shows whether query is done or not

        """
        return self.status() == 'ready' and self._blobs is not None

    @property
    def table(self):
        """
        A helper function for the nested query to return its table result.

        If query is not nested or query is not finished, an exception will be raised.
        """
        if not self.done:
            raise Exception("Query isn't finished yet. Can not create table now.")
        if not self._nest:
            raise TypeError("Query is not a nested query. Did you forgot to specify 'nest = True'?")
        if self._table is not None:
            return self._table

        self._table = self._job.get_result_schema(self._project)
        self._table._blobs = self._blobs
        return self._table
