"""
:mod:`hustle` -- Hustle Distributed OLAP Query Engine
=====================================================

The :mod:`hustle` module contains everything a client application will
typically need to create hustle :term:`Tables` and to insert and query
data to/from them.

"""
__version__ = '0.1.1'

import os
import ujson
from hustle.core.marble import Aggregation, Marble, json_decoder, Column, check_query


_TAG_PREFIX = 'hustle:'
_ALG_RIGHT, _ALG_LEFT, _ALG_CENTER = 0x01, 0x10, 0x20


def print_seperator(width=80):
    print "- - " * (width / 4)


def print_title(title, width=80):
    print_seperator(width)
    print title.rjust((width / 2) + len(title) / 2)
    print_seperator(width)


def safe_str(s):
    try:
        return str(s)
    except UnicodeEncodeError:
        return unicode(s).encode("utf-8")


def print_line(items, width=80, cols=3, alignments=None):
    i = 0
    line = ""
    if alignments is None:
        alignments = [_ALG_CENTER] * max(cols, len(items))
    for item, algn in zip(items, alignments):
        if type(item) in (int, long):
            item = "{:,}".format(item)
        else:
            item = safe_str(item)

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


def h_sum(col):
    """
    Return an aggregation for the sum of the given column
    """
    return Aggregation("sum", col, lambda a, v: a + v, default=lambda: 0)


def h_count():
    return Aggregation("count",
                       Column('all', None, type_indicator=1),
                       lambda a, v: a + (v or 1),
                       default=lambda: 0)


def h_max(col):
    """
    Return an aggregation for the maximum of the given column
    """
    return Aggregation("max", col, lambda a, v: a if a > v else v, default=lambda: -9223372036854775808)


def h_min(col):
    """
    Return an aggregation for the minimum of the given column
    """
    return Aggregation("min", col, lambda a, v: a if a < v else v, default=lambda: 9223372036854775807)


def h_avg(col):
    """
    Return an aggregation for the average of the given column
    """
    return Aggregation("avg", col, lambda (a, c), v: (a + v, c + 1), lambda (a, c): float(a) / c, default=lambda: (0, 0))


class Table(Marble):
    def __init__(self, **kwargs):
        super(Table, self).__init__(**kwargs)
        self._blobs = []

    def _save(self, name, merge=False, **kwargs):
        """saving a table is typically for persisting results of queries.  Normally (merge=False), the
        operation will fail if the table already exists."""
        from hustle.core.settings import Settings
        settings = Settings(**kwargs)
        ddfs = settings['ddfs']
        self._name = name

        if self._blobs:
            new_table = create(name, self._fields, ddfs)
            if new_table or merge:
                ddfs.tag(self.base_tag(name), self._blobs)
                return True
        raise Exception("Couldn't save table %s.  Table arleady exists, or has no data." % name)

    @classmethod
    def base_tag(cls, name, partition=None, extension=''):
        rval = "hustle:" + name
        if extension:
            rval += ':' + extension
        if partition:
            rval += ':' + str(partition)
        return rval

    @classmethod
    def from_tag(cls, name, **kwargs):
        from hustle.core.settings import Settings
        settings = Settings(**kwargs)
        ddfs = settings['ddfs']

        partition = ujson.loads(ddfs.getattr(cls.base_tag(name), '_partition_'))
        fields = ujson.loads(ddfs.getattr(cls.base_tag(name), '_fields_'))
        return cls(name=name, fields=fields, partition=partition)


def _get_blobs(table_or_expr, ddfs):
    # assume a table is being passed in
    table = table_or_expr
    where = None
    if hasattr(table_or_expr, 'table'):
        # nope, its a where clause (ie. and expr)
        table = table_or_expr.table
        where = table_or_expr

    if table._blobs:
        return table._blobs
    elif where and table._partition:
        # collect the blobs
        basetag = table.base_tag(table._name) + ':'
        tags = [tag[len(basetag):] for tag in ddfs.list(basetag)]
        seltags = [table.base_tag(table._name, part) for part in where.partition(tags)]
        rval = list()
        for tag in seltags:
            rval.extend(ddfs.blobs(tag))
        return rval
    else:
        tags = ddfs.list(table.base_tag(table._name))
        blobs = []
        for tag in tags:
            replicas = list(ddfs.blobs(tag))
            blobs.extend(replicas)
        return blobs


def star(table):
    return [table._columns[col] for col in table._field_names]


def create(name, fields=(), partition=None, force=False, **kwargs):
    from hustle.core.settings import Settings
    settings = Settings(**kwargs)
    ddfs = settings['ddfs']

    if ddfs.exists(Table.base_tag(name)):
        print "Table already exists..."
        if force:
            print "   Overwriting schema..."
        else:
            return None

    ddfs.setattr(Table.base_tag(name), '_fields_', ujson.dumps(fields))
    ddfs.setattr(Table.base_tag(name), '_partition_', ujson.dumps(partition))
    return Table(name=name, fields=fields, partition=partition)


def extract_fields(raw_file, excludes=(), strings=(), additional=(), uncompressed=()):
    """extract the list of fields found in the first line of a json file. This can be used as the 3rd arg of create()"""
    rkeys = []
    excludes = set(excludes)
    strings = set(strings)
    with open(raw_file) as fd:
        data = ujson.loads(fd.readline())
        keys = data.keys() + list(additional)
        for key in keys:
            if key not in excludes:
                if key not in strings:
                    if key in data and type(data[key]) in (long, int):
                        key = '#' + key
                    elif key in uncompressed:
                        key = '$' + key
                rkeys.append(key)
    return rkeys


def insert(table, file=None, streams=None, preprocess=None,
           maxsize=100 * 1024 * 1024, tmpdir='/tmp', decoder=json_decoder,
           lru_size=10000, **kwargs):
    from hustle.core.settings import Settings
    settings = Settings(**kwargs)
    ddfs = settings['ddfs']
    # print 'committed'

    def part_tag(name, partition=None):
        rval = "hustle:" + name
        if partition:
            rval += ':' + str(partition)
        return rval
    if file:
        streams = [open(file)]
    lines, partition_files = table._insert(streams, preprocess=preprocess,
                                           maxsize=maxsize, tmpdir=tmpdir,
                                           decoder=decoder, lru_size=lru_size)
    if partition_files is not None:
        for part, pfile in partition_files.iteritems():
            tag = part_tag(table._name, part)
            ddfs.push(tag, [pfile])
            print 'pushed %s, %s to %s' % (part, tag, ddfs)
            os.unlink(pfile)
    return table._name, lines


def dump(result_urls, width=80):
    """
    Dump the results of a query or a table.

    Arguments:
    result_urls - result of a project/aggregate query
    width - the number of columns to constrain output to
    """
    from disco.core import result_iterator
    alignments = None
    for columns, _ in result_iterator(result_urls):
        if not alignments:
            alignments = []
            for column in columns:
                try:
                    float(column)
                    alignments.append(_ALG_RIGHT)
                except:
                    alignments.append(_ALG_LEFT)

        print_line(columns, width=width, cols=len(alignments),
                   alignments=alignments)


def select(*project, **kwargs):
    """
    Perform a relational query, by selecting rows and columns from one or more tables.

    Arguments:
    project - a positional argument list of columns and aggregate expressions to return in the result
    where - a  list of tables and/or column expressions to restrict the rows of the relations
    join - a list of columns to perform a relational join on
    order_by - a list of columns to order the output by
    distinct - a boolean indicating whether to remove duplicates
    limit - a number or a tuple(offset, total_number) to limit the number of output

    Example:
    # simple projection - note the where expression syntax
    select(
        impressions.timestamp, impressions.userid, impressions.time_on_site,
        where=(impressions.date > '2013-11-01') & (impressions.site == 'metalmusicmachine.com'))

    # simple aggregation - note that a grouping by employee.name is implied
    #   also note that where clauses can have entire tables
    select(
        employee.name, h_sum(employee.salary),
        where=employee,
        order_by=employee.name)

    # aggregation with joins, ordering and limits
    #   - note the where and join clauses must each contain exactly two tables
    select(
        employee.name, department.name, h_sum(employee.salary), h_sum(department.population),
        where=((employee.age > 27) & (employee.sex == 'male'), department),
        join=(employee.department_id, department.id),
        order_by=(department.name, 'sum(salary)'),
        desc=True)"""

    from hustle import _get_blobs
    from hustle.core.settings import Settings
    from hustle.core.pipeline import SelectPipe
    from hustle.core.util import ensure_list

    settings = Settings(**kwargs)
    wheres = ensure_list(settings.pop('where', ()))
    order_by = ensure_list(settings.pop('order_by', ()))
    join = settings.pop('join', ())
    distinct = settings.pop('distinct', False)
    desc = settings.pop('desc', False)
    limit = settings.pop('limit', None)
    ddfs = settings['ddfs']
    autodump = settings['dump']
    partition = settings.get('partition', 0)
    if partition < 0:
        partition = 0
    nest = settings.get('nest', False)
    try:
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
                     distinct=distinct,
                     desc=desc,
                     limit=limit,
                     partition=partition,
                     nest=nest)

    job.run(name='select_from_%s' % name, input=job_blobs, **settings)
    blobs = job.wait()
    if nest:
        rtab = job.get_result_schema(project)
        rtab._blobs = blobs
        return rtab
    elif autodump:
        # the result will be just dumped to stdout
        cols = [c.name for c in project]
        print_seperator(80)
        print_line(cols, width=80, cols=len(cols),
                   alignments=[_ALG_RIGHT if c.is_numeric else _ALG_LEFT for c in project])
        print_seperator(80)
        dump(blobs, 80)
        return
    else:
        return blobs


def get_tables(**kwargs):
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
    """Print all available tables."""
    uniqs = get_tables(**kwargs)
    print_line(uniqs)


def schema(tab, index_only=False, **kwargs):
    """Print the schema for a given table"""
    if not isinstance(tab, Marble):
        table = Table.from_tag(tab, **kwargs)
    else:
        table = tab
    if kwargs.get('verbose'):
        print_title("Columns of (IX=index, PT=partition) " + table._name)

    # decorate the fields with index and partition information
    cols = sorted([c.description() for c in table._columns.values()
                   if c.index_indicator or not index_only])
    print_line(cols, width=100, alignments=[_ALG_LEFT] * len(cols))
    if kwargs.get('verbose'):
        print_seperator(100)


def partitions(table, **kwargs):
    """Print the partitions for a given table."""
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
    print_line(sorted(uniqs), width=132)
