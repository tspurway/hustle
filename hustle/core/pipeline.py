from disco.core import Job
from disco.worker.task_io import task_input_stream
import hustle
import hustle.core
import hustle.core.marble
from hustle.core.marble import Marble, Column, Aggregation
from functools import partial
from hustle.core.pipeworker import HustleStage
import sys

SPLIT = "split"
GROUP_ALL = "group_all"
GROUP_LABEL = "group_label"
GROUP_LABEL_NODE = "group_node_label"
GROUP_NODE = "group_node"

# default number of partitions, users can set this in the settings.yaml
_NPART = 16


def hustle_output_stream(stream, partition, url, params, result_table):
    class HustleOutputStream(object):
        def __init__(self, stream, url, params, **kwargs):
            import tempfile
            from wtrie import Trie

            self.result_table = result_table
            self.result_columns = result_table._field_names
            tmpdir = getattr(params, 'tmpdir', '/tmp')
            self.filename = tempfile.mktemp(prefix="hustle", dir=tmpdir)
            maxsize = getattr(params, 'maxsize', 100 * 1024 * 1024)
            self.env, self.txn, self.dbs, self.meta = self.result_table._open(self.filename, maxsize, write=True, lru_size=10000)
            self.autoinc = 1
            self.url = url
            self.vid_trie = Trie()
            self.vid16_trie = Trie()

        def add(self, k, v):
            from hustle.core.marble import _insert_row
            data = dict(zip(self.result_columns, list(k) + list(v)))
            #print "BOZAK! adding %s %s %s" % (self.result_columns, k, v)
            _insert_row(data,
                        self.txn,
                        self.dbs,
                        self.autoinc,
                        self.vid_trie,
                        self.vid16_trie)
            self.autoinc += 1

        def close(self):
            import os
            import ujson

            self.meta.put(self.txn, '_total_rows', str(self.autoinc))
            vid_nodes, vid_kids, _ = self.vid_trie.serialize()
            vid16_nodes, vid16_kids, _ = self.vid16_trie.serialize()
            vn_ptr, vn_len = vid_nodes.buffer_info()
            vk_ptr, vk_len = vid_kids.buffer_info()
            vn16_ptr, vn16_len = vid16_nodes.buffer_info()
            vk16_ptr, vk16_len = vid16_kids.buffer_info()
            self.meta.put_raw(self.txn, '_vid_nodes', vn_ptr, vn_len)
            self.meta.put_raw(self.txn, '_vid_kids', vk_ptr, vk_len)
            self.meta.put_raw(self.txn, '_vid16_nodes', vn16_ptr, vn16_len)
            self.meta.put_raw(self.txn, '_vid16_kids', vk16_ptr, vk16_len)
            self.meta.put(self.txn, 'name', ujson.dumps(self.result_table._name))
            self.meta.put(self.txn, 'fields', ujson.dumps(self.result_table._fields))
            self.meta.put(self.txn, 'partition', ujson.dumps(self.result_table._partition))
            for index, (subdb, subindexdb, bitmap_dict, column) in self.dbs.iteritems():
                if subindexdb:
                    # process all values for this bitmap index
                    if column.index_indicator == 2:
                        bitmap_dict.evictAll()
                    else:
                        for val, bitmap in bitmap_dict.iteritems():
                            subindexdb.put(self.txn, val, bitmap.dumps())
            self.txn.commit()

            try:
                self.env.copy(self.url)
                print "Dumped result to %s" % self.url
            except Exception as e:
                print "Copy error: %s" % e
                self.txn.abort()
                raise e
            self.env.close()
            os.unlink(self.filename)
            os.unlink(self.filename + '-lock')

    return HustleOutputStream(stream, url, params)


def hustle_input_stream(fd, size, url, params, wheres, gen_where_index, key_names):
    from disco import util
    from hustle.core.marble import Expr, MarbleStream
    empty = ()

    try:
        scheme, netloc, rest = util.urlsplit(url)
    except Exception as e:
        print "Error handling hustle_input_stream for %s. %s" % (url, e)
        raise e

    fle = util.localize(rest, disco_data=params._task.disco_data, ddfs_data=params._task.ddfs_data)
    # print "FLOGLE: %s %s" % (url, fle)

    otab = None
    try:
        # import sys
        # sys.path.append('/Library/Python/2.7/site-packages/pycharm-debug.egg')
        # import pydevd
        # pydevd.settrace('localhost', port=12999, stdoutToServer=True, stderrToServer=True)
        otab = MarbleStream(fle)
        bitmaps = {}
        for index, where in enumerate(wheres):
            # do not process where clauses that have nothing to do with this marble
            if where._name == otab.marble._name:
                if type(where) is Expr and not where.is_partition:
                    bitmaps[index] = where(otab)
                else:
                    # it is either the table itself, or a partition expression.  either way,
                    # return the entire table
                    bitmaps[index] = otab.iter_all()

        for index, bitmap in bitmaps.iteritems():
            prefix = [index] if gen_where_index else []
            for row_id in bitmap:
                record = [otab.get(col, row_id) if col else None for col in key_names[index]]
                # print "Gibbled: %s" % repr(record)
                record[0:0] = prefix  # this looks odd, but is faster than 'prefix + record'
                yield tuple(record), empty
    finally:
        if otab:
            otab.close()


class SelectPipe(Job):
    # profile = True
    required_modules = [
        ('hustle', hustle.__file__),
        ('hustle.core', hustle.core.__file__),
        ('hustle.core.pipeline', __file__),
        ('hustle.core.marble', hustle.core.marble.__file__)]

    def get_result_schema(self, project):
        from hustle import Table
        fields = []
        for col in project:
            col = col.column
            if col.name not in fields:
                fields.append(col.schema_string())
        # print "GEEWHIZ: %s %s %s" % (indicies, fields, blobs)
        name = '-'.join([w._name for w in self.wheres])[:64]
        return Table(name="sub-%s" % name,
                     fields=fields)

    def _get_table(self, obj):
        """If obj is a table return its name otherwise figure out what it is and return the tablename"""
        if isinstance(obj, Marble):
            return obj
        else:
            return obj.table

    def _resolve(self, cols, check, types=(Column, Aggregation)):
        rval = []
        for i, col in enumerate(cols):
            if isinstance(col, types):
                rval.append(col)
            elif isinstance(col, basestring):
                selectcol = next((c for c in check if c.name == col or c.fullname == col), None)
                if selectcol:
                    rval.append(selectcol)
            elif isinstance(col, int):
                if col < len(check):
                    rval.append(check[col])
        return rval

    def _get_key_names(self, project, join):
        result = []
        for where in self.wheres:
            table_name = self._get_table(where)._name
            rval = []
            if join:
                join_column = next(c.name for c in join if c.table._name == table_name)
                rval.append(join_column)
            rval += tuple(c.column.name if c.table and c.table._name == table_name else None for c in project)
            result.append(rval)
        return result

    def __init__(self,
                 master,
                 wheres,
                 project=(),
                 order_by=(),
                 join=(),
                 distinct=False,
                 desc=False,
                 limit=0,
                 partition=0,
                 nest=False,
                 pre_order_stage=()):
        from hustle.core.pipeworker import Worker

        super(SelectPipe, self).__init__(master=master, worker=Worker())
        self.wheres = wheres
        self.order_by = self._resolve(order_by, project)
        # print "whohah: %s" % repr(self.order_by)
        partition = partition or _NPART
        binaries = [i for i, c in enumerate(project) if isinstance(c, (Column, Aggregation)) and c.is_binary]
        # print "BINS: %s" % repr(binaries)

        # build the pipeline
        select_hash_cols = ()
        sort_range = _get_sort_range(0, project, self.order_by)
        join_stage = []
        if join:
            joinbins = [i + 2 for i in binaries]
            join_stage = [
                (GROUP_LABEL, HustleStage('join',
                                          sort=(1, 0),
                                          binaries=joinbins,
                                          process=partial(process_join,
                                                          label_fn=partial(_tuple_hash,
                                                                           cols=sort_range,
                                                                           p=partition))))]
            select_hash_cols = (1,)

        efs, gees, ehches, dflts = zip(*[(c.f, c.g, c.h, c.default)
                                         if isinstance(c, Aggregation) else (None, None, None, None)
                                         for c in project])
        group_by_stage = []
        if any(efs):
            # If all columns in project are aggregations, use process_skip_group
            # to skip the internal groupby
            if all([isinstance(c, Aggregation) for c in project]):
                process_group_fn = process_skip_group
                group_by_range = []
            else:
                process_group_fn = process_group
                group_by_range = [i for i, c in enumerate(project) if isinstance(c, Column)]

            # build the pipeline
            group_by_stage = [
                (GROUP_LABEL_NODE, HustleStage('group-combine',
                                               sort=group_by_range,
                                               binaries=binaries,
                                               process=partial(process_group_fn,
                                                               ffuncs=efs,
                                                               ghfuncs=ehches,
                                                               deffuncs=dflts,
                                                               label_fn=partial(_tuple_hash,
                                                                                cols=group_by_range,
                                                                                p=partition)))),
                (GROUP_LABEL, HustleStage('group-reduce',
                                          input_sorted=True,
                                          combine=True,
                                          sort=group_by_range,
                                          process=partial(process_group_fn,
                                                          ffuncs=efs,
                                                          ghfuncs=gees,
                                                          deffuncs=dflts)))]

        # process the order_by/distinct stage
        order_stage = []
        if self.order_by or distinct or limit:
            order_stage = [
                (GROUP_LABEL_NODE, HustleStage('order-combine',
                                               sort=sort_range,
                                               binaries=binaries,
                                               desc=desc,
                                               process=partial(process_order,
                                                               distinct=distinct,
                                                               limit=limit or sys.maxint))),
                (GROUP_ALL, HustleStage('order-reduce',
                                        sort=sort_range,
                                        desc=desc,
                                        input_sorted=True,
                                        combine_labels=True,
                                        process=partial(process_order,
                                                        distinct=distinct,
                                                        limit=limit or sys.maxint))),
            ]

        if not select_hash_cols:
            select_hash_cols = sort_range

        pipeline = [(SPLIT, HustleStage('restrict-select',
                                        process=partial(process_restrict,
                                                        label_fn=partial(_tuple_hash,
                                                                         cols=select_hash_cols,
                                                                         p=partition)),
                                        input_chain=[task_input_stream,
                                                     partial(hustle_input_stream,
                                                             wheres=wheres,
                                                             gen_where_index=join,
                                                             key_names=self._get_key_names(project, join))]))
                    ] + join_stage + group_by_stage + list(pre_order_stage) + order_stage

        # determine the style of output (ie. if it is a Hustle Table), and modify the last stage accordingly
        if nest:
            pipeline[-1][1].output_chain = [partial(hustle_output_stream, result_table=self.get_result_schema(project))]
        self.pipeline = pipeline


def _tuple_hash(key, cols, p):
    r = 0
    for c in cols:
        r ^= hash(key[c])
    return r % p


def process_restrict(interface, state, label, inp, task, label_fn):
    from disco import util

    # inp contains a set of replicas, let's force local #HACK
    input_processed = False
    for i, inp_url in inp.input.replicas:
        scheme, (netloc, port), rest = util.urlsplit(inp_url)
        if netloc == task.host:
            input_processed = True
            inp.input = inp_url
            break

    if not input_processed:
        raise Exception("Input %s not processed, no LOCAL resource found." % str(inp.input))

    for key, value in inp:
        out_label = label_fn(key)
        # print "RESTRICT: %s %s" % (key, value)
        interface.output(out_label).add(key, value)


def process_join(interface, state, label, inp, task, label_fn):
    '''Processor function for the join stage.

    Note that each key in the 'inp' is orgnized as:
        key = (where_index, join_column, other_columns)

    Firstly, all keys are divided into different groups based on the join_column.
    Then the where_index is used to separate keys from different where clauses.
    Finally, merging columns together.
    '''
    from itertools import groupby

    def _merge_record(offset, r1, r2):
        return [i if i is not None else j for i, j in zip(r1[offset:], r2[offset:])]

    # inp is a list of (key, value) tuples, the join_cloumn is the 2nd item of the key.
    for joinkey, rest in groupby(inp, lambda k: k[0][1]):
        # To process this join key, we must have values from both tables
        first_table = []
        for record, value in rest:
            # Grab all records from first table by using where index
            if record[0] == 0:
                first_table.append(record)
            else:
                if not len(first_table):
                    break
                # merge each record from table 2 with all records from table 1
                for first_record in first_table:
                    # dispose of the where_index and join column
                    newrecord = _merge_record(2, first_record, record)
                    newlabel = label_fn(newrecord)
                    # print "JOIN: %s %s %s" % (newrecord, first_record, record)
                    interface.output(newlabel).add(newrecord, value)


def process_order(interface, state, label, inp, task, distinct, limit):
    from itertools import groupby, islice
    empty = ()
    if distinct:
        for uniqkey, _ in islice(groupby(inp, lambda (k, v): tuple(k)), 0, limit):
            # print "ORDERED %s" % repr(uniqkey)
            interface.output(label).add(uniqkey, empty)
    else:
        for key, value in islice(inp, 0, limit):
            # print "ORDERED %s" % repr(key)
            interface.output(label).add(key, value)


def process_group(interface, state, label, inp, task, ffuncs, ghfuncs, deffuncs, label_fn=None):
    """Process function of aggregation combine stage.

    """
    from itertools import groupby
    import copy

    empty = ()

    # import sys
    # sys.path.append('/Library/Python/2.7/site-packages/pycharm-debug.egg')
    # import pydevd
    # pydevd.settrace('localhost', port=12999, stdoutToServer=True, stderrToServer=True)

    baseaccums = [default() if default else None for default in deffuncs]
    # print "Base: %s" % repr(baseaccums)

    # pull the key apart
    for group, tups in groupby(inp, lambda (k, _): tuple([e if ef is None else None for e, ef in zip(k, ffuncs)])):
        accums = copy.copy(baseaccums)
        for record, _ in tups:
            # print "REC: %s" % repr(record)
            try:
                accums = [f(a, v) if f and a is not None else None
                          for f, a, v in zip(ffuncs, accums, record)]
            except Exception as e:
                print e
                print "YOLO: f=%s a=%s r=%s g=%s" % (ffuncs, accums, record, group)
                raise e

        accum = [h(a) if h else None for h, a in zip(ghfuncs, accums)]
        if label_fn:
            label = label_fn(group)
        key = tuple(g or a for g, a in zip(group, accum))
        # print "KEY: %s" % repr(key)
        interface.output(label).add(key, empty)


def process_skip_group(interface, state, label, inp, task, ffuncs, ghfuncs, deffuncs, label_fn=None):
    """Process function of aggregation combine stage without groupby.
    """
    empty = ()
    accums = [default() if default else None for default in deffuncs]
    for record, _ in inp:
        try:
            accums = [f(a, v) if f and a is not None else None
                      for f, a, v in zip(ffuncs, accums, record)]
        except Exception as e:
            raise e

    accum = [h(a) if h else None for h, a in zip(ghfuncs, accums)]
    interface.output(0).add(tuple(accum), empty)


def _get_sort_range(select_offset, select_columns, order_by_columns):
    # sort by all
    sort_range = [i + select_offset for i, c in enumerate(select_columns) if isinstance(c, Column) and not c.is_binary]
    if order_by_columns:
        scols = ["%s%s" % (c.table._name if c.table else '', c.name) for c in select_columns]
        ocols = ["%s%s" % (c.table._name if c.table else '', c.name) for c in order_by_columns]
        rcols = set(scols) - set(ocols)
        # make sure to include the columns *not* in the order_by expression as well
        # this is to ensure that 'distinct' will work
        sort_range = tuple(select_offset + scols.index(c) for c in ocols) +\
            tuple(select_offset + scols.index(c) for c in rcols)
    return sort_range
