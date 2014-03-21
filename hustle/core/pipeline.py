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
    """
    A disco output stream for creating a Hustle :class:`hustle.Table` output from a stage.
    """
    class HustleOutputStream(object):
        def __init__(self, stream, url, params, **kwargs):
            import tempfile
            from wtrie import Trie

            self.result_table = result_table
            self.result_columns = result_table._field_names
            tmpdir = getattr(params, 'tmpdir', '/tmp')
            self.filename = tempfile.mktemp(prefix="hustle", dir=tmpdir)
            maxsize = getattr(params, 'maxsize', 100 * 1024 * 1024)
            self.env, self.txn, self.dbs, self.meta = self.result_table._open(self.filename,
                                                                              maxsize,
                                                                              write=True,
                                                                              lru_size=10000)
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

    return HustleOutputStream(stream, url, params)


def hustle_input_stream(fd, size, url, params, wheres, gen_where_index, key_names, default_values=None):
    from disco import util
    from hustle.core.marble import Expr, MarbleStream
    from itertools import izip, repeat
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

        if default_values is None:
            default_values = tuple(repeat(None, ))
        for index, where in enumerate(wheres):
            # do not process where clauses that have nothing to do with this marble
            if where._name == otab.marble._name:
                if type(where) is Expr and not where.is_partition:
                    bm = where(otab)
                    bitmaps[index] = (bm, len(bm))
                else:
                    # it is either the table itself, or a partition expression.  either way,
                    # return the entire table
                    bitmaps[index] = (otab.iter_all(), otab.number_rows)

        for index, (bitmap, blen) in bitmaps.iteritems():
            prefix_gen = [repeat(index, blen)] if gen_where_index else []

            row_iter = prefix_gen + [otab.mget(col, bitmap) if col is not None else repeat(def_val, blen)
                                     for def_val, col in zip(default_values[index], key_names[index])]

            for row in izip(*row_iter):
                yield row, empty

    finally:
        if otab:
            otab.close()


def hustle_combine_input_stream(fd, size, url, params):
    pass


class SelectPipe(Job):
    # profile = True
    required_modules = [
        ('hustle', hustle.__file__),
        ('hustle.core', hustle.core.__file__),
        ('hustle.core.pipeline', __file__),
        ('hustle.core.marble', hustle.core.marble.__file__)]

    def get_result_schema(self, project):
        import random
        from hustle import Table

        if self.output_table:
            return self.output_table
        fields = []
        for col_spec in project:
            col = col_spec.column
            if col.name not in fields:
                fields.append(col.schema_string())
        name = '-'.join([w._name for w in self.wheres])[:64]
        # append a 3-digit random suffix to avoid name collision
        self.output_table = Table(name="sub-%s-%03d" % (name, random.randint(0, 999)),
                                  fields=fields)
        return self.output_table

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
        key_names = []
        default_values = []
        for where in self.wheres:
            table_name = self._get_table(where)._name
            keys = []
            defs = []
            if join:
                join_column = next(c.name for c in join if c.table._name == table_name)
                keys.append(join_column)
                defs.append(None)
            keys += tuple(c.column.name if c.table and c.table._name == table_name else None for c in project)
            defs += tuple(c.default() if type(c) is Aggregation and c.table is None else None for c in project)
            key_names.append(keys)
            default_values.append(defs)
        return key_names, default_values

    def __init__(self,
                 master,
                 wheres,
                 project=(),
                 order_by=(),
                 join=(),
                 full_join=False,
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
        partition = partition or _NPART
        binaries = [i for i, c in enumerate(project) if isinstance(c, (Column, Aggregation)) and c.is_binary]
        # if nest is true, use output_schema to store the output table
        self.output_table = None

        # build the pipeline
        select_hash_cols = ()
        sort_range = _get_sort_range(0, project, self.order_by)
        join_stage = []
        if join or full_join:
            joinbins = [i + 2 for i in binaries]
            join_stage = [
                (GROUP_LABEL, HustleStage('join',
                                          sort=(1, 0),
                                          binaries=joinbins,
                                          process=partial(process_join,
                                                          full_join=full_join,
                                                          label_fn=partial(_tuple_hash,
                                                                           cols=sort_range,
                                                                           p=partition))))]
            select_hash_cols = (1,)

        efs, gees, ehches, dflts = zip(*[(c.f, c.g, c.h, c.default)
                                         if isinstance(c, Aggregation) else (None, None, None, None)
                                         for c in project])
        group_by_stage = []
        if all([isinstance(c, Aggregation) for c in project]):
            process_group_fn = _skip
            group_by_range = []
        else:
            process_group_fn = _group
            group_by_range = [i for i, c in enumerate(project) if isinstance(c, Column)]
        if any(efs):
            # If all columns in project are aggregations, use process_skip_group
            # to skip the internal groupby

            # build the pipeline
            group_by_stage = [
                (GROUP_LABEL_NODE, HustleStage('group-combine',
                                               sort=group_by_range,
                                               binaries=binaries,
                                               process=partial(process_group,
                                                               ffuncs=efs,
                                                               ghfuncs=ehches,
                                                               deffuncs=dflts,
                                                               group_fn=process_group_fn,
                                                               label_fn=partial(_tuple_hash,
                                                                                cols=group_by_range,
                                                                                p=partition)))),
                # A Hack here that overrides disco stage's default option 'combine'.
                # Hustle needs all inputs with the same label to be combined.
                (GROUP_LABEL, HustleStage('group-reduce',
                                          input_sorted=True,
                                          combine=True,
                                          sort=group_by_range,
                                          process=partial(process_group,
                                                          ffuncs=efs,
                                                          ghfuncs=gees,
                                                          deffuncs=dflts,
                                                          group_fn=process_group_fn)))]

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

        key_names, default_values = self._get_key_names(project, join)

        pipeline = [(SPLIT, HustleStage('restrict-select',
                                        process=partial(process_restrict,
                                                        ffuncs=efs,
                                                        ghfuncs=ehches,
                                                        deffuncs=dflts,
                                                        group_fn=process_group_fn,
                                                        label_fn=partial(_tuple_hash,
                                                                         cols=select_hash_cols,
                                                                         p=partition)),
                                        input_chain=[task_input_stream,
                                                     partial(hustle_input_stream,
                                                             wheres=wheres,
                                                             gen_where_index=join or full_join,
                                                             key_names=key_names,
                                                             default_values=default_values)]))
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


def process_restrict(interface, state, label, inp, task, label_fn, ffuncs, ghfuncs, deffuncs, group_fn):
    from disco import util
    empty = ()

    # import sys
    # sys.path.append('/Library/Python/2.7/site-packages/pycharm-debug.egg')
    # import pydevd
    # pydevd.settrace('localhost', port=12999, stdoutToServer=True, stderrToServer=True)
    #

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

    if any(ffuncs):
        import copy
        vals = {}
        baseaccums = [default() if default else None for default in deffuncs]
        for rec, value in inp:
            key = tuple([e if ef is None else None for e, ef in zip(rec, ffuncs)])
            if key not in vals:
                vals[key] = copy.copy(baseaccums)
                accums = vals[key]
            else:
                accums = vals[key]

            vals[key] = [f(a, v) if None not in (f, a, v) else None
                      for f, a, v in zip(ffuncs, accums, rec)]

        for key, accums in vals.iteritems():
            accum = [h(a) if None not in (h, a) else None for h, a in zip(ghfuncs, accums)]
            yield tuple(g if g is not None else a for g, a in zip(key, accum)), empty

    else:
        for key, value in inp:
            out_label = label_fn(key)
            # print "RESTRICT: %s %s" % (key, value)
            interface.output(out_label).add(key, value)


def process_join(interface, state, label, inp, task, full_join, label_fn):
    """
    Processor function for the join stage.

    Note that each key in the 'inp' is orgnized as:
        key = (where_index, join_column, other_columns)

    Firstly, all keys are divided into different groups based on the join_column.
    Then the where_index is used to separate keys from different where clauses.
    Finally, merging columns together.
    """
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


def process_group(interface, state, label, inp, task, ffuncs, ghfuncs, deffuncs, group_fn, label_fn=None):
    empty = ()
    for lab, key in group_fn(inp, label, ffuncs, ghfuncs, deffuncs, label_fn):
        interface.output(lab).add(key, empty)


def _group(inp, label, ffuncs, ghfuncs, deffuncs, label_fn=None):
    """
    Process function of aggregation combine stage.
    """
    from itertools import groupby
    import copy

    # import sys
    # sys.path.append('/Library/Python/2.7/site-packages/pycharm-debug.egg')
    # import pydevd
    # pydevd.settrace('localhost', port=12999, stdoutToServer=True, stderrToServer=True)
    #
    baseaccums = [default() if default else None for default in deffuncs]
    # print "Base: %s" % repr(baseaccums)

    # pull the key apart
    for group, tups in groupby(inp, lambda (k, _): tuple(e if ef is None else None for e, ef in zip(k, ffuncs))):
        accums = copy.copy(baseaccums)
        for record, _ in tups:
            # print "REC: %s" % repr(record)
            try:
                accums = [f(a, v) if None not in (f, a, v) else None
                          for f, a, v in zip(ffuncs, accums, record)]
            except Exception as e:
                print e
                print "YOLO: f=%s a=%s r=%s g=%s" % (ffuncs, accums, record, group)
                import traceback
                print traceback.format_exc(15)
                raise e

        accum = [h(a) if None not in (h, a) else None for h, a in zip(ghfuncs, accums)]
        if label_fn:
            label = label_fn(group)
        key = tuple(g if g is not None else a for g, a in zip(group, accum))
        # print "GROUP: %s \nKEY: %s" % (repr(group), repr(key))
        yield label, key


def _skip(inp, label, ffuncs, ghfuncs, deffuncs, label_fn=None):
    """
    Process function of aggregation combine stage without groupby.
    """
    accums = [default() if default else None for default in deffuncs]
    for record, _ in inp:
        try:
            accums = [f(a, v) if None not in (f, a, v) else None
                      for f, a, v in zip(ffuncs, accums, record)]
        except Exception as e:
            raise e

    accum = [h(a) if None not in (h, a) else None for h, a in zip(ghfuncs, accums)]
    yield 0, tuple(accum)


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
