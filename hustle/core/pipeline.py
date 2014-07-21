from disco.core import Job
from disco.worker.task_io import task_input_stream
from functools import partial
from hustle.core.marble import Marble, Column, Aggregation,\
    dflt_f, dflt_gh, dflt_default
from hustle.core.pipeworker import HustleStage

import sys
import hustle
import hustle.core
import hustle.core.marble
import hustle.core.column_fn


SPLIT = "split"
GROUP_ALL = "group_all"
GROUP_LABEL = "group_label"
GROUP_LABEL_NODE = "group_node_label"
GROUP_NODE = "group_node"

_POOL = 'abcdefghijklmnopqrstuvwxyz0123456789'
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
            self.env, self.txn, self.dbs, self.meta = \
                self.result_table._open(self.filename, maxsize, write=True,
                                        lru_size=10000)
            self.autoinc = 1
            self.url = url
            self.vid_trie = Trie()
            self.vid16_trie = Trie()

        def add(self, k, v):
            from hustle.core.marble import _insert_row
            data = dict(zip(self.result_columns, list(k) + list(v)))
            #print "BOZAK! adding %s %s %s" % (self.result_columns, k, v)
            updated_dbs = _insert_row(data, self.txn, self.dbs, self.autoinc,
                                      self.vid_trie, self.vid16_trie)
            if updated_dbs:
                self.dbs = updated_dbs
            self.autoinc += 1

        def close(self):
            import os
            import ujson
            from disco import util

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
            for index, (subdb, subindexdb, bitmap_dict, column, last) in self.dbs.iteritems():
                if subindexdb:
                    # process all values for this bitmap index
                    if column.index_indicator == 2:
                        bitmap_dict.evictAll()
                    else:
                        for val, bitmap in bitmap_dict.iteritems():
                            subindexdb.put(self.txn, val, bitmap.dumps())
                # insert a sentinel row to value table
                subdb.put(self.txn, self.autoinc + 1, last)
            self.txn.commit()

            try:
                self.env.copy(self.url)
                # print "Dumped result to %s" % self.url
            except Exception as e:
                msg = "Copy error: %s" % e
                print msg
                self.txn.abort()
                raise util.DataError(msg, "")
            finally:
                self.env.close()
                os.unlink(self.filename)

    return HustleOutputStream(stream, url, params)


def hustle_input_stream(fd, size, url, params, wheres, gen_where_index, key_names, limit):
    from disco import util
    from hustle.core.marble import Expr, MarbleStream
    from itertools import izip, repeat, islice, imap
    from sys import maxint
    from pyebset import BitSet

    empty = ()

    try:
        scheme, netloc, rest = util.urlsplit(url)
    except Exception as e:
        msg = "Error handling hustle_input_stream for %s. %s" % (url, e)
        raise util.DataError(msg, url)

    fle = util.localize(rest, disco_data=params._task.disco_data,
                        ddfs_data=params._task.ddfs_data)

    otab = None
    try:
        otab = MarbleStream(fle)
        bitmaps = {}

        for index, where in enumerate(wheres):
            # do not process where clauses that have nothing to do with this marble
            if where._name == otab.marble._name:
                if type(where) is Expr and not where.is_partition:
                    bm = where(otab)
                    if limit != maxint:
                        bs = BitSet()
                        for i in islice(bm, 0, limit):
                            bs.set(i)
                        bitmaps[index] = (bs, len(bs))
                    else:
                        bitmaps[index] = (bm, len(bm))
                else:
                    # it is either the table itself, or a partition expression.
                    # Either way, returns the entire table
                    if limit != maxint:
                        bs = BitSet()
                        for i in islice(otab.iter_all(), 0, limit):
                            bs.set(i)
                        bitmaps[index] = (bs, len(bs))
                    else:
                        bitmaps[index] = (otab.iter_all(), otab.number_rows)

        for index, (bitmap, blen) in bitmaps.iteritems():
            prefix_gen = [repeat(index, blen)] if gen_where_index else []

            # row_iter = prefix_gen + \
                # [otab.mget(col, bitmap) if col is not None else repeat(None, blen)
                 # for col in key_names[index]]
            row_creators = []
            for col, column_fn in key_names[index]:
                if col is not None:
                    if column_fn is None:
                        row_creators.append(otab.mget(col, bitmap))
                    else:
                        row_creators.append(imap(column_fn, otab.mget(col, bitmap)))
                else:
                    row_creators.append(repeat(None, blen))
            row_iter = prefix_gen + row_creators

            for row in izip(*row_iter):
                yield row, empty
    finally:
        if otab:
            otab.close()


class SelectPipe(Job):
    # profile = True
    required_modules = [
        ('hustle', hustle.__file__),
        ('hustle.core', hustle.core.__file__),
        ('hustle.core.pipeline', __file__),
        ('hustle.core.marble', hustle.core.marble.__file__),
        ('hustle.core.column_fn', hustle.core.column_fn.__file__)]

    def get_result_schema(self, project, tag=None):
        import random
        from hustle import Table

        if self.output_table:
            return self.output_table
        fields = []
        for col_or_agg in project:
            col_spec = col_or_agg.schema_string()
            if col_spec not in fields:
                fields.append(col_spec)
        if tag is None:
            name = '_'.join([w._name for w in self.wheres])[:64]
            # append a 3-charactor random suffix to avoid name collision
            name = "sub_%s_%s" % (name, "".join(random.sample(_POOL, 3)))
        else:
            name = tag
        self.output_table = Table(name=name, fields=fields)
        return self.output_table

    def _get_table(self, obj):
        """If obj is a table return its name otherwise figure out
        what it is and return the tablename"""
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
                selectcol = next((c for c in check
                                  if c.name == col or c.fullname == col), None)
                if selectcol:
                    rval.append(selectcol)
            elif isinstance(col, int):
                if col < len(check):
                    rval.append(check[col])
        return rval

    def _get_key_names(self, project, join):
        key_names = []
        for where in self.wheres:
            table_name = self._get_table(where)._name
            keys = []
            if join:
                join_column = next((c.name, c.column_fn) for c in join
                                   if c.table._name == table_name)
                keys.append(join_column)
            keys += tuple((c.column.name, c.column.column_fn)
                          if c.table is None or c.table._name == table_name
                          else (None, None) for c in project)
            key_names.append(keys)
        return key_names

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
                 wide=False,
                 pre_order_stage=(),
                 tag=None,
                 max_cores=0,
                 profile=False):
        from hustle.core.pipeworker import Worker

        super(SelectPipe, self).__init__(master=master, worker=Worker())
        if max_cores < 0:
            max_cores = 0
        if max_cores > 0:
            self.scheduler = {"max_cores": max_cores}
        self.profile = profile
        self.wheres = wheres
        self.order_by = self._resolve(order_by, project)
        partition = partition or _NPART
        binaries = [i for i, c in enumerate(project)
                    if isinstance(c, (Column, Aggregation)) and c.is_binary]
        # if nest is true, use output_schema to store the output table
        self.output_table = None

        # aggregation functions and their defaults
        efs, gees, ehches, dflts = zip(*[(c.f, c.g, c.h, c.default)
                                         if isinstance(c, Aggregation)
                                         else (dflt_f, dflt_gh, dflt_gh, dflt_default)
                                         for c in project])
        need_agg = False  # need to commit aggregatation
        all_agg = True    # whether all columns in select are aggregates
        for c in project:
            if isinstance(c, Aggregation):
                need_agg = True
            else:
                all_agg = False
        if all_agg:
            _agg_fn = _aggregate_fast
        else:
            _agg_fn = _aggregate

        # build the pipeline
        select_hash_cols = ()
        sort_range = _get_sort_range(0, project, self.order_by)

        join_stage = []
        if join or full_join:
            joinbins = [i + 2 for i in binaries]
            join_stage = [
                (GROUP_LABEL,
                 HustleStage('join',
                             sort=(1, 0),
                             binaries=joinbins,
                             process=partial(process_join,
                                             full_join=full_join,
                                             ffuncs=efs,
                                             ghfuncs=ehches,
                                             deffuncs=dflts,
                                             wide=wide,
                                             need_agg=need_agg,
                                             agg_fn=_agg_fn,
                                             label_fn=partial(_tuple_hash,
                                                              cols=sort_range,
                                                              p=partition))))]
            select_hash_cols = (1,)

        group_by_stage = []
        if need_agg:
            # If all columns in project are aggregations, use process_skip_group
            # to skip the internal groupby
            if all_agg:
                process_group_fn = process_skip_group
                group_by_range = []
            else:
                process_group_fn = process_group
                group_by_range = [i for i, c in enumerate(project)
                                  if isinstance(c, Column)]

            # build the pipeline
            group_by_stage = []
            if wide:
                group_by_stage = [
                    (GROUP_LABEL_NODE,
                     HustleStage('group-combine',
                                 sort=group_by_range,
                                 binaries=binaries,
                                 process=partial(process_group_fn,
                                                 ffuncs=efs,
                                                 ghfuncs=ehches,
                                                 deffuncs=dflts,
                                                 label_fn=partial(_tuple_hash,
                                                                  cols=group_by_range,
                                                                  p=partition))))]
            # A Hack here that overrides disco stage's default option 'combine'.
            # Hustle needs all inputs with the same label to be combined.
            group_by_stage.append((GROUP_LABEL,
                                   HustleStage('group-reduce',
                                               combine=True,
                                               input_sorted=wide,
                                               sort=group_by_range,
                                               binaries=binaries,
                                               process=partial(process_group_fn,
                                                               ffuncs=efs,
                                                               ghfuncs=gees,
                                                               deffuncs=dflts))))

        # process the order_by/distinct stage
        order_stage = []
        if self.order_by or distinct or limit:
            order_stage = [
                (GROUP_LABEL_NODE,
                 HustleStage('order-combine',
                             sort=sort_range,
                             binaries=binaries,
                             desc=desc,
                             process=partial(process_order,
                                             distinct=distinct,
                                             limit=limit or sys.maxint))),
                (GROUP_ALL,
                 HustleStage('order-reduce',
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

        key_names = self._get_key_names(project, join)

        restrict_distinct = False
        restrict_limit = 0
        # check whether need to do a distinct/limit in the restrict select stage
        if not (join or full_join) and not need_agg and not self.order_by and distinct:
            restrict_distinct = True
            restrict_limit = limit or 0
        # check whether need to do a limit in the hustle_input stream
        input_stream_limit = 0
        if not (join or full_join) and not need_agg and not self.order_by and not distinct:
            input_stream_limit = limit or 0
        pipeline = [(SPLIT,
                     HustleStage('restrict-select',
                                 # combine=True,  # cannot set combine -- see #hack in restrict-select phase
                                 process=partial(process_restrict,
                                                 ffuncs=efs,
                                                 ghfuncs=ehches,
                                                 deffuncs=dflts,
                                                 wide=wide or join or full_join,
                                                 need_agg=need_agg,
                                                 agg_fn=_agg_fn,
                                                 distinct=restrict_distinct,
                                                 limit=restrict_limit or sys.maxint,
                                                 label_fn=partial(_tuple_hash,
                                                                  cols=select_hash_cols,
                                                                  p=partition)),
                                 input_chain=[task_input_stream,
                                              partial(hustle_input_stream,
                                                      wheres=wheres,
                                                      gen_where_index=join or full_join,
                                                      key_names=key_names,
                                                      limit=input_stream_limit or sys.maxint)]))
                    ] + join_stage + group_by_stage + list(pre_order_stage) + order_stage

        # determine the style of output (ie. if it is a Hustle Table),
        # and modify the last stage accordingly
        if nest:
            pipeline[-1][1].output_chain = \
                [partial(hustle_output_stream, result_table=self.get_result_schema(project, tag))]
        self.pipeline = pipeline


def _tuple_hash(key, cols, p):
    r = 0
    for c in cols:
        r ^= hash(key[c])
    return r % p


def _aggregate(inp, label_fn, ffuncs, ghfuncs, deffuncs):
    from functools import partial
    """
    General channel for executing aggregate function, would be used if
    columns in project are either aggregation or group by columns
    """
    vals = {}
    # TODO: figure out a way to set __name__ for partial object, update_wrapper
    # didn't work. Then we can get rid of "type(f) != partial"
    group_template = [(lambda a: a) if type(f) != partial and f.__name__ == 'dflt_f' else (lambda a: None)
                      for f in ffuncs]
    for record, _ in inp:
        group = tuple(f(e) for e, f in zip(record, group_template))
        if group in vals:
            accums = vals[group]
        else:
            accums = [default() for default in deffuncs]

        try:
            accums = [f(a, v) for f, a, v in zip(ffuncs, accums, record)]
        except Exception as e:
            print e
            print "YEEHEQW: f=%s a=%s r=%s g=%s" % (ffuncs, accums, record, group)
            import traceback
            print traceback.format_exc(15)
            raise e

        vals[group] = accums

    for group, accums in vals.iteritems():
        key = tuple(h(a) for h, a in zip(ghfuncs, accums))
        out_label = label_fn(group)
        yield out_label, key


def _aggregate_fast(inp, label_fn, ffuncs, ghfuncs, deffuncs):
    """
    Fast channel for executing aggregate function, would be used if
    all columns in project are aggregations
    """
    accums = [default() for default in deffuncs]
    for record, _ in inp:
        try:
            accums = [f(a, v) for f, a, v in zip(ffuncs, accums, record)]
        except Exception as e:
            print e
            print "YEEHEQW: f=%s a=%s r=%s" % (ffuncs, accums, record)
            import traceback
            print traceback.format_exc(15)
            raise e

    key = tuple(h(a) for h, a in zip(ghfuncs, accums))
    yield 0, key


def process_restrict(interface, state, label, inp, task, label_fn, ffuncs,
                     ghfuncs, deffuncs, agg_fn, wide=False, need_agg=False,
                     distinct=False, limit=sys.maxint):
    from disco import util
    from itertools import groupby, islice
    empty = ()

    # inp contains a set of replicas, let's force local #HACK
    input_processed = False
    for i, inp_url in inp.input.replicas:
        scheme, (netloc, port), rest = util.urlsplit(inp_url)
        if netloc == task.host:
            input_processed = True
            inp.input = inp_url
            break

    if not input_processed:
        raise util.DataError("Input %s not processed, no LOCAL resource found."
                             % str(inp.input), '')

    # opportunistically aggregate, distinct and limit in this stage
    if need_agg and not wide:
        for out_label, key in agg_fn(inp, label_fn, ffuncs, ghfuncs, deffuncs):
            interface.output(out_label).add(key, empty)
    else:
        if distinct:
            for uniqkey, _ in islice(groupby(inp, lambda (k, v): tuple(k)), 0, limit):
                label = label_fn(uniqkey)
                interface.output(label).add(uniqkey, empty)
        else:
            for key, value in islice(inp, 0, limit):
                out_label = label_fn(key)
                interface.output(out_label).add(key, value)


def process_join(interface, state, label, inp, task, full_join, label_fn,
                 ffuncs, ghfuncs, deffuncs, agg_fn, wide=False, need_agg=False):
    """
    Processor function for the join stage.

    Note that each key in the 'inp' is orgnized as:
        key = (where_index, join_column, other_columns)

    Firstly, all keys are divided into different groups based on the join_column.
    Then the where_index is used to separate keys from different where clauses.
    Finally, merging columns together.
    """
    from itertools import groupby
    empty = ()

    def _merge_record(offset, r1, r2):
        return [i if i is not None else j for i, j in zip(r1[offset:], r2[offset:])]

    def _join_input():
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
                        yield newrecord, value

    if need_agg and not wide:
        for out_label, key in agg_fn(_join_input(), label_fn, ffuncs, ghfuncs, deffuncs):
            interface.output(out_label).add(key, empty)
    else:
        for key, value in _join_input():
            out_label = label_fn(key)
            # print "JOIN: %s %s" % (key, value)
            interface.output(out_label).add(key, value)


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


def process_group(interface, state, label, inp, task, ffuncs, ghfuncs,
                  deffuncs, label_fn=None):
    """Process function of aggregation combine stage."""
    from itertools import groupby
    from functools import partial

    empty = ()

    # TODO: figure out a way to set __name__ for partial object, update_wrapper
    # didn't work. Then we can get rid of "type(f) != partial"
    group_template = [(lambda a: a) if type(f) != partial and f.__name__ == 'dflt_f' else (lambda a: None)
                      for f in ffuncs]
    # pull the key apart
    for group, tups in groupby(inp,
                               lambda (k, _):
                               tuple(ef(e) for e, ef in zip(k, group_template))):
        accums = [default() for default in deffuncs]
        for record, _ in tups:
            # print "Group: %s, REC: %s" % (group, repr(record))
            try:
                accums = [f(a, v) for f, a, v in zip(ffuncs, accums, record)]
            except Exception as e:
                print e
                print "YOLO: f=%s a=%s r=%s g=%s" % (ffuncs, accums, record, group)
                import traceback
                print traceback.format_exc(15)
                raise e

        key = tuple(h(a) for h, a in zip(ghfuncs, accums))
        if label_fn:
            label = label_fn(group)
        interface.output(label).add(key, empty)


def process_skip_group(interface, state, label, inp, task, ffuncs,
                       ghfuncs, deffuncs, label_fn=None):
    """Process function of aggregation combine stage without groupby.
    """
    empty = ()
    accums = [default() for default in deffuncs]
    for record, _ in inp:
        try:
            accums = [f(a, v) for f, a, v in zip(ffuncs, accums, record)]
        except Exception as e:
            raise e

    key = tuple(h(a) for h, a in zip(ghfuncs, accums))
    interface.output(0).add(key, empty)


def _get_sort_range(select_offset, select_columns, order_by_columns):
    # sort by all
    sort_range = [i + select_offset for i, c in enumerate(select_columns)
                  if isinstance(c, Column) and not c.is_binary]
    if order_by_columns:
        scols = ["%s%s" % (c.table._name if c.table else '', c.name)
                 for c in select_columns]
        ocols = ["%s%s" % (c.table._name if c.table else '', c.name)
                 for c in order_by_columns]
        rcols = set(scols) - set(ocols)
        # make sure to include the columns *not* in the order_by expression as well
        # this is to ensure that 'distinct' will work
        sort_range = tuple(select_offset + scols.index(c) for c in ocols) +\
            tuple(select_offset + scols.index(c) for c in rcols)
    return sort_range
