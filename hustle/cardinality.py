from hustle import select, Table, _create_job
from hustle.core.marble import Aggregation, Column, json_decoder
from hustle.core.pipeline import HustleStage, GROUP_ALL


def h_cardinality(col):
    """
    """
    def _inner_deault():
        from cardunion import Cardunion
        return Cardunion(12)

    def _inner_hll_accumulate(a, v):
        a.bunion([v])
        return a

    return Aggregation("cardinality",
                       col,
                       f=_inner_hll_accumulate,
                       g=lambda a: a.count(),
                       h=lambda a: a.dumps(),
                       default=_inner_deault)


def h_union(col):
    def _inner_deault():
        from cardunion import Cardunion
        return Cardunion(12)

    def _inner_hll_accumulate(a, v):
        a.bunion([v])
        return a

    return Aggregation("union",
                       col,
                       f=_inner_hll_accumulate,
                       g=lambda a, c: a.dumps(),
                       h=lambda a: a.dumps(),
                       default=_inner_deault)


def h_minhash_merge(col):
    def _inner_deault():
        from maxhash import MaxHash
        return MaxHash()

    def _inner_hll_accumulate(a, v):
        from maxhash import MaxHash
        a.merge(MaxHash.loads(v))
        return a

    return Aggregation("minhash_merge",
                       col,
                       f=_inner_hll_accumulate,
                       g=lambda a, c: a.dumps(),
                       h=lambda a: a.dumps(),
                       default=_inner_deault)


def intersect(*project, **kwargs):
    intersect_stage = (GROUP_ALL, HustleStage('intersect', process=_process_intersect))


def _process_intersect(interface, state, label, inp, task):
    pass
#
# def insert_hll(table, file=None, streams=None, preprocess=None,
#                maxsize=100 * 1024 * 1024, tmpdir='/tmp', decoder=json_decoder,
#                lru_size=10000, hll_field=None, **kwargs):
#     from hustle.core.settings import Settings
#     from cardunion import Cardunion
#     from maxhash import MaxHash
#     from scamurmur3 import murmur3_x64_64
#     import ujson
#     import os
#
#     settings = Settings(**kwargs)
#     ddfs = settings['ddfs']
#
#     def part_tag(name, partition=None):
#         rval = "hustle:" + name
#         if partition:
#             rval += ':' + str(partition)
#         return rval
#
#     def hll_iter(strms):
#         buf = {}
#         fields = table._field_names
#         fields.remove('hll')
#         fields.remove('maxhash')
#
#         for stream in strms:
#             for line in stream:
#                 #print "Line: %s" % line
#                 try:
#                     data = decoder(line)
#                 except Exception as e:
#                     print "Exception decoding record (skipping): %s %s" % (e, line)
#                 else:
#                     if preprocess:
#                         preprocess(data)
#                     key = ujson.dumps([data[f] for f in fields])
#                     if key not in buf:
#                         hll = Cardunion(12)
#                         maxhash = MaxHash()
#                         buf[key] = (hll, maxhash)
#                     else:
#                         hll, maxhash = buf[key]
#
#                     h = murmur3_x64_64(data[hll_field])
#                     hll.add_hashed(h)
#                     maxhash.add_hashed(h)
#
#         for key, (hll, maxhash) in buf.iteritems():
#             data = dict(zip(fields, ujson.loads(key)))
#             data['hll'] = hll.dumps()
#             data['maxhash'] = maxhash.dumps()
#             # print "Dizzol: %s" % repr(data)
#             yield data
#
#     if file:
#         streams = [open(file)]
#     lines, partition_files = table._insert([hll_iter(streams)],
#                                            maxsize=maxsize, tmpdir=tmpdir,
#                                            decoder=lambda x: x, lru_size=lru_size)
#     if partition_files is not None:
#         for part, pfile in partition_files.iteritems():
#             tag = part_tag(table._name, part)
#             ddfs.push(tag, [pfile])
#             print 'pushed %s, %s' % (part, tag)
#             os.unlink(pfile)
#     return table._name, lines
#
# def intersect(*wheres, **kwargs):
#     """Intersect one or more Hustle Cardinality tables.  Print the result to stdout.
#
#     Keyword arguments:
#     wheres -- the clauses specifying which rows from which tables to select
#     group_by -- the list of columns to group_by
#     order_by -- the list of columns to order_by. All columns in group_by are valid.
#                 Additionally, h_jaccard() and h_intersection() stand for jaccard
#                 index and cardinality of intersection
#     desc -- boolean to indicate whether sort descendingly
#     limit - a number or a tuple(offset, total_number) to limit the number of output
#
#
#     Returns:
#     None
#
#     Example:
#     intersect(
#         (impressions.date > '2013-10-15') & (impressions.page == 'metalbands.io'),
#         (searches.date > '2013-10-15') & (searches.term == 'metalica'),
#         group_by=[impressions.date], order_by=[h_intersection()], desc=True)
#
#     This statement assumes that the tables all have `hll` and `maxhash` columns with
#     appropriate content (see hustle.examples.test_insert_hll.py)"""
#
#     from hustle.core.pipeline import create_set_pipe
#     from hustle.core.settings import Settings
#     settings = Settings(**kwargs)
#     nest = settings.pop('nest', False)
#     group_by = settings.pop('group_by', ())
#     order_by = settings.pop('order_by', ())
#     desc = settings.pop('desc', False)
#     limit = settings.pop('limit', None)
#     partition = settings.get('partition', 0)
#     if partition < 0:
#         partition = 0
#     ddfs = settings['ddfs']
#     try:
#         check_query_agg([h_intersection()],
#                         (), group_by, order_by, limit, *wheres)
#     except ValueError as e:
#         print "  Invalid query:\n    %s" % e
#         return None
#
#     name = '-'.join([where._name for where in wheres[:2]])
#     job_blobs = set()
#     for where in wheres:
#         job_blobs.update(tuple(sorted(w)) for w in _get_blobs(where, ddfs))
#
#     select = [h_union(), h_minhash_merge()]
#     job = create_set_pipe('intersect',
#                           settings['server'],
#                           select=select,
#                           wheres=wheres,
#                           order_by=order_by,
#                           group_by=group_by,
#                           desc=desc,
#                           limit=limit,
#                           partition=partition,
#                           nest=nest)
#     job.reduce_output_stream = (reduce_output_stream, disco_output_stream)
#     job.run(name='intersect_from_%s' % name,
#             input=list(job_blobs), save=settings['save'], **kwargs)
#     blobs = job.wait()
#     if nest:
#         # the result will be just dumped to stdout
#         rtab = job.get_result_schema()
#         rtab._blobs = blobs
#         return rtab
#     else:
#         group_names = [c.name for c in group_by]
#         cols = ['Intersection', 'Jaccard Index', 'HLL Union', 'HLL Est.',
#                 'HLL Error', 'HLL Resolution']
#         print_seperator(110)
#         print_line(group_names + cols, width=100, cols=len(group_names) + len(cols))
#         print_seperator(110)
#         dump(blobs, 100)
#         return
#
#
# def union(*wheres, **kwargs):
#     """Union one or more Hustle Cardinality tables.  Print the result to stdout.
#
#     Keyword arguments:
#     wheres -- the clauses specifying which rows from which tables to select
#     group_by -- the list of columns to group_by
#     order_by -- the list of columns to order_by. All columns in group_by are valid.
#                 Additionally, h_union stands for cardinality of union
#     desc -- boolean to indicate whether sort descendingly
#     limit - a number or a tuple(offset, total_number) to limit the number of output
#
#
#     Returns:
#     None
#
#     Example:
#     union(
#         (impressions.date > '2013-10-15') & (impressions.page == 'metalbands.io'),
#         (searches.date > '2013-10-15') & (searches.term == 'metalica'),
#         group_by=[impressions.date], order_by=[h_union], desc=True)
#
#     This statement assumes that the tables all have `hll` columns with
#     appropriate content (see hustle.examples.test_insert_hll.py)
#     """
#     from hustle.core.pipeline import create_set_pipe
#     from hustle.core.settings import Settings
#     settings = Settings(**kwargs)
#     select = settings.pop('select', ())
#     nest = settings.pop('nest', False)
#     group_by = settings.pop('group_by', ())
#     order_by = settings.pop('order_by', ())
#     desc = settings.pop('desc', False)
#     limit = settings.pop('limit', None)
#     partition = settings.get('partition', 0)
#     if partition < 0:
#         partition = 0
#     ddfs = settings['ddfs']
#     try:
#         check_query_agg([h_union()], (), group_by, order_by, limit, *wheres)
#     except ValueError as e:
#         print "  Invalid query:\n    %s" % e
#         return None
#
#     name = '-'.join([where._name for where in wheres[:2]])
#     job_blobs = set()
#     for where in wheres:
#         job_blobs.update(tuple(sorted(w)) for w in _get_blobs(where, ddfs))
#
#     select = [h_union()]
#     job = create_set_pipe('union',
#                           settings['server'],
#                           select=select,
#                           wheres=wheres,
#                           order_by=order_by,
#                           group_by=group_by,
#                           desc=desc,
#                           limit=limit,
#                           partition=partition,
#                           nest=nest)
#     job.reduce_output_stream = (reduce_output_stream, disco_output_stream)
#     job.run(name='union_from_%s' % name,
#             input=list(job_blobs), save=settings['save'], **kwargs)
#     blobs = job.wait()
#     if nest:
#         # the result will be just dumped to stdout
#         rtab = job.get_result_schema()
#         rtab._blobs = blobs
#         return rtab
#     else:
#         group_names = [c.name for c in group_by]
#         cols = ['HLL Union']
#         print_seperator(40)
#         print_line(group_names + cols, width=40, cols=len(group_names) + len(cols))
#         print_seperator(40)
#         dump(blobs, 40)
#         return
#
# def calculate_intersect(interface, state, lable, inp, task, label_fn):
#     """Process function of intersection calculation stage.
#
#     Calculate jaccard index and hll unions based on input from reduce stage.
#     Then send result to a single node for the possible order_by stage.
#     """
#     from itertools import groupby
#     from cardunion import Cardunion
#     from maxhash import MaxHash
#     empty = ()
#
#     # The first item of key is the where index, ignore it when grouping keys
#     for key, values in groupby(inp, lambda (k, _): k[1:]):
#         hll = Cardunion(12)
#         maxhashes = []
#         hlls = []
#         for k, value in values:
#             ihll, imaxhash, icount = value
#             hll.bunion([ihll])
#             c = Cardunion(12)
#             c.bunion([ihll])
#             hlls.append(c)
#             maxhashes.append(MaxHash.loads(imaxhash))
#
#         j = MaxHash.get_jaccard_index(maxhashes) if len(maxhashes) > 1 else .0
#         hll_intersect, error, resolution = Cardunion.intersect(hlls)
#         c = hll.count()
#         keys = key + [long(j * c), j, c, hll_intersect, error, resolution]
#         interface.output(0).add(keys, empty)
#
#
# def calculate_union(interface, state, lable, inp, task, label_fn):
#     """Process function of union calculation stage.
#
#     Calculate hll unions based on input from reduce stage.
#     Then send result to a single node for the possible order_by stage.
#     """
#     from itertools import groupby
#     from cardunion import Cardunion
#     empty = ()
#
#     # The first item of key is the where index, ignore it when grouping keys
#     for key, values in groupby(inp, lambda (k, _): k[1:]):
#         hll = Cardunion(12)
#         hlls = []
#
#         for _, value in values:
#             ihll, icount = value
#             hll.bunion([ihll])
#             c = Cardunion(12)
#             c.bunion([ihll])
#             hlls.append(c)
#
#         keys = key + [hll.count()]
#         interface.output(0).add(keys, empty)
#
#
# def create_set_pipe(set_ops, master, wheres, select, order_by=(), group_by=(),
#                     desc=False, limit=None, partition=0, nest=False):
#     """Create a pipeline job for union or intersection based on AggregatePipe.
#
#     A set pipe layout is:
#
#     select-restrict | group-combine | group-reduce | set-ops | order-by | limit
#     """
#     pipe = SelectPipe(master, wheres, project=select, order_by=(),
#                          join=(), distinct=False,
#                          desc=desc, limit=limit, partition=partition, nest=nest)
#
#     # overwrite the reduce stage from Aggregate class
#     if order_by:
#         reduce_partition = 1
#     else:
#         reduce_partition = pipe.partition
#     reduce_stage = (GROUP_LABEL,
#                     HustleStage('group-reduce',
#                                 init=init_reduce,
#                                 process=partial(set_reduce,
#                                                 label_fn=partial(_tuple_hash, p=reduce_partition)),
#                                 sort=_inner_sort_range(len(group_by))))
#     pipe.pipeline[2] = reduce_stage
#
#     # insert a new union or intersection stage behind reduce stage
#     if set_ops.lower() == 'union':
#         from hustle import h_union
#         set_stage = (GROUP_LABEL,
#                      HustleStage('union',
#                                  process=partial(calculate_union,
#                                                  label_fn=partial(_tuple_hash, p=pipe.partition)),
#                                  sort=_inner_sort_range(len(group_by))))
#         sort_range = _get_sort_range_agg(order_by, group_by, (h_union(),))
#     elif set_ops.lower() == 'intersect':
#         from hustle import h_intersection, h_jaccard
#         set_stage = (GROUP_LABEL,
#                      HustleStage('intersect',
#                                  process=partial(calculate_intersect,
#                                                  label_fn=partial(_tuple_hash, p=pipe.partition)),
#                                  sort=_inner_sort_range(len(group_by))))
#         sort_range = _get_sort_range_agg(order_by, group_by, (h_intersection(), h_jaccard()))
#     else:
#         raise ValueError("Can't support operation %s, only take 'union' or 'intersect'." % set_ops)
#     pipe.pipeline.insert(3, set_stage)
#
#     # process the order_by if any
#     if order_by:
#         order_stage = (GROUP_LABEL,
#                        HustleStage('order-by',
#                                    process=partial(process_order, distinct=False),
#                                    sort=sort_range,
#                                    desc=desc))
#         if limit:
#             pipe.pipeline.insert(-1, order_stage)
#         else:
#             pipe.pipeline.append(order_stage)
#     return pipe


