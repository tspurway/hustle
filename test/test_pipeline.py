import unittest
from hustle.core.pipeline import SelectPipe, _get_sort_range
from hustle.core.marble import Marble

EMP_FIELDS = ("+@2id", "+$name", "+%2hire_date", "+@4salary", "+@2department_id")
DEPT_FIELDS = ("+@2id", "+%2name", "+%2building", "+@2manager_id")


class TestPipeline(unittest.TestCase):
    def setUp(self):
        self.emp = Marble(name="employee",
                          fields=EMP_FIELDS)
        self.dept = Marble(name="department",
                           fields=DEPT_FIELDS)

    def test_get_key_names(self):
        wheres = [(self.emp.salary > 25000), self.dept]
        project = [self.emp.name, self.emp.salary, self.dept.building]

        pipe = SelectPipe('server', wheres=wheres, project=project)
        self.assertTupleEqual(('name', 'salary', None), tuple(pipe._get_key_names(project, ())[0][0]))
        self.assertTupleEqual((None, None, 'building'), tuple(pipe._get_key_names(project, ())[0][1]))

        join = [self.dept.id, self.emp.department_id]
        pipe = SelectPipe('server', wheres=wheres, project=project, join=join)
        self.assertTupleEqual(('department_id', 'name', 'salary', None), tuple(pipe._get_key_names(project, join)[0][0]))
        self.assertTupleEqual(('id', None, None, 'building'), tuple(pipe._get_key_names(project, join)[0][1]))

        project = [self.dept.building, self.emp.name, self.emp.salary]
        pipe = SelectPipe('server', wheres=wheres, project=project, join=join)
        self.assertTupleEqual(('department_id', None, 'name', 'salary'), tuple(pipe._get_key_names(project, join)[0][0]))
        self.assertTupleEqual(('id', 'building', None, None), tuple(pipe._get_key_names(project, join)[0][1]))

    def test_get_sort_range(self):
        project = [self.emp.name, self.emp.salary, self.dept.building]
        order_by = []

        # first case is with an empty order_by, it should sort by all columns
        sort_range = _get_sort_range(2, project, order_by)
        self.assertTupleEqual(tuple(sort_range), (2, 3, 4))
        sort_range = _get_sort_range(0, project, order_by)
        self.assertTupleEqual(tuple(sort_range), (0, 1, 2))

        # test with a specified order_by, note that we should always be sorting all columns - the order_by
        # just specifies the order.  The unspecified columns are not in a defined order.
        order_by = [self.emp.salary]
        sort_range = _get_sort_range(2, project, order_by)
        self.assertEqual(len(sort_range), 3)
        self.assertEqual(sort_range[0], 3)

        order_by = [self.dept.building, self.emp.name]
        sort_range = _get_sort_range(1, project, order_by)
        self.assertEqual(len(sort_range), 3)
        self.assertTupleEqual(sort_range[:2], (3, 1))

    def test_get_pipeline(self):
        wheres = [(self.emp.salary > 25000), self.dept]
        project = [self.emp.name, self.emp.salary, self.dept.building]

        pipe = SelectPipe('server',
                            wheres=wheres,
                            project=project)
        #(SPLIT, HustleStage('restrict-project',
        #                            process=partial(process_restrict, jobobj=job),
        #                            input_chain=[partial(hustle_stream, jobobj=job)]))
        pipeline = pipe.pipeline
        self.assertEqual(len(pipeline), 1)
        self.assertEqual('split', pipeline[0][0])
        self.assertEqual('restrict-select', pipeline[0][1].name)

        order_by = [self.dept.building, self.emp.name]
        pipe = SelectPipe('server',
                            wheres=wheres,
                            project=project,
                            order_by=order_by)
        #(SPLIT, HustleStage('restrict-project',
        #                            process=partial(process_restrict, jobobj=job),
        #                            input_chain=[partial(hustle_stream, jobobj=job)])),
        #(GROUP_LABEL, HustleStage('order',
        #                          process=partial(process_order, jobobj=job, distinct=job.distinct),
        #                          sort=sort_range))]

        pipeline = pipe.pipeline
        self.assertEqual(len(pipeline), 3)
        self.assertEqual('split', pipeline[0][0])
        self.assertEqual('group_node_label', pipeline[1][0])
        self.assertEqual('order-combine', pipeline[1][1].name)

        order_by = [self.dept.building, self.emp.name]
        join = [self.dept.id, self.emp.department_id]
        pipe = SelectPipe('server',
                            wheres=wheres,
                            project=project,
                            order_by=order_by,
                            join=join)
        pipeline = pipe.pipeline
        self.assertEqual(len(pipeline), 4)
        self.assertEqual('split', pipeline[0][0])
        self.assertEqual('group_label', pipeline[1][0])
        self.assertEqual('join', pipeline[1][1].name)
        self.assertEqual('group_all', pipeline[3][0])
        self.assertEqual('order-reduce', pipeline[3][1].name)

    def test_column_aliases_project(self):
        wheres = [(self.emp.salary > 25000), self.dept]
        project = [self.emp.name, self.emp.salary, self.dept.building, self.dept.name]
        order_by = ['name', 'employee.salary', self.dept.building, 3]
        join = [self.emp.name, self.dept.name]

        pipe = SelectPipe('server',
                            wheres=wheres,
                            project=project,
                            order_by=order_by,
                            join=join)

        self.assertEqual(len(pipe.order_by), 4)
        self.assertEqual(pipe.order_by[0], self.emp.name)
        self.assertEqual(pipe.order_by[1], self.emp.salary)
        self.assertEqual(pipe.order_by[2], self.dept.building)
        self.assertEqual(pipe.order_by[0], self.dept.name)




