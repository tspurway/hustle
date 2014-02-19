# -*- coding: utf-8 -*-
from unittest import TestCase
from mdb import Writer, Reader, DupReader


class TestReaderWriter(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        import shutil
        try:
            shutil.rmtree('./test_rw')
        except OSError:
            pass
        try:
            shutil.rmtree('./test_rw_dup')
        except OSError:
            pass

    def test_reader_and_writer(self):
        writer = Writer('./test_rw', int_key=True)
        writer.drop()
        writer.put(1234, 'bar')
        writer.put(5678, 'spam')
        reader = Reader('./test_rw', int_key=True)
        value = reader.get(1234)
        self.assertEqual(value, 'bar')
        value = reader.get(5678)
        self.assertEqual(value, 'spam')

    def test_dup_reader_and_writer(self):
        def key_value_gen():
            for i in range(3):
                yield 789, "value%d" % (i * i)
        writer = Writer('./test_rw_dup', int_key=True, dup=True)
        writer.drop()
        writer.put(123, 'bar')
        writer.put(456, 'spam')
        writer.mput({123: "bar1", 456: "spam1"})
        writer.mput(key_value_gen())
        reader = DupReader('./test_rw_dup', int_key=True)
        values = reader.get(123)
        self.assertEqual(list(values), ['bar', 'bar1'])
        values = reader.get(456)
        self.assertEqual(list(values), ['spam', 'spam1'])
        values = reader.get(789)
        self.assertEqual(list(values), ['value0', 'value1', 'value4'])
