# -*- coding: utf-8 -*-
from unittest import TestCase
from mdb import Writer, Reader, DupReader
from ujson import dumps, loads


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
        writer = Writer('./test_rw', encode_fn=dumps)
        writer.drop()
        writer.put('foo', 'bar')
        writer.put('egg', 'spam')
        reader = Reader('./test_rw', decode_fn=loads)
        value = reader.get('foo')
        self.assertEqual(value, 'bar')
        value = reader.get('egg')
        self.assertEqual(value, 'spam')

    def test_dup_reader_and_writer(self):
        def key_value_gen():
            for i in range(3):
                yield 'fixed', "value%d" % (i * i)
        writer = Writer('./test_rw_dup', dup=True,
                        encode_fn=dumps)
        writer.drop()
        writer.put('foo', 'bar')
        writer.put('egg', 'spam')
        writer.mput({"foo": "bar1", "egg": "spam1"})
        writer.mput(key_value_gen())
        reader = DupReader('./test_rw_dup',
                           decode_fn=loads)
        values = reader.get('foo')
        self.assertEqual(list(values), ['bar', 'bar1'])
        values = reader.get('egg')
        self.assertEqual(list(values), ['spam', 'spam1'])
        values = reader.get('fixed')
        self.assertEqual(list(values), ['value0', 'value1', 'value4'])
