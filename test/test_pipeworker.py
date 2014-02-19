import unittest
from hustle.core.pipeworker import sort_reader, disk_sort
from StringIO import StringIO
import os

OUT_FILE = '/tmp/test_disk_sort'

TEST_FILE = \
    b"stuff\xff19\xffvalue1\x00" \
    b"morestuff\xff29\xffvalue2\x00" \
    b"reallylongkeyprobablylongerthanthebufferljkfdskjlkjjkjkjjjjjjjjjjjjjsfddfsfdsdfsdfsfdsfdsdfsfdsfdsdsffdsdfsdfsfdsdfsdfsdfsdfsfdsdsfdfsfdsfdsdsfdsffdsdfsdsfdsfdfsdfsfdssdfdfsdfsdfsdfsdfsfdsfdssdfdfs\xff15\xfffinalvalue\x00"\

EXPECTED = [
    (["stuff", "19"], "value1"),
    (["morestuff", "29"], "value2"),
    (["reallylongkeyprobablylongerthanthebufferljkfdskjlkjjkjkjjjjjjjjjjjjjsfddfsfdsdfsdfsfdsfdsdfsfdsfdsdsffdsdfsdfsfdsdfsdfsdfsdfsfdsdsfdfsfdsfdsdsfdsffdsdfsdsfdsfdfsdfsfdssdfdfsdfsdfsdfsdfsfdsfdssdfdfs", "15"], "finalvalue"),
]

RESPECTED = [
    (["stuff", 1900], 'value1'),
    (["morestuff", 9], 'value2'),
    (["anymore", 290], 'value3'),
    (["stuff", 29], 'value4'),
    (["toeat", 1500], 'value5'),
    (["reallystuff", 15], 'finalvalue'),
]


SOMENULLS = [
    (["olay", 1900], 'value1'),
    (["morestuff", 9], 'value2'),
    (["anymore", 290], 'value3'),
    ([None, 29], 'value4'),
    (["toeat", 1500], 'value5'),
    (["reallystuff", 15], 'finalvalue'),
]


class TestPipeworker(unittest.TestCase):
    def setUp(self):
        pass

    def _clean_ds_tmp(self):
        try:
            os.unlink(OUT_FILE)
        except:
            pass

    def test_sort_reader(self):
        for buf_size in [8, 16, 32, 64, 256, 8192]:
            infile = StringIO(TEST_FILE)
            for actual, expected in zip(sort_reader(infile, 'test', buf_size), EXPECTED):
                self.assertListEqual(actual[0], expected[0])
                self.assertEqual(actual[1], expected[1])

    def test_simple_disk_sort(self):
        self._clean_ds_tmp()
        actual = [(key, value) for key, value in disk_sort(RESPECTED, OUT_FILE, (0, 1))]
        print "ACTUAL: ", actual
        self.assertEqual(actual[0][0][0], "anymore")
        self.assertEqual(actual[1][0][1], 9)
        self.assertEqual(actual[2][0][0], "reallystuff")
        self.assertEqual(actual[3][1], ()) # tests secondary sorting

    def test_positional_disk_sort(self):
        self._clean_ds_tmp()
        actual = [(key, value) for key, value in disk_sort(RESPECTED, OUT_FILE, [1])]
        print "ACTUAL: ", actual
        self.assertEqual(actual[0][0][0], "morestuff")
        self.assertEqual(actual[1][0][1], 15)
        self.assertEqual(actual[2][0][0], "stuff")
        self.assertEqual(actual[3][1], ())
        self.assertEqual(actual[5][1], ())

    def test_nulls(self):
        self._clean_ds_tmp()
        actual = [(key, value) for key, value in disk_sort(SOMENULLS, OUT_FILE, [0])]
        print "ACTUAL: ", actual
        self.assertEqual(actual[0][0][0], None)

