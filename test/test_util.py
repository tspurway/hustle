import unittest
from hustle.core.util import SortedIterator

class TestSortedIterator(unittest.TestCase):

    def test_merges_sorted_inputs(self):
        data = [
            [
                ((1, 1), 'some_value'),
                ((1, 2), 'some_value'),
                ((1, 3), 'some_value')
            ],
            [
                ((1, 100), 'some_value'),
                ((1, 200), 'some_value'),
                ((1, 300), 'some_value')
            ],
            [
                ((1, 10), 'some_value'),
                ((1, 20), 'some_value'),
                ((1, 30), 'some_value')
            ],
            [
                ((1, 4), 'some_value'),
                ((1, 40), 'some_value'),
                ((1, 400), 'some_value')
            ]
        ]
        sorted_iterator = SortedIterator(data)
        expected = [
            ((1, 1), 'some_value'),
            ((1, 2), 'some_value'),
            ((1, 3), 'some_value'),
            ((1, 4), 'some_value'),
            ((1, 10), 'some_value'),
            ((1, 20), 'some_value'),
            ((1, 30), 'some_value'),
            ((1, 40), 'some_value'),
            ((1, 100), 'some_value'),
            ((1, 200), 'some_value'),
            ((1, 300), 'some_value'),
            ((1, 400), 'some_value')]
        self.assertListEqual(list(sorted_iterator), expected)

    def test_assumes_individual_inputs_are_already_sorted(self):
        data = [
            [
                ((2, 1), 'some_value'),
                ((1, 1), 'some_value'),
            ],
            [
                ((4, 1), 'some_value'),
                ((3, 1), 'some_value'),
            ]
        ]
        sorted_iterator = SortedIterator(data)
        expected = [
            ((2, 1), 'some_value'),
            ((1, 1), 'some_value'),
            ((4, 1), 'some_value'),
            ((3, 1), 'some_value')]
        self.assertListEqual(list(sorted_iterator), expected)

    def test_handles_duplicates(self):
        data = [
            [
                ((1, 1), 'some_value'),
                ((1, 2), 'some_value'),
            ],
            [
                ((1, 1), 'some_value'),
                ((1, 2), 'some_value'),
                ((1, 3), 'some_value'),
            ],
            [
                ((1, 3), 'some_value'),
            ]
        ]
        sorted_iterator = SortedIterator(data)
        expected = [
            ((1, 1), 'some_value'),
            ((1, 1), 'some_value'),
            ((1, 2), 'some_value'),
            ((1, 2), 'some_value'),
            ((1, 3), 'some_value'),
            ((1, 3), 'some_value')]
        self.assertListEqual(list(sorted_iterator), expected)

    def test_handles_empty_input(self):
        data = [
            [((1, 1), 'some_value')],
            [],  # <----- empty input
            [((2, 1), 'some_value')],
        ]
        sorted_iterator = SortedIterator(data)
        expected = [
            ((1, 1), 'some_value'),
            ((2, 1), 'some_value')]
        self.assertListEqual(list(sorted_iterator), expected)
