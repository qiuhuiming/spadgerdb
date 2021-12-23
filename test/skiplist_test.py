from unittest import TestCase
from skiplist import Skiplist, Iterator
import random


def get_random_keys(size):
    return [random.randint(0, size) for _ in range(size)]


def basic_test(tc: TestCase, rounds, key_size):
    for i in range(rounds):
        sl = Skiplist()
        tc.assertEqual(sl.size(), 0)
        if i < 10:
            keys = get_random_keys(key_size)
        else:
            keys = range(key_size)

        expected_size = 0
        exist = set()
        for key in keys:
            if key not in exist:
                expected_size += 1
                exist.add(key)
            sl.insert(key, None)

        tc.assertEqual(sl.size(), expected_size)
        for key in keys:
            node = sl.search(key)
            tc.assertIsNotNone(node)


class SkiplistTest(TestCase):
    def test_compare(self):
        def comparator(x, y):
            return x - y

        sl = Skiplist(comparator=comparator)
        self.assertLess(sl._compare(1, 2), 0)
        self.assertLessEqual(sl._compare(1, 1), 0)
        self.assertGreater(sl._compare(2, 1), 0)
        self.assertGreaterEqual(sl._compare(2, 2), 0)

    def test_insert_basic(self):
        basic_test(self, 20, 100)

    def test_insert_many(self):
        basic_test(self, 1, 100000)

    def test_delete(self):
        for i in range(20):
            sl = Skiplist()
            if i < 10:
                keys = get_random_keys(100)
            else:
                keys = range(100)

            expected_size = 0
            exist = set()
            for key in keys:
                if key not in exist:
                    expected_size += 1
                    exist.add(key)
                sl.insert(key, None)

            delete = range(20, 88, 2)
            for key in delete:
                sl.delete(key)
                if key in keys:
                    expected_size -= 1

            self.assertEqual(sl.size(), expected_size)
            for key in keys:
                if key in delete:
                    self.assertIsNone(sl.search(key))
                else:
                    self.assertIsNotNone(sl.search(key))

    def test_iter(self):
        sl = Skiplist()
        for i in get_random_keys(1000):
            sl.insert(i, None)
        for node in sl:
            self.assertIsNotNone(node)

        prev = None
        for node in sl:
            self.assertIsNotNone(node)
            if prev is not None:
                self.assertLess(prev.key, node.key)
            prev = node

    def test_find_greater_or_equal(self):
        for cnt in range(3):
            sl = Skiplist()
            for i in range(1000):
                sl.insert(i, None)
            node, prev_list = sl.find_greater_or_equal(500, need_prev=True)
            self.assertIsNotNone(node)
            self.assertGreaterEqual(node.key, 500)
            self.assertEqual(sl.head_level(), len(prev_list))
            for prev in prev_list:
                self.assertTrue(prev.key is None or prev.key < node.key)

            node, _ = sl.find_greater_or_equal(500, need_prev=False)
            self.assertIsNotNone(node)
            self.assertGreaterEqual(node.key, 500)

            node, prev_list = sl.find_greater_or_equal(-1, need_prev=True)
            self.assertIsNotNone(node)
            self.assertEqual(node.key, 0)
            self.assertEqual(sl.head_level(), len(prev_list))
            for prev in prev_list:
                self.assertIsNone(prev.key)

    def test_find_last_less(self):
        for i in range(3):
            sl = Skiplist()
            for i in range(1000):
                sl.insert(i, None)

            node = sl.find_less_than(500)
            self.assertIsNotNone(node)
            self.assertLess(node.key, 500)
            self.assertEqual(node.key, 499)


class IteratorTest(TestCase):
    def test_iterator(self):
        sl = Skiplist()
        for i in range(1000):
            sl.insert(i, None)
        for node in sl:
            self.assertIsNotNone(node)

        it = sl.iter()
        prev = None
        for node in it:
            self.assertIsNotNone(node)
            if prev is not None:
                self.assertLess(prev.key, node.key)
            prev = node

        it.seek_to_first()

        it.seek(500)
        self.assertGreaterEqual(it.current().key, 500)

        it.seek(1000)
        self.assertIsNone(it.current())

    def test_iterator_while_modify(self):
        sl = Skiplist()
        for i in range(1000):
            sl.insert(i, None)
        for node in sl:
            self.assertIsNotNone(node)

        it = sl.iter()
        it.seek(500)
        self.assertGreaterEqual(it.current().key, 500)

        sl.insert(500.5, None)
        it.next()
        self.assertEqual(it.current().key, 500.5)

        sl.delete(501)
        it.next()
        self.assertEqual(it.current().key, 502)
