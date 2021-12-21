from unittest import TestCase
from skiplist import Skiplist, Iterator
import random


def get_random_keys(size):
    return [random.randint(0, size) for _ in range(size)]


def basic_test(tc: TestCase, rounds, key_size):
    for i in range(rounds):
        print('== ROUND:', i + 1)
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
    def test_insert_basic(self):
        print('TEST_INSERT_BASIC')
        basic_test(self, 20, 100)

    def test_insert_many(self):
        print('TEST_INSERT_MANY')
        basic_test(self, 3, 100000)

    def test_delete(self):
        print('TEST_DELETE')
        for i in range(20):
            print('== ROUND:', i + 1)
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
        print('TEST_ITER')
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

    def test_iterator(self):
        print('TEST_ITERATOR')
        sl = Skiplist()
        for i in range(1000):
            sl.insert(i, None)
        for node in sl:
            self.assertIsNotNone(node)

        it = sl.get_iterator()
        prev = None
        for node in it:
            self.assertIsNotNone(node)
            if prev is not None:
                self.assertLess(prev.key, node.key)
            prev = node

        it.reset()

        it.find_greater_or_equal(500)
        self.assertGreaterEqual(it.get_current().key, 500)

        it.find_greater_or_equal(1000)
        self.assertIsNone(it.get_current())

        it.reset()

        it.find_last_less(500)
        self.assertEqual(it.get_current().key, 499)

    def test_iterator_while_modify(self):
        print('TEST_ITERATOR_WHILE_MODIFY')
        sl = Skiplist()
        for i in range(1000):
            sl.insert(i, None)
        for node in sl:
            self.assertIsNotNone(node)

        it = sl.get_iterator()
        it.find_greater_or_equal(500)
        self.assertGreaterEqual(it.get_current().key, 500)

        sl.insert(500.5, None)
        self.assertEqual(it.next().key, 500.5)

        sl.delete(501)
        self.assertEqual(it.next().key, 502)
