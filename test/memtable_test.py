from memtable import MemTable
from unittest import TestCase
from dbformat import ValueType, LookupKey
from status import Status
from test.test_utils import random_user_str


class MemtableTest(TestCase):
    def test_basic(self):
        mem = MemTable()
        mem.add(1, 'hello', 'world', ValueType.kTypeValue)

        value = []
        s = Status.OK()
        lkey = LookupKey('hello', 1)
        self.assertTrue(mem.get(lkey, value, s))
        self.assertEqual(s, Status.OK())
        self.assertEqual(value[0], 'world')

    def test_sequence_number(self):
        mem = MemTable()
        mem.add(1, 'hello', 'world1', ValueType.kTypeValue)
        mem.add(2, 'hello', 'world2', ValueType.kTypeValue)
        mem.add(3, 'hello', 'world3', ValueType.kTypeValue)
        mem.add(4, 'hello', 'world4', ValueType.kTypeValue)
        mem.add(5, 'hello', 'world5', ValueType.kTypeValue)

        for seq in range(1, 6):
            value = []
            s = Status.OK()
            lkey = LookupKey('hello', seq)
            self.assertTrue(mem.get(lkey, value, s))
            self.assertEqual(s, Status.OK())
            self.assertEqual(value[0], 'world' + str(seq))

    def test_delete(self):
        mem = MemTable()
        mem.add(1, 'hello', 'world1', ValueType.kTypeValue)
        mem.add(2, 'hello', 'world2', ValueType.kTypeValue)
        mem.add(3, 'hello', '', ValueType.kTypeDeletion)
        mem.add(4, 'hello', 'world4', ValueType.kTypeValue)

        expected = {
            1: ('world1', ValueType.kTypeValue),
            2: ('world2', ValueType.kTypeValue),
            3: ('', ValueType.kTypeDeletion),
            4: ('world4', ValueType.kTypeValue),
        }

        for (k, v) in expected.items():
            value = []
            s = Status.OK()
            lkey = LookupKey('hello', k)
            self.assertTrue(mem.get(lkey, value, s))
            if v[1] == ValueType.kTypeDeletion:
                self.assertEqual(s, Status.NotFound())
                self.assertEqual(len(value), 0)
            else:
                self.assertEqual(s, Status.OK())
                self.assertEqual(value[0], v[0])

    def test_multiple(self):

        for cnt in range(3):
            # Test multiple times.
            mem = MemTable()
            records = {}

            for seq in range(1, 10001):
                # Insert a key with a random user-string value and sequence number
                user_str = random_user_str(10)
                user_value = random_user_str(10)
                records[seq] = (user_str, user_value)
                mem.add(seq, user_str, user_value, ValueType.kTypeValue)

            for seq in range(1, 10001):
                # Assert that the key is present.
                value = []
                s = Status.OK()
                lkey = LookupKey(records[seq][0], seq)
                self.assertTrue(mem.get(lkey, value, s))
                self.assertEqual(s, Status.OK())
                self.assertEqual(value[0], records[seq][1])

            for seq in range(10001, 20001):
                # Assert that the key is present with a greater sequence numbers.
                value = []
                s = Status.OK()
                lkey = LookupKey(records[seq - 10000][0], seq)
                self.assertTrue(mem.get(lkey, value, s))
                self.assertEqual(s, Status.OK())
                self.assertEqual(value[0], records[seq - 10000][1])

            for (k, v) in records.items():
                seq = 10001
                # Delete all records
                user_str = v[0]
                user_value = ''
                mem.add(seq, user_str, user_value, ValueType.kTypeDeletion)

                # Assert that all records are deleted
                value = []
                s = Status.OK()
                lkey = LookupKey(user_str, seq)
                self.assertTrue(mem.get(lkey, value, s))
                self.assertEqual(s, Status.NotFound())
                self.assertEqual(len(value), 0)
