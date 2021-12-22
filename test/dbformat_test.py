from unittest import TestCase
from dbformat import LookupKey


class LookupKeyTest(TestCase):
    def test_basic(self):
        user_key = 'hello'
        sequence = 1
        lookup_key = LookupKey(user_key, sequence)
        self.assertEqual(lookup_key.user_key(), user_key)

    def test_uft8(self):
        user_key = '你好'
        sequence = 1
        lookup_key = LookupKey(user_key, sequence)
        self.assertEqual(lookup_key.user_key(), user_key)

    def test_compare(self):
        user_key = 'hello'
        lookup_key1 = LookupKey(user_key, 1)
        lookup_key2 = LookupKey(user_key, 2)
        self.assertGreater(lookup_key2.memtable_key(),
                           lookup_key1.memtable_key())

    def test_length(self):
        user_key = 'hello'
        sequence = 1
        lookup_key = LookupKey(user_key, sequence)
        self.assertEqual(len(lookup_key.memtable_key()), len(user_key) + 8 + 4)
