from unittest import TestCase
from dbformat import LookupKey, pack_user_key_sequence_type, ValueType, byte_order
from dbformat import parse_user_key_from_memtable_key, parse_user_size_from_memtable_key, parse_internal_size_from_memtable_key, parse_tag_from_memtable_key


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

    def test_value_type_and_sequence_number(self):
        internal_key_bytes = pack_user_key_sequence_type(
            'hello', 1, ValueType.kTypeValue)
        self.assertEqual(
            internal_key_bytes[-1], ValueType.kTypeValue.value)
        self.assertEqual(
            int.from_bytes(internal_key_bytes[-8:-1], byte_order), 1)

    def test_parse(self):
        user_key = 'difasnjklcnaslkjcnasklcmaslkcmklasmcklsamnclkasmvlksamvklasmckl;msalcmaslkcmaslkcmasc'
        sequence = 100000000
        lookup_key = LookupKey(user_key, sequence)
        self.assertEqual(
            len(user_key), parse_user_size_from_memtable_key(lookup_key.memtable_key()))
        self.assertEqual(lookup_key.user_key(), parse_user_key_from_memtable_key(
            lookup_key.memtable_key()))
        self.assertEqual(len(lookup_key.internal_key()),
                         parse_internal_size_from_memtable_key(lookup_key.memtable_key()))
        tag: bytes = parse_tag_from_memtable_key(lookup_key.memtable_key())
        parsed_sequence = int.from_bytes(
            tag[:7], byte_order)
        parsed_value_type = ValueType(tag[7])
        self.assertEqual(sequence, parsed_sequence)
        self.assertEqual(ValueType.kTypeValue, parsed_value_type)
