from enum import Enum
from db_types import SequenceNumber

byte_order = 'little'


class ValueType(Enum):
    kTypeDeletion = 0
    kTypeValue = 1

    def to_bytes(self, length: int, byteorder: str, *, signed: bool = ...) -> bytes:
        return int(self.value).to_bytes(length, byteorder, signed=signed)


class Encoder:

    @staticmethod
    def encode_sequence_and_type(seq: SequenceNumber, t: ValueType) -> bytearray:
        """

        Args:
            seq (SequenceNumber)
            t (ValueType)

        Returns:
            bytearray: tag
        """
        res = bytearray(seq.to_bytes(7, byte_order))
        res.extend(t.to_bytes(1, byte_order))
        return res

    @staticmethod
    def encode_user_key_sequence_type(user_key: str, seq: SequenceNumber, t: ValueType) -> bytearray:
        """

        Args:
            user_key (str): [description]
            seq (SequenceNumber): 
            t (ValueType): 

        Returns:
            bytearray: internal_key
        """
        res = bytearray(user_key.encode('utf-8'))
        res.extend(Encoder.encode_sequence_and_type(seq, t))
        return res

    @staticmethod
    def encode_full_memtable_key(s: SequenceNumber, key: str, value: str, t: ValueType) -> bytearray:
        """

        Encode Format:
        |<internal_key_size>|<internal_key>|<value_size>|<value>|

        Args:
            s (SequenceNumber): 
            key (str): 
            value (str): 
            t (ValueType): 

        Returns:
            bytearray: memtable_key
        """
        internal_key_bytes = Encoder.encode_user_key_sequence_type(key, s, t)
        encoded_len = 4 + len(internal_key_bytes) + 4 + len(value)
        buf = bytearray(int(len(internal_key_bytes)).to_bytes(4, byte_order))
        buf.extend(internal_key_bytes)
        buf.extend(int(len(value)).to_bytes(4, byte_order))
        buf.extend(value.encode('utf-8'))
        assert len(buf) == encoded_len

        return buf


class Decoder:

    @staticmethod
    def decode_internal_size_from_memtable_key(mkey: bytearray) -> int:
        return int.from_bytes(mkey[:4], byte_order)

    @staticmethod
    def decode_user_size_from_memtable_key(mkey: bytearray) -> int:
        return Decoder.decode_internal_size_from_memtable_key(mkey) - 8

    @staticmethod
    def decode_user_key_from_memtable_key(mkey: bytearray) -> str:
        return mkey[4: 4 + Decoder.decode_user_size_from_memtable_key(mkey)].decode('utf-8')

    @staticmethod
    def decode_tag_from_memtable_key(mkey: bytearray) -> bytes:
        start = 4 + Decoder.decode_user_size_from_memtable_key(mkey)
        return mkey[start: start + 8]

    @staticmethod
    def decode_value_from_memtable_key(mkey: bytearray) -> str:
        start = 4 + Decoder.decode_user_size_from_memtable_key(mkey) + 8 + 4
        return mkey[start:].decode('utf-8')


class LookupKey:
    def __init__(self, user_key: str, sequence: SequenceNumber):
        length = len(user_key) + 8  # The length of internal key
        self._bytes = bytearray(length.to_bytes(4, byte_order))
        self._bytes.extend(user_key.encode('utf-8'))
        self._bytes.extend(Encoder.encode_sequence_and_type(
            sequence, ValueType.kTypeValue))
        """
        |<length>|<user_key>|<sequence>|<type> |
        | 4 bytes|<user_key>| 7 bytes  | 1 byte|
        |<-            memtable key          ->|
        |        |<-        internal key     ->|
        """

    def __eq__(self, other):
        return self.key == other.key

    def __hash__(self):
        return hash(self._bytes)

    def memtable_key(self) -> bytearray:
        return self._bytes

    def internal_key(self) -> bytearray:
        return self._bytes[4:]

    def user_key(self) -> str:
        return str(self._bytes[4:-8], 'utf-8')
