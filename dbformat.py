from enum import Enum
from db_types import SequenceNumber

byte_order = 'big'


class ValueType(Enum):
    kTypeDeletion = 0
    kTypeValue = 1

    def to_bytes(self, length: int, byteorder: str, *, signed: bool = ...) -> bytes:
        return int(self.value).to_bytes(length, byteorder, signed=signed)


def pack_sequence_and_type(seq: SequenceNumber, t: ValueType) -> bytearray:
    res = bytearray(seq.to_bytes(7, byte_order))
    res.extend(t.to_bytes(1, byte_order))
    return res


def pack_user_key_sequence_type(user_key: str, seq: SequenceNumber, t: ValueType) -> bytearray:
    res = bytearray(user_key.encode('utf-8'))
    res.extend(pack_sequence_and_type(seq, t))
    return res


class LookupKey:
    def __init__(self, user_key: str, sequence: SequenceNumber):
        length = len(user_key) + 8  # The length of internal key
        self._bytes = bytearray(length.to_bytes(4, byte_order))
        self._bytes.extend(user_key.encode('utf-8'))
        self._bytes.extend(pack_sequence_and_type(
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

    def memtable_key(self):
        return str(self._bytes, 'utf-8')

    def internal_key(self):
        return str(self._bytes[4:], 'utf-8')

    def user_key(self):
        return str(self._bytes[4:-8], 'utf-8')
