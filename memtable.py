from skiplist import Skiplist
from status import Status
from dbformat import ValueType, Encoder, Decoder, LookupKey
from db_types import SequenceNumber
from utils import internal_key_comparator, user_key_comparator
from typing import List


class MemTable:
    def __init__(self):
        self._comparator = internal_key_comparator
        self._table = Skiplist(comparator=self._comparator)
        self._mem_usages = 0

    def compare_internal_key(self, x, y) -> int:
        return self._comparator(x, y)

    def compare_user_key(self, x, y) -> int:
        return user_key_comparator(x, y)

    def add(self, s: SequenceNumber, key: str, value: str, t: ValueType):
        buf = Encoder.encode_full_memtable_key(s, key, value, t)
        self._table.insert(buf)
        self._mem_usages += len(buf)

    def get(self, lkey: LookupKey, value: List[str], s: Status) -> bool:
        it = self._table.iter()
        it.seek(lkey.memtable_key())
        if it.valid():
            user_key = Decoder.decode_user_key_from_memtable_key(mkey=it.key())
            if self.compare_user_key(user_key, lkey.user_key()) == 0:
                # We found user_key is matched, and then we check ValueType.
                # If the ValueType is kTypeValue, we return the true with the value.
                # Else we return true with NotFound status.
                tag = Decoder.decode_tag_from_memtable_key(mkey=it.key())
                value_type = ValueType(tag[7])
                if value_type == ValueType.kTypeValue:
                    value.append(
                        Decoder.decode_value_from_memtable_key(it.key()))
                    s.assign(Status.OK())
                    return True
                elif value_type == ValueType.kTypeDeletion:
                    s.assign(Status.NotFound())
                    return True
        return False

    def approximate_memory_usage(self):
        return self._mem_usages
