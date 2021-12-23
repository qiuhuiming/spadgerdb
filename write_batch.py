from db_types import SequenceNumber
from memtable import MemTable
from dbformat import ValueType
from status import Status


class WriteBatch:
    def __init__(self):
        self._batch = []
        self._count = 0
        self._size = 0
        self._sequence_number: SequenceNumber = None

    def put(self, key, value):
        self._batch.append((key, value, ValueType.kTypeValue))
        self._size += len(key) + len(value)
        self._count += 1

    def delete(self, key):
        self._batch.append((key, '', ValueType.kTypeDeletion))
        self._size += len(key)
        self._count += 1

    def count(self):
        return self._count

    def size(self):
        return self._size

    def __iter__(self):
        for k, v, t in self._batch:
            yield k, v, t

    def set_sequence_number(self, sequence_number):
        self._sequence_number = sequence_number

    class Handler:
        def __init__(self, mem_table: MemTable, sequence_number: SequenceNumber):
            self._mem_table = mem_table
            self._sequence_number = sequence_number

        def put(self, key, value):
            self._mem_table.add(self._sequence_number, key,
                                value, ValueType.kTypeValue)
            self._sequence_number += 1

        def delete(self, key):
            self._mem_table.add(self._sequence_number, key,
                                '', ValueType.kTypeDeletion)
            self._sequence_number += 1

    def apply(self, mem_table: MemTable) -> Status:
        assert self._sequence_number is not None
        handler = WriteBatch.Handler(mem_table, self._sequence_number)
        found = 0
        for k, v, t in self:
            found += 1
            if t == ValueType.kTypeValue:
                handler.put(k, v)
            elif t == ValueType.kTypeDeletion:
                handler.delete(k)
            else:
                return Status.Corruption("unknown WriteBatch tag")

        if found != self.count():
            return Status.Corruption("WriteBatch has wrong count")
        return Status.OK()
