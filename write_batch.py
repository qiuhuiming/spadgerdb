from db_types import SequenceNumber
from memtable import MemTable
from dbformat import ValueType, byte_order
from status import Status
from typing import Tuple


class WriteBatch:
    pass


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

    def serialize(self) -> bytearray:
        """
        Serialize the WriteBatch to a byte array.
        sequence_number: 4 Bytes
        count: 4 Bytes
        content: records[count]
        records = match ValueType {
            case kTypeValue:    kTypeValueSize|key_size|key_content|value_size|value_content
            case kTypeDeletion: kTypeDeletionSize|key_size|key_content
        }
        :return:  byte array
        """
        assert self._sequence_number is not None
        buf = bytearray(self._sequence_number.to_bytes(4, byte_order))
        buf.extend(bytearray(self._count.to_bytes(4, byte_order)))
        content = bytearray()

        def mapper(entry: Tuple[str, str, ValueType]) -> bytearray:
            k, v, t = entry
            if t == ValueType.kTypeValue:
                buffer = bytearray(ValueType.kTypeValue.value.to_bytes(1, byte_order))
                buffer.extend(bytearray(len(k).to_bytes(4, byte_order)))
                buffer.extend(bytearray(k.encode('utf-8')))
                buffer.extend(bytearray(len(v).to_bytes(4, byte_order)))
                buffer.extend(bytearray(v.encode('utf-8')))
                return buffer
            elif t == ValueType.kTypeDeletion:
                buffer = bytearray(ValueType.kTypeDeletion.value.to_bytes(1, byte_order))
                buffer.extend(bytearray(len(k).to_bytes(4, byte_order)))
                buffer.extend(bytearray(k.encode('utf-8')))
                return buffer
            else:
                raise Exception("unknown WriteBatch tag")

        for entry in map(mapper, self):
            content.extend(entry)

        buf.extend(content)
        return buf

    @staticmethod
    def deserialize(data: bytearray) -> WriteBatch:
        seq = int.from_bytes(data[:4], byte_order)
        assert seq >= 0
        count = int.from_bytes(data[4:8], byte_order)
        assert count > 0

        content = data[8:]
        records = []
        size = 0
        for i in range(count):
            t = int.from_bytes(content[:1], byte_order)
            t = ValueType(t)
            if t == ValueType.kTypeValue:
                key_size = int.from_bytes(content[1:5], byte_order)
                key = content[5:5 + key_size].decode('utf-8')
                value_size = int.from_bytes(content[5 + key_size:9 + key_size], byte_order)
                value = content[9 + key_size:9 + key_size + value_size].decode('utf-8')
                records.append((key, value, t))
                size = size + len(key) + len(value)
                content = content[9 + key_size + value_size:]
            else:
                key_size = int.from_bytes(content[1:5], byte_order)
                key = content[5:5 + key_size].decode('utf-8')
                records.append((key, '', t))
                size = size + len(key)
                content = content[5 + key_size:]

        batch = WriteBatch()
        batch._sequence_number = seq
        batch._count = count
        batch._batch = records
        batch._size = size
        return batch

    def __eq__(self, other):
        if self._count != other._count or self._sequence_number != other._sequence_number:
            return False
        for i in range(self._count):
            if self._batch[i][0] != other._batch[i][0] or self._batch[i][1] != other._batch[i][1] or self._batch[i][2] != other._batch[i][2]:
                return False
        return True
