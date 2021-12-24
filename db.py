from status import Status
from threading import Lock
from option import ReadOption, WriteOption, DBOption
from version import VersionSet
from dbformat import LookupKey
from memtable import MemTable
from write_batch import WriteBatch
from typing import List


class DB:
    pass


class DB:
    def __init__(self, db_name: str, option: DBOption):
        self._mem: MemTable = None
        self._imm: MemTable = None
        self._has_imm = False
        self._log_file = None
        self._log_file_num = None
        self._db_name = db_name
        self._option = option
        self._versions = VersionSet(db_name=db_name, option=option)

    @staticmethod
    def open(db_name: str, option: DBOption) -> (DB, Status):
        db = DB(db_name, option)
        # TODO: Recover from WAL
        db._mem = MemTable()
        return db, Status.OK()

    def get(self, option: ReadOption, key: str, value: List[str]) -> Status:
        s = Status.OK()
        # Read sequence number from option.
        # If option.snapshot is None, use the last sequence number from versions.
        seq = 0
        if option.snapshot:
            seq = option.snapshot.get_sequence_number()
        else:
            seq = self._versions.get_last_sequence()

        lkey = LookupKey(user_key=key, sequence=seq)
        # Firstly, try to get from memtable.
        if self._mem.get(lkey, value, s):
            # Done
            pass
        # If not found, try to get from immutable memtable.
        elif self._imm and self._imm.get(lkey, value, s):
            # Done
            pass
        # If not found, try to get from SSTable.
        # TODO: Read from persistent storage
        else:
            pass

        # TODO: schedule to compact the memtable.

        return s

    def put(self, option: WriteOption, key, value) -> Status:
        batch = WriteBatch()
        batch.put(key, value)
        return self.write(option, batch)

    def delete(self, option: WriteOption, key) -> Status:
        batch = WriteBatch()
        batch.delete(key)
        return self.write(option, batch)

    def write(self, option: WriteOption, batch: WriteBatch) -> Status:
        s = Status.OK()

        # TODO: Initialize the writers

        # Accquire last sequence number
        last_sequence = self._versions.get_last_sequence()
        batch.set_sequence_number(last_sequence + 1)
        last_sequence += batch.count()

        # TODO: Write to WAL

        # TODO: Write to memtable
        s = batch.apply(mem_table=self._mem)

        self._versions.set_last_sequence(last_sequence)

        # TODO: Update writers

        return s
