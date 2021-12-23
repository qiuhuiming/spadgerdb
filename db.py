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
        self._mu = Lock()
        self._versions = VersionSet(db_name=db_name, option=option)

    @staticmethod
    def open(db_name: str, option: DBOption) -> (DB, Status):
        db = DB(db_name, option)
        # TODO: Recover from WAL
        db._mem = MemTable()
        return db, Status.OK()

    def get(self, option: ReadOption, key: str, value: List[str]) -> Status:
        self._mu.acquire()
        s = Status.OK()
        try:
            # Read sequence number from option.
            # If option.snapshot is None, use the last sequence number from versions.
            seq = 0
            if option.snapshot:
                seq = option.snapshot.get_sequence_number()
            else:
                seq = self._versions.get_last_sequence()

            # We can unlock while reading from the DB and then re-lock when doing the next Get.
            self._mu.release()

            lkey = LookupKey(user_key=key, sequence=seq)
            if self._mem.get(lkey, value, s):
                # Done
                pass
            elif self._imm and self._imm.get(lkey, value, s):
                # Done
                pass
            else:
                # TODO: read from persistent storage
                pass
            self._mu.acquire()

            # TODO: schedule to compact the memtable.
        finally:
            self._mu.release()

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

        self._mu.acquire()

        # TODO: Initialize the writers

        # Accquire last sequence number
        last_sequence = self._versions.get_last_sequence()
        batch.set_sequence_number(last_sequence + 1)
        last_sequence += batch.count()

        self._mu.release()
        # TODO: Write to WAL

        # TODO: Write to memtable
        s = batch.apply(mem_table=self._mem)

        self._mu.acquire()

        self._versions.set_last_sequence(last_sequence)

        # TODO: Update writers

        self._mu.release()

        return s
