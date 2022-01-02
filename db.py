import logging
import os.path

import utils
from status import Status
from option import ReadOption, WriteOption, DBOption
from version_edit import VersionEdit
from version_set import VersionSet
from dbformat import LookupKey
from memtable import MemTable
from write_batch import WriteBatch
from typing import List
from log_writer import Writer
from log_reader import Reader
from utils import log_file_name, current_file_name, USER_KEY_COMPARATOR, manifest_file_name, save_current_file

logging.basicConfig(level=logging.CRITICAL)


class DB:
    def __init__(self, db_name: str, option: DBOption):
        self._mem: MemTable = None
        self._imm: MemTable = None
        self._has_imm = False
        self._log_file_num: int = None
        self._db_name = db_name
        self._option = option
        self.versions = VersionSet(db_name=db_name, option=option)
        self.writer: Writer = None
        self._logger = utils.get_logger_from_db_option(db_name, option.log_level)

    @staticmethod
    def open(db_name: str, option: DBOption) -> ('DB', Status):
        db = DB(db_name, option)
        db._logger.info('open db %s' % db_name)
        s, should_save_manifest = db.recover()
        if not s.ok():
            return None, s

        edit = VersionEdit()
        if db._mem is None:
            new_log_number = db.versions.new_file_number()
            try:
                writer = Writer(log_file_name(db_name, new_log_number))
            except Exception as e:
                return None, Status.IOError(str(e))

            edit.set_log_number(new_log_number)
            db.writer = writer
            db._mem = MemTable()
            db._log_file_num = new_log_number

        if should_save_manifest:
            db._logger.info('save manifest when opening db %s' % db_name)
            edit.set_prev_log_number(0)
            edit.set_log_number(db._log_file_num)
            s = db.versions.log_and_apply(edit)
            if not s.ok():
                return None, s

        return db, Status.OK()

    def recover(self) -> (Status, bool):
        s = Status.OK()
        should_save_manifest = False

        if not os.path.exists(self._db_name):
            os.mkdir(self._db_name)
        # Maybe we need lock the file

        if not os.path.exists(current_file_name(self._db_name)):
            if not self._option.create_if_missing:
                s = Status.InvalidArgument('DB not exist')
                return s, should_save_manifest
            else:
                s = self.new_db()
                if not s.ok():
                    return s, should_save_manifest
        else:
            if self._option.error_if_exists:
                s = Status.InvalidArgument('DB already exist')
                return s, should_save_manifest

        s, should_save_manifest = self.versions.recover()
        if not s.ok():
            return s, should_save_manifest

        max_sequence = 0

        min_log_number = self.versions.log_number()
        prev_log_number = self.versions.prev_log_number()
        filenames = os.listdir(self._db_name)
        # Here we do not check whether there is missing file for simplicity.

        log_numbers = []
        for name in filenames:
            if name.endswith('.log'):
                log_number = int(name[:-4])
                if log_number >= min_log_number or log_number == prev_log_number:
                    log_numbers.append(log_number)

        log_numbers.sort()
        self._logger.info('recover log numbers: len=%s' % len(log_numbers))
        self._logger.debug('recover log numbers: detail=%s' % log_numbers)

        for log_number in log_numbers:
            s, max_sequence, should_save_manifest = self.recover_log_file(log_number)
            if not s.ok():
                return s, should_save_manifest

            self.versions.mark_file_number_used(log_number)
        self.versions.set_last_sequence(max(max_sequence, self.versions.last_sequence()))

        return s, should_save_manifest

    def get(self, option: ReadOption, key: str, value: List[str]) -> Status:
        s = Status.NotFound()
        # Read sequence number from option.
        # If option.snapshot is None, use the last sequence number from versions.
        seq = 0
        if option.snapshot:
            seq = option.snapshot.get_sequence_number()
        else:
            seq = self.versions.last_sequence()

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

        # Acquire last sequence number
        last_sequence = self.versions.last_sequence()
        batch.set_sequence_number(last_sequence + 1)
        last_sequence += batch.count()

        s = self.make_room_for_write(force=(batch is None))
        self.writer.write_record(batch.serialize())

        # Write to memtable
        s = batch.apply(mem_table=self._mem)

        # Update last sequence number
        self.versions.set_last_sequence(last_sequence)

        return s

    def new_db(self) -> Status:
        self._logger.info('new db %s' % self._db_name)
        if not os.path.exists(self._db_name):
            os.mkdir(self._db_name)

        edit = VersionEdit()
        edit.set_log_number(0)
        edit.set_next_file_number(2)
        edit.set_last_sequence(0)
        edit.set_comparator(USER_KEY_COMPARATOR)

        manifest = manifest_file_name(self._db_name, 1)
        writer = Writer(manifest)
        records = edit.serialize()
        writer.write_record(records)
        writer.flush()
        writer.close()

        s = save_current_file(self._db_name, 1)
        self._logger.info('saved manifest file %s in new_db' % manifest)
        if not s.ok():
            if os.path.exists(manifest):
                os.remove(manifest)
            return s

        return Status.OK()

    def recover_log_file(self, log_number: int) -> (Status, int, bool):
        s = Status.OK()
        max_sequence = 0
        should_save_manifest = False

        path = log_file_name(self._db_name, log_number)
        if not os.path.exists(path):
            return Status.IOError('Log file not exist'), max_sequence, should_save_manifest

        reader = Reader(path)
        while True:
            record = reader.read_record()
            if record is None:
                break
            try:
                batch = WriteBatch.deserialize(record)
                assert batch.count() is not None and batch.sequence_number() is not None
            except Exception as e:
                return Status.SerdeError(str(e)), max_sequence, should_save_manifest

            last_seq = batch.sequence_number() + batch.count() - 1

            if self._mem is None:
                self._mem = MemTable()

            s = batch.apply(mem_table=self._mem)
            if not s.ok():
                return s, max_sequence, should_save_manifest

            max_sequence = max(max_sequence, last_seq)

        reader.close()

        # TODO: schedule to compact the memtable.
        return Status.OK(), max_sequence, should_save_manifest

    def last_sequence(self) -> int:
        return self.versions.last_sequence()

    def close(self):
        if self.writer and not self.writer.closed():
            self.writer.close()

        if self.versions:
            self.versions.close()

    def log_number(self) -> int:
        return self._log_file_num

    def make_room_for_write(self, force: bool) -> Status:
        if self._mem is None:
            # New memtable
            self._mem = MemTable()

        if self.log_number() is None:
            # New WAL file
            if self.writer:
                self.writer.close()
            self._log_file_num = self.versions.new_file_number()
            self.writer = Writer(log_file_name(self._db_name, self._log_file_num))

        if self._option.only_mem:
            return Status.OK()

        if self._mem.approximate_memory_usage() < self._option.write_buffer_size:
            # There is room for writing, we do not need to force
            return Status.OK()
        elif self._imm is not None:
            # We have filled up the current memtable, but the previous one is not
            # being compacted, so we can wait for the previous one to be compacted.
            # TODO: At this time, we raise an exception.
            raise Exception('imm is not None')
        else:
            # Switch to a new memtable
            self._logger.info("switch to a new memtable")
            self._imm = self._mem
            self._mem = MemTable()
            self._has_imm = True
            self._logger = self.versions.new_file_number()
            self.writer.close()
            self.writer = Writer(log_file_name(self._db_name, self._log_file_num))
            self.maybe_schedule_compaction()
            return Status.OK()

    def maybe_schedule_compaction(self):
        # TODO
        pass



