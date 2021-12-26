import os.path

import config
import utils
from log_writer import Writer
from version_edit import VersionEdit
from option import DBOption
from db_types import SequenceNumber
from status import Status
from typing import List
from version_edit import FileMetaData
from config import MAX_NUM_LEVEL
from utils import current_file_name, USER_KEY_COMPARATOR
from log_reader import Reader


class Version:
    def __init__(self, vs: 'VersionSet'):
        self.version_set = vs
        self.next = self
        self.prev = self
        self.file_to_compact: List[List[FileMetaData]] = [[]] * MAX_NUM_LEVEL
        self.file_to_compact_level: int = -1
        self._ref = 0

    def ref(self):
        self._ref += 1

    def unref(self):
        assert self._ref > 0
        self._ref -= 1
        if self._ref == 0:
            self.prev.next = self.next
            self.next.prev = self.prev

    def get_ref(self) -> int:
        return self._ref


class VersionBuilder:
    def __init__(self, vs: 'VersionSet', base: Version):
        self._version_set = vs
        self._base = base
        # TODO: more status about metadata of files

    def apply(self, edit: VersionEdit):
        pass

    def build(self, v: Version):
        # TODO: do something like adding files to version
        pass


class VersionSet:
    def __init__(self, db_name: str, option: DBOption):
        self._db_name = db_name
        self._option = option
        self._next_file_number = 2
        self._prev_log_number = 0
        self._last_sequence: SequenceNumber = 0
        self._manifest_file_number = 0
        self._log_number = 0

        # The writer to write manifest
        self._descriptor_log: Writer = None

        # VersionSet keeps a linked list of active versions.
        # like this:
        # * - * - * - <_current> - <_dummy>
        # _current is the current version. _current == dummy.prev
        # self.append(version) will insert a new version between _current and _dummy.
        self._current: Version = None
        self._dummy: Version = Version(self)
        self.append(Version(self))

    def next_file_number(self) -> int:
        return self._next_file_number

    def prev_log_number(self) -> int:
        return self._prev_log_number

    def last_sequence(self) -> SequenceNumber:
        return self._last_sequence

    def manifest_file_number(self) -> int:
        return self._manifest_file_number

    def log_number(self) -> int:
        return self._log_number

    def set_next_file_number(self, number: int):
        self._next_file_number = number

    def set_prev_log_number(self, number: int):
        self._prev_log_number = number

    def set_last_sequence(self, seq: int):
        assert seq >= self._last_sequence
        self._last_sequence = seq

    def set_log_number(self, number: int):
        self._log_number = number

    def set_manifest_file_number(self, number: int):
        self._manifest_file_number = number

    def current(self) -> Version:
        return self._current

    def recover(self) -> (Status, bool):
        """
        recover from persistent storage
        1. read from current file
        2. read from manifest file
        3. for each record in manifest file
            1. deserialize it to a VersionEdit
            2. update metadata
            3. apply each version edit to VersionBuilder
        4. build the version and append it to the version set
        :return: status, should_save_manifest
        """
        s = Status.OK()
        should_save_manifest = False

        current_path = current_file_name(self._db_name)
        if not os.path.exists(current_path):
            s = Status.Corruption('current file not found')
            return s, should_save_manifest

        current = ''
        with open(current_path, 'r') as current_fd:
            current = current_fd.readline()
            if current == '':
                s = Status.Corruption('current file is empty')
                return s, should_save_manifest

        manifest_path = os.path.join(self._db_name, current)
        if not os.path.exists(manifest_path):
            s = Status.Corruption('manifest file not found')
            return s, should_save_manifest

        has_log_number = False
        has_prev_log_number = False
        has_last_sequence_number = False
        has_next_file_number = False
        log_number = 0
        prev_log_number = 0
        last_sequence_number = 0
        next_file_number = 0
        reads_records = 0
        builder = VersionBuilder(self, self._current)

        reader = Reader(manifest_path)
        while True:
            record = reader.read_record()
            if record is None:
                break
            edit = VersionEdit.deserialize(record)

            if edit.has_comparator:
                if edit.comparator != USER_KEY_COMPARATOR:
                    s = Status.InvalidArgument(f'{edit.comparator} is not match {USER_KEY_COMPARATOR}')
                    return s, should_save_manifest

            builder.apply(edit)

            if edit.has_log_number:
                has_log_number = True
                log_number = edit.log_number

            if edit.has_prev_log_number:
                has_prev_log_number = True
                prev_log_number = edit.prev_log_number

            if edit.has_last_sequence:
                has_last_sequence_number = True
                last_sequence_number = edit.last_sequence

            if edit.has_next_file_number:
                has_next_file_number = True
                next_file_number = edit.next_file_number
        del reader

        if not has_log_number:
            s = Status.Corruption('log number not found')
            return s, should_save_manifest
        elif not has_next_file_number:
            s = Status.Corruption('next file number not found')
            return s, should_save_manifest
        elif not has_last_sequence_number:
            s = Status.Corruption('last sequence number not found')
            return s, should_save_manifest

        if not has_last_sequence_number:
            last_sequence_number = 0

        self.mark_file_number_used(log_number)
        self.mark_file_number_used(prev_log_number)

        version = Version(self)
        builder.build(version)
        self.append(version)
        self._manifest_file_number = next_file_number
        self._next_file_number = next_file_number + 1
        self._last_sequence = last_sequence_number
        self._log_number = log_number
        self._prev_log_number = prev_log_number

        if os.path.getsize(manifest_path) > config.MAX_REUSED_MANIFEST_SIZE:
            should_save_manifest = True

        return Status.OK(), should_save_manifest

    def append(self, version: Version):
        assert version.get_ref() == 0
        assert self._current != version

        if version is None:
            return
        if self._current is not None:
            self._current.unref()
        version.ref()
        self._current = version
        version.next = self._dummy
        version.prev = self._dummy.prev
        version.prev.next = version
        version.next.prev = version

    def mark_file_number_used(self, log_number: int):
        if self._next_file_number <= log_number:
            self._next_file_number = log_number + 1

    def log_and_apply(self, edit: VersionEdit) -> Status:
        """
        save version edit to log file and apply it to current version set

        :param edit:
        :return:
        """
        # We can not apply to the current version if it is not the last version
        if edit.has_log_number:
            assert edit.log_number >= self._log_number
            assert edit.log_number < self._next_file_number
        else:
            edit.set_log_number(self.log_number())

        if not edit.has_prev_log_number:
            edit.set_prev_log_number(self.prev_log_number())

        edit.set_next_file_number(self._next_file_number)
        edit.set_last_sequence(self._last_sequence)

        # Build the new version based on the current version
        v = Version(self)
        builder = VersionBuilder(self, self.current())
        builder.apply(edit)
        builder.build(v)

        if self._descriptor_log is None:
            # The descriptor_log does not exist, create it.
            # And we need to write the snapshot to the descriptor_log
            manifest = utils.manifest_file_name(self._db_name, self._manifest_file_number)
            self._descriptor_log = Writer(manifest)
            s, snapshot = self.build_snapshot()
            if not s.ok():
                return s
            print('write snapshot to descriptor_log')
            self._descriptor_log.write_record(snapshot)

        # Write the version edit to the descriptor_log
        record = edit.serialize()
        self._descriptor_log.write_record(record)
        self._descriptor_log.flush()
        print('write version edit to descriptor_log')
        s = utils.save_current_file(self._db_name, self._manifest_file_number)
        if not s.ok():
            manifest = utils.manifest_file_name(self._db_name, self._manifest_file_number)
            if os.path.exists(manifest):
                os.remove(manifest)
            self._descriptor_log.close()
            return s

        # Accept the new version
        self.append(v)
        self._log_number = edit.log_number
        self._prev_log_number = edit.prev_log_number
        print('install new version. log_number: {}, prev_log_number: {}'.format(self._log_number, self._prev_log_number))

        return Status.OK()

    def build_snapshot(self) -> (Status, bytearray):
        edit = VersionEdit()
        edit.set_comparator(USER_KEY_COMPARATOR)

        # TODO: Save compact pointers and files
        return Status.OK(), edit.serialize()

    def new_file_number(self) -> int:
        """
        Allocate a new file number
        :return old file number
        """
        ret = self._next_file_number
        self._next_file_number += 1
        return ret

    def close(self):
        if self._descriptor_log is not None:
            self._descriptor_log.close()
            self._descriptor_log = None
