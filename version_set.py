from option import DBOption
from db_types import SequenceNumber
from status import Status
from typing import List
from version_edit import FileMetaData
from config import MAX_NUM_LEVEL



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


class VersionSet:
    def __init__(self, db_name: str, option: DBOption):
        self._db_name = db_name
        self._option = option
        self._next_file_number = 2
        self._last_sequence: SequenceNumber = 0
        self._log_number = 0

        # VersionSet keeps a linked list of active versions.
        # like this:
        # * - * - * - <_current> - <_dummy>
        # _current is the current version. _current == dummy.prev
        # self.append(version) will insert a new version between _current and _dummy.
        self._current: Version = None
        self._dummy: Version = Version(self)
        self.append(Version(self))

    def current(self) -> Version:
        return self._current

    def recover(self) -> Status:
        return Status.OK()

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

    def get_last_sequence(self) -> int:
        return self._last_sequence

    def set_last_sequence(self, seq: int):
        assert seq >= self._last_sequence
        self._last_sequence = seq
