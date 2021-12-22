from option import DBOption


class Version:
    def __init__(self, vs: VersionSet):
        self.version_set = vs
        self.next = self
        self.prev = self


class VersionSet:
    def __init__(self, db_name: str, option: DBOption):
        self._db_name = db_name
        self._option = option
        self._next_file_number = 2
        self._last_sequence = 0
        self._log_number = 0

        # VersionSet keeps a linked list of active versions.
        # like this:
        # * - * - * - <_current> - <_dummy>
        # _current is the current version. _current == dummy.prev
        # self.append(version) will insert a new version between _current and _dummy.
        self._current = None
        self._dummy: Version = Version(self)
        self.append(Version(self))

    def current(self) -> Version:
        return self._current

    def recover(self) -> Status:
        return Status.OK()

    def append(self, version: Version):
        if version is None:
            return
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
