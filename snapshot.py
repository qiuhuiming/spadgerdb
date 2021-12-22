class Snapshot:
    def __init__(self, sequence_number: int):
        self._sequence_number = sequence_number

    def get_sequence_number(self) -> int:
        return self._sequence_number


class SnapshotList:
    """
    SnapshotList is a linked list of snapshots.
    The newest snapshot is always the prevent element of head.
    Like this:
    <1> - <2> - <3> - <4>(_newest) - <head> - <1>(oldest)
    """

    def __init__(self):
        self._head = Snapshot(0)
        self._head.next = self._head
        self._head.prev = self._head

    def newest(self) -> Snapshot:
        return self._head.prev

    def oldest(self) -> Snapshot:
        return self._head.next

    def empty(self) -> bool:
        return self._head.next == self._head

    def new(self, sequence_number: int) -> Snapshot:
        snapshot = Snapshot(sequence_number)
        self.insert(snapshot)
        return snapshot

    def insert(self, snapshot: Snapshot):
        snapshot.next = self._head
        snapshot.prev = self._head.prev
        snapshot.prev.next = snapshot
        snapshot.next.prev = snapshot

    def delete(self, snapshot: Snapshot):
        snapshot.prev.next = snapshot.next
        snapshot.next.prev = snapshot.prev