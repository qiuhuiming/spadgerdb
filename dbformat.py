
def pack_sequence_and_type(seq: int, t: int) -> bytearray:
    res = bytearray(seq.to_bytes(7, 'little'))
    res.extend(t.to_bytes(1, 'little'))
    return res


# ValueType
kValueTypeForSeek = 0


class LookupKey:
    def __init__(self, user_key: str, sequence: int()):
        length = len(user_key) + 8  # The length of internal key
        self._bytes = bytearray(length.to_bytes(4, 'little'))
        self._bytes.extend(user_key.encode('utf-8'))
        self._bytes.extend(pack_sequence_and_type(sequence, kValueTypeForSeek))
        """
        |<length>|<user_key>|<sequence>|<type> |
        | 4 bytes|<user_key>| 7 bytes  | 1 byte|
        |<-            memtable key          ->|
        |        |<-        internal key     ->|
        """

    def __eq__(self, other):
        return self.key == other.key

    def __hash__(self):
        return hash(self._bytes)

    def memtable_key(self):
        return str(self._bytes, 'utf-8')

    def internal_key(self):
        return str(self._bytes[4:], 'utf-8')

    def user_key(self):
        return str(self._bytes[4:-8], 'utf-8')
