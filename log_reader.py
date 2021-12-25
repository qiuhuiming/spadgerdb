import zlib

from dbformat import byte_order


class Reader:

    def __init__(self, file_name):
        self._fd = open(file_name, 'rb')
        self.end_of_file = False

    def __del__(self):
        self._fd.close()

    def end(self):
        return self.end_of_file

    def read_record(self) -> bytearray:
        """
        Reads a record from the log file.
        :return: A bytearray object. If the file is end, returns None.
        """
        if self.end():
            return None

        length = int.from_bytes(self._fd.read(4), byteorder=byte_order)
        if length == 0:
            self.end_of_file = True
            return None
        data = bytearray(self._fd.read(length))
        checksum_from_log = int.from_bytes(self._fd.read(4), byteorder=byte_order)

        # Check CRC32 checksum
        to_checked = bytearray(length.to_bytes(4, byteorder=byte_order))
        to_checked.extend(data)
        checksum = zlib.crc32(to_checked)

        if checksum != checksum_from_log:
            raise Exception("Checksum mismatch")

        return data
