import zlib
from dbformat import byte_order


class Writer:
    def __init__(self, file_name):
        self._file_name = file_name
        self._fd = open(file_name, 'ab')
        self._write_size = 0
        self._closed = False

    def write_record(self, data: bytearray):
        """
        The log writer writes a record of bytes to the log file.
        The format of the log file is as follows:
        |length: 4 bytes|batch_data|checksum: 4 bytes|
        :param data: bytearray to write
        :return:
        """
        buffer = bytearray()

        length = len(data)
        buffer.extend(length.to_bytes(4, byteorder=byte_order))
        buffer.extend(data)
        checksum = zlib.crc32(buffer)
        buffer.extend(checksum.to_bytes(4, byteorder=byte_order))

        self._fd.write(buffer)
        self._write_size += len(buffer)

    def write_size(self) -> int:
        return self._write_size

    def flush(self):
        self._fd.flush()

    def close(self):
        self._fd.close()
        self._closed = True

    def closed(self) -> bool:
        return self._closed
