from write_batch import WriteBatch
from option import WriteOption
import zlib
from dbformat import byte_order


class Writer:
    def __init__(self, file_name):
        self._file_name = file_name
        self._fd = open(file_name, 'ab')
        self._write_size = 0

    def __del__(self):
        self._fd.flush()
        self._fd.close()

    def write(self, option: WriteOption, batch: WriteBatch):
        """
        The log writer writes a batch of write operations to the log file.
        The format of the log file is as follows:
        |length: 4 bytes|batch_data|checksum: 4 bytes|
        :param option:
        :param batch:
        :return:
        """
        buffer = bytearray()

        data = batch.serialize()
        length = len(data)
        buffer.extend(length.to_bytes(4, byteorder=byte_order))
        buffer.extend(data)
        checksum = zlib.crc32(buffer)
        buffer.extend(checksum.to_bytes(4, byteorder=byte_order))

        self._fd.write(buffer)
        if option and option.sync:
            self._fd.flush()
        self._write_size += len(buffer)

    def write_size(self) -> int:
        return self._write_size

    def flush(self):
        self._fd.flush()