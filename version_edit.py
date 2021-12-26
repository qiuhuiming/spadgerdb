import json

from typing import List, Tuple, Set


class FileMetaData:
    def __init__(self):
        self.allow_seek = 1 << 30
        self.file_size = 0
        self.smallest_key: bytearray = bytearray()
        self.greatest_key: bytearray = bytearray()
        self.number = 0

    def serialize(self) -> bytearray:
        # bytearray Object is not JSON serializable, so we need to convert it to a string.
        json_map = self.__dict__.copy()
        json_map['smallest_key'] = json_map['smallest_key'].decode('utf-8')
        json_map['greatest_key'] = json_map['greatest_key'].decode('utf-8')
        return bytearray(json.dumps(json_map).encode('utf-8'))

    @staticmethod
    def deserialize(data: bytearray) -> 'FileMetaData':
        json_map = json.loads(data.decode('utf-8'))
        meta_data = FileMetaData()
        meta_data.__dict__ = json_map
        meta_data.smallest_key = bytearray(meta_data.smallest_key.encode('utf-8'))
        meta_data.greatest_key = bytearray(meta_data.greatest_key.encode('utf-8'))

        return meta_data


class VersionEdit:
    def __init__(self):
        self.comparator = ''
        self.log_number = 0
        self.prev_log_number = 0
        self.next_file_number = 0
        self.last_sequence = 0
        self.has_comparator = False
        self.has_log_number = False
        self.has_prev_log_number = False
        self.has_next_file_number = False
        self.has_last_sequence = False
        self.clear()

        # List[Tuple[level, InternalKey]]
        self.compact_pointers: List[Tuple[int, bytearray]] = []
        self.deleted_files: Set[Tuple[int, int]] = set()
        self.new_files: List[Tuple[int, FileMetaData]] = []

    def clear(self):
        self.comparator = ''
        self.log_number = 0
        self.prev_log_number = 0
        self.next_file_number = 0
        self.last_sequence = 0
        self.has_comparator = False
        self.has_log_number = False
        self.has_prev_log_number = False
        self.has_next_file_number = False
        self.has_last_sequence = False
        self.deleted_files = []
        self.new_files = []

    def serialize(self) -> bytearray:
        # For simplicity, we use json to serialize the edit. The result is a byte array of utf-8 encoded json.
        # Because some fields are not serialized, we need to manually add them.
        json_map = self.__dict__.copy()
        json_map['compact_pointers'] = list(map(lambda x: (x[0], x[1].decode('utf-8')), json_map['compact_pointers']))
        json_map['deleted_files'] = list(map(lambda x: (x[0], x[1]), json_map['deleted_files']))
        json_map['new_files'] = list(map(lambda x: (x[0], x[1].serialize().decode('utf-8')), json_map['new_files']))
        json_str = json.dumps(json_map)
        return bytearray(json_str.encode('utf-8'))

    @staticmethod
    def deserialize(data: bytearray) -> 'VersionEdit':
        json_str = data.decode('utf-8')
        edit = VersionEdit()
        edit.__dict__ = json.loads(json_str)
        edit.compact_pointers = list(map(lambda x: (x[0], bytearray(x[1].encode('utf-8'))), edit.compact_pointers))
        edit.deleted_files = set(map(lambda x: (x[0], x[1]), edit.deleted_files))
        edit.new_files = list(
            map(lambda x: (x[0], FileMetaData.deserialize(bytearray(x[1].encode('utf-8')))), edit.new_files))

        return edit

    def set_log_number(self, log_number: int):
        self.log_number = log_number
        self.has_log_number = True

    def set_prev_log_number(self, prev_log_number: int):
        self.prev_log_number = prev_log_number
        self.has_prev_log_number = True

    def set_next_file_number(self, next_file_number: int):
        self.next_file_number = next_file_number
        self.has_next_file_number = True

    def set_last_sequence(self, last_sequence: int):
        self.last_sequence = last_sequence
        self.has_last_sequence = True

    def set_comparator(self, comparator: str):
        self.comparator = comparator
        self.has_comparator = True
