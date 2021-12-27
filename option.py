import logging

import utils
from snapshot import Snapshot


class DBOption:
    def __init__(self):
        self.create_if_missing = False
        self.error_if_exists = False
        self.write_buffer_size = 1024 * 1024 * 4
        self.block_size = 4 * 1024
        self.log_level = logging.CRITICAL
        self.log_format = utils.basic_logging_format()

class WriteOption:
    def __init__(self):
        self.sync = False


class ReadOption:
    def __init__(self):
        self.verify_checksums = True
        self.fill_cache = True
        self.snapshot: Snapshot = None
