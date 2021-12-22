class DBOption:
    def __init__(self):
        self.create_if_missing = False
        self.error_if_exists = False
        self.write_buffer_size = 1024 * 1024 * 4
        self.block_size = 4 * 1024


class WriteOption:
    pass


class ReadOption:
    def __init__(self):
        self.verify_checksums = True
        self.fill_cache = True
        self.snapshot = None
