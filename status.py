OK_CODE = 0
NOT_FOUND_CODE = 1
IO_ERROR_CODE = 2
SERDE_ERROR_CODE = 3
INVALID_ARGUMENT_CODE = 4


def status_str(status_code) -> str:
    if status_code == OK_CODE:
        return 'OK'
    elif status_code == NOT_FOUND_CODE:
        return 'NOT_FOUND'
    elif status_code == IO_ERROR_CODE:
        return 'IO_ERROR'
    elif status_code == SERDE_ERROR_CODE:
        return 'SERDE_ERROR'
    elif status_code == INVALID_ARGUMENT_CODE:
        return 'INVALID_ARGUMENT'
    else:
        return 'UNKNOWN'


class Status:
    def __init__(self, code, msg):
        self.code = code
        self.msg = msg

    def __str__(self):
        return 'Status code: ' + str(self.code) + ' ' + self.msg

    def __repr__(self):
        return self.__str__()

    def __eq__(self, other):
        if isinstance(other, Status):
            return self.code == other.code and self.msg == other.msg
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    def ok(self) -> bool:
        return self.code == OK_CODE

    @staticmethod
    def OK():
        return Status(OK_CODE, 'OK')

    @staticmethod
    def NotFound(msg):
        return Status(NOT_FOUND_CODE, msg)

    @staticmethod
    def IOError(msg):
        return Status(IO_ERROR_CODE, msg)

    @staticmethod
    def SerdeError(msg):
        return Status(SERDE_ERROR_CODE, msg)

    @staticmethod
    def InvalidArgument(msg):
        return Status(INVALID_ARGUMENT_CODE, msg)
