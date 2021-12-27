import logging
import os.path

from option import DBOption
from status import Status

from dbformat import Decoder, byte_order

DEFAULT_COMPARATOR = 'default_comparator'


def default_comparator(comparator: lambda x, y: int) -> lambda x, y: int:
    if comparator is None:
        return lambda x, y: 0 if x == y else -1 if x < y else 1
    return comparator


USER_KEY_COMPARATOR = 'user_key_comparator'


def user_key_comparator(key_x, key_y) -> int:
    return 0 if key_x == key_y else -1 if key_x < key_y else 1


INTERNAL_KEY_COMPARATOR = 'internal_key_comparator'


def internal_key_comparator(key_x, key_y) -> int:
    """compare internal key

    Order by:
    increasing use_key
    decreasing sequence number
    decreasing type

    Args:
        key_x (internal_key): 
        key_y (internal_key): 

    Returns:
        int: result
    """
    r = user_key_comparator(Decoder.decode_user_key_from_memtable_key(
        key_x), Decoder.decode_user_key_from_memtable_key(key_y))
    if r != 0:
        return r
    tag_x = Decoder.decode_tag_from_memtable_key(key_x)
    tag_y = Decoder.decode_tag_from_memtable_key(key_y)
    seq_x = int.from_bytes(tag_x[:7], byte_order)
    seq_y = int.from_bytes(tag_y[:7], byte_order)
    if seq_x < seq_y:
        return 1
    elif seq_x > seq_y:
        return -1

    type_x = tag_x[7]
    type_y = tag_y[7]
    if type_x < type_y:
        return 1
    elif type_x > type_y:
        return -1

    return 0


def current_file_name(db_name) -> str:
    return os.path.join(db_name, 'CURRENT')


def log_file_name(db_name, log_number: int) -> str:
    return os.path.join(db_name, f'{log_number}.log')


def manifest_file_name(db_name: str, file_number: int) -> str:
    return os.path.join(db_name, f'{file_number}.manifest')


def save_current_file(db_name: str, file_number: int) -> Status:
    current_file = current_file_name(db_name)
    try:
        with open(current_file, 'w') as f:
            f.write(f'{file_number}.manifest')
            f.close()
        return Status.OK()
    except Exception as e:
        return Status.IOError(f'{e}')


def read_logging_level_from_env() -> int:
    env_level = os.getenv('LOG_LEVEL')
    if env_level is None:
        return logging.FATAL
    env_level = env_level.upper()
    if env_level == 'DEBUG':
        return logging.DEBUG
    elif env_level == 'INFO':
        return logging.INFO
    elif env_level == 'WARNING':
        return logging.WARNING
    elif env_level == 'ERROR':
        return logging.ERROR
    elif env_level == 'CRITICAL':
        return logging.CRITICAL
    elif env_level == 'FATAL':
        return logging.FATAL
    elif env_level == 'NOTSET':
        return logging.NOTSET

    return logging.CRITICAL


def basic_logging_format() -> str:
    return '%(asctime)s - %(filename)s:%(lineno)d - %(funcName)s - %(levelname)s: %(message)s'


def get_logger_from_db_option(db_name: str, option: DBOption) -> logging.Logger:
    logger = logging.getLogger(db_name)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter(basic_logging_format()))
    logger.setLevel(option.log_level)
    logger.addHandler(handler)
    logger.propagate = False
    return logger
