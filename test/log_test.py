import random
import unittest
from log_writer import Writer
from log_reader import Reader
from option import WriteOption
from write_batch import WriteBatch
import os
from test.test_utils import random_user_str


class LogTest(unittest.TestCase):
    def test_basic_1(self):
        try:
            writer = Writer('tmp.log')
            key = random_user_str(100)
            data = bytearray(key.encode('utf-8'))
            writer.write_record(data)

            writer.flush()

            reader = Reader('tmp.log')
            read_data = reader.read_record()
            read_key = read_data.decode('utf-8')
            self.assertEqual(key, read_key)

            self.assertIsNone(reader.read_record())
        finally:
            if os.path.exists('tmp.log'):
                os.remove('tmp.log')

    def test_basic_2(self):
        try:
            writer = Writer('tmp.log')
            keys = [''] * 200
            for i in range(len(keys)):
                keys[i] = random_user_str(100)
                data = bytearray(keys[i].encode('utf-8'))
                writer.write_record(data)

            writer.flush()

            reader = Reader('tmp.log')
            for i in range(len(keys)):
                read_data = reader.read_record()
                read_key = read_data.decode('utf-8')
                self.assertEqual(keys[i], read_key)

            self.assertIsNone(reader.read_record())
        finally:
            if os.path.exists('tmp.log'):
                os.remove('tmp.log')


if __name__ == '__main__':
    unittest.main()
