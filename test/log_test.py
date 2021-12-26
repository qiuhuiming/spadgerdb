import unittest
from log_writer import Writer
from log_reader import Reader
import os
from test.test_utils import random_user_str


class LogTest(unittest.TestCase):
    def test_basic_1(self):
        try:
            log_name = f'tmp_{random_user_str(10)}'
            writer = Writer(log_name)
            key = random_user_str(100)
            data = bytearray(key.encode('utf-8'))
            writer.write_record(data)

            writer.flush()
            writer.close()

            reader = Reader(log_name)
            read_data = reader.read_record()
            read_key = read_data.decode('utf-8')
            self.assertEqual(key, read_key)

            self.assertIsNone(reader.read_record())
            self.assertTrue(reader.closed())

        finally:
            if os.path.exists(log_name):
                os.remove(log_name)

    def test_basic_2(self):
        try:
            log_name = f'tmp_{random_user_str(10)}'
            writer = Writer(log_name)
            keys = [''] * 200
            for i in range(len(keys)):
                keys[i] = random_user_str(100)
                data = bytearray(keys[i].encode('utf-8'))
                writer.write_record(data)

            writer.flush()
            writer.closed()

            reader = Reader(log_name)
            for i in range(len(keys)):
                read_data = reader.read_record()
                read_key = read_data.decode('utf-8')
                self.assertEqual(keys[i], read_key)

            self.assertIsNone(reader.read_record())
            self.assertTrue(reader.closed())
        finally:
            if os.path.exists(log_name):
                os.remove(log_name)


if __name__ == '__main__':
    unittest.main()
