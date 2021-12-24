import random
import unittest
from log_writer import Writer
from log_reader import Reader
from option import WriteOption
from write_batch import WriteBatch
import os
from test.test_utils import random_user_str


class LogTest(unittest.TestCase):
    def test_basic(self):
        try:
            writer = Writer('tmp.log')
            batch = WriteBatch()
            batch.put('key1', 'value1')
            batch.set_sequence_number(1)

            writer.write(WriteOption(), batch)
            writer.flush()

            reader = Reader('tmp.log')
            batch2 = reader.read_batch()
            self.assertEqual(batch, batch2)

            self.assertIsNone(reader.read_batch())
        finally:
            if os.path.exists('tmp.log'):
                os.remove('tmp.log')

    def test_basic_2(self):
        try:
            writer = Writer('tmp.log')
            batch = WriteBatch()
            for i in range(200):
                key = random_user_str(10)
                value = random_user_str(10)
                if random.random() < 0.8:
                    batch.put(key, value)
                else:
                    batch.delete(key)
            batch.set_sequence_number(1)

            writer.write(WriteOption(), batch)
            writer.flush()

            reader = Reader('tmp.log')
            batch2 = reader.read_batch()
            self.assertEqual(batch, batch2)

            self.assertIsNone(reader.read_batch())
        finally:
            if os.path.exists('tmp.log'):
                os.remove('tmp.log')

    def test_basic_3(self):
        try:
            writer = Writer('tmp.log')
            batches = []
            seq = 10
            for batch_i in range(200):
                op_num = random.randint(1, 10)
                for i in range(op_num):
                    batch = WriteBatch()
                    key = random_user_str(10)
                    value = random_user_str(10)
                    if random.random() < 0.8:
                        batch.put(key, value)
                    else:
                        batch.delete(key)
                batch.set_sequence_number(seq)
                seq += op_num
                writer.write(WriteOption(), batch)
                batches.append(batch)
            writer.flush()

            reader = Reader('tmp.log')
            for batch_i in range(len(batches)):
                batch = reader.read_batch()
                self.assertIsNotNone(batch)
                self.assertEqual(batches[batch_i], batch)

            self.assertIsNone(reader.read_batch())
        finally:
            if os.path.exists('tmp.log'):
                os.remove('tmp.log')


if __name__ == '__main__':
    unittest.main()
