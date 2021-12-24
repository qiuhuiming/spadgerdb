import random
import unittest

from write_batch import WriteBatch


class WriteBatchTest(unittest.TestCase):
    def test_serde(self):
        batch = WriteBatch()
        for i in range(100):
            if random.random() < 0.3:
                batch.delete(str(i))
            else:
                batch.put(str(i), str(i + 1000))
        batch.set_sequence_number(10)

        buffer = batch.serialize()

        batch2 = WriteBatch.deserialize(buffer)

        self.assertEqual(batch, batch2)
