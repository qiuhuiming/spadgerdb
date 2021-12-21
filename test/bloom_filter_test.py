import unittest
from bloom_filter import BloomFilter


class BloomFilterTest(unittest.TestCase):

    def test_hash_seed(self):
        bf = BloomFilter(size=10000, num_item=3)
        seeds = [1, 2, 3]
        hash_values = [bf.hash('a', seed) for seed in seeds]
        self.assertNotEqual(hash_values[0], hash_values[1])
        self.assertNotEqual(hash_values[0], hash_values[2])
        self.assertNotEqual(hash_values[1], hash_values[2])

    def test_basic(self):
        bf = BloomFilter(size=10000, num_item=3)
        keys = ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j']
        for key in keys:
            bf.add(key)
        for key in keys:
            self.assertTrue(bf.check(key))
