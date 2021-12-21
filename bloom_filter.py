class BloomFilter:
    def __init__(self, size, num_item):
        self.size = size
        self.bit_array = [False] * size
        self.num_item = num_item

    def add(self, key):
        for i in range(self.num_item):
            index = self.hash(key, i)
            self.bit_array[index] = True

    def check(self, key):
        """
        (self, object) -> bool
        """
        for i in range(self.num_item):
            index = self.hash(key, i)
            if self.bit_array[index] == False:
                return False
        return True

    def hash(self, key, seed):
        """
        (self, str, int) -> int
        """
        val = hash(hash(key) + hash(seed))
        return val % self.size
