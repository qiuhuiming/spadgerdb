import random


class Node:
    def __init__(self, key, value, level):
        self.value = value
        self.key = key
        self.level = level
        self.forward = [None] * level


class Skiplist:
    def __init__(self, max_level=32, p=0.25):
        self.head = Node(None, None, 1)
        self.p = p
        self.max_level = max_level
        self._size = 0

    def size(self):
        return self._size

    def search(self, key) -> Node:
        x = self.head
        for l in range(self.head.level, 0, -1):
            i = l - 1
            while x.forward[i] and x.forward[i].key < key:
                x = x.forward[i]
            if x.forward[i] and x.forward[i].key == key:
                return x.forward[i]
        return None

    def contains(self, key):
        return self.search(key) is not None

    def insert(self, key, value):
        new_level = self.random_level()

        update = [None] * max(new_level, self.head.level)
        x = self.head
        for l in range(self.head.level, 0, -1):
            i = l - 1
            while x.forward[i] and x.forward[i].key < key:
                x = x.forward[i]
            if x.forward[i] and x.forward[i].key == key:
                x.forward[i].value = value
                return
            update[i] = x

        if new_level > self.head.level:
            self.head.forward = self.head.forward + \
                [None] * (new_level - self.head.level)
            for i in range(self.head.level + 1, new_level + 1):
                i = i - 1
                update[i] = self.head
            self.head.level = new_level

        new_node = Node(key, value, new_level)
        for i in range(new_level):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node
        self._size += 1

    def delete(self, key):
        update = [None] * self.head.level
        x = self.head
        for l in range(self.head.level, 0, -1):
            i = l - 1
            while x.forward[i] and x.forward[i].key < key:
                x = x.forward[i]
            update[i] = x

        if x.forward[0] and x.forward[0].key == key:
            for l in range(self.head.level, 0, -1):
                i = l - 1
                if update[i].forward[i] and update[i].forward[i].key == key:
                    update[i].forward[i] = update[i].forward[i].forward[i]
            while self.head.level > 0 and self.head.forward[self.head.level - 1] is None:
                self.head.level -= 1
                self.head.forward.pop(-1)

            self._size -= 1

    def random_level(self):
        level = 1
        while random.random() < self.p and level < self.max_level:
            level += 1
        return level

    def __iter__(self):
        x = self.head.forward[0]
        while x:
            yield x
            x = x.forward[0]

    def get_iterator(self):
        return Iterator(self)


class Iterator:
    def __init__(self, s: Skiplist):
        self._skiplist = s
        self._current = s.head

    def __next__(self):
        if self._current.forward[0] is None:
            raise StopIteration
        self._current = self._current.forward[0]
        return self._current

    def next(self):
        if self._current:
            self._current = self._current.forward[0]
        return self._current

    def __iter__(self):
        x = self._skiplist.head.forward[0]
        while x:
            yield x
            x = x.forward[0]

    def find_greater_or_equal(self, key) -> Node:
        x = self._skiplist.head
        for l in range(self._skiplist.head.level, 0, -1):
            i = l - 1
            while x.forward[i] and x.forward[i].key < key:
                x = x.forward[i]
        self._current = x.forward[0]
        return x.forward[0]

    def find_last_less(self, key) -> Node:
        x = self._skiplist.head
        for l in range(self._skiplist.head.level, 0, -1):
            i = l - 1
            while x.forward[i] and x.forward[i].key < key:
                x = x.forward[i]
        self._current = x
        return x

    def get_current(self) -> Node:
        return self._current

    def reset(self):
        self._current = self._skiplist.head
