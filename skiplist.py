import random
from typing import List
from utils import default_comparator


class Node:
    def __init__(self, key, value, level):
        self.value = value
        self.key = key
        self.level = level
        self.forward = [None] * level


class Skiplist:
    def __init__(self, max_level=32, p=0.25, comparator=None):
        self._head = Node(None, None, 1)
        self.p = p
        self.max_level = max_level
        self._size = 0
        self._comparator = default_comparator(comparator)

    def _compare(self, key_x, key_y) -> int:
        """
        Returns -1 if x < y, 1 if x > y, and 0 if x == y.
        """
        return self._comparator(key_x, key_y)

    def size(self):
        return self._size

    def search(self, key) -> Node:
        x = self._head
        for l in range(self._head.level, 0, -1):
            i = l - 1
            while x.forward[i] and self._compare(x.forward[i].key, key) < 0:
                x = x.forward[i]
            if x.forward[i] and self._compare(x.forward[i].key, key) == 0:
                return x.forward[i]
        return None

    def contains(self, key):
        return self.search(key) is not None

    def insert(self, key, value=None):
        new_level = self._random_level()

        update = [None] * max(new_level, self._head.level)
        x = self._head
        for l in range(self._head.level, 0, -1):
            i = l - 1
            while x.forward[i] and self._compare(x.forward[i].key, key) < 0:
                x = x.forward[i]
            if x.forward[i] and self._compare(x.forward[i].key, key) == 0:
                x.forward[i].value = value
                return
            update[i] = x

        if new_level > self._head.level:
            self._head.forward = self._head.forward + \
                [None] * (new_level - self._head.level)
            for i in range(self._head.level + 1, new_level + 1):
                i = i - 1
                update[i] = self._head
            self._head.level = new_level

        new_node = Node(key, value, new_level)
        for i in range(new_level):
            new_node.forward[i] = update[i].forward[i]
            update[i].forward[i] = new_node
        self._size += 1

    def delete(self, key):
        update = [None] * self._head.level
        x = self._head
        for l in range(self._head.level, 0, -1):
            i = l - 1
            while x.forward[i] and self._compare(x.forward[i].key, key) < 0:
                x = x.forward[i]
            update[i] = x

        if x.forward[0] and self._compare(x.forward[i].key, key) == 0:
            for l in range(self._head.level, 0, -1):
                i = l - 1
                if update[i].forward[i] and update[i].forward[i].key == key:
                    update[i].forward[i] = update[i].forward[i].forward[i]
            while self._head.level > 0 and self._head.forward[self._head.level - 1] is None:
                self._head.level -= 1
                self._head.forward.pop(-1)

            self._size -= 1

    def _random_level(self) -> int:
        level = 1
        while random.random() < self.p and level < self.max_level:
            level += 1
        return level

    def __iter__(self):
        x = self._head.forward[0]
        while x:
            yield x
            x = x.forward[0]

    def find_last(self) -> Node:
        x = self._head
        l = self._head.level - 1
        while True:
            if x.forward[l] is None:
                if l == 0:
                    return x
                else:
                    l -= -1
            else:
                x = x.forward[l]

    def iter(self):
        return Iterator(self)

    def find_less_than(self, key) -> Node:
        x = self._head
        for l in range(self._head.level, 0, -1):
            i = l - 1
            while x.forward[i] and x.forward[i].key < key:
                x = x.forward[i]
        return x

    def find_greater_or_equal(self, key, need_prev=False) -> (Node, List[Node]):
        """[summary]
        Returns the node with the smallest key >= key, and the list of prevent nodes
        that are less than the key for each level.
        """
        x = self._head
        l = self._head.level - 1
        if need_prev:
            prev = [None] * self._head.level

        for l in range(self._head.level, 0, -1):
            i = l - 1
            while x.forward[i] and self._compare(x.forward[i].key, key) < 0:
                x = x.forward[i]
            if need_prev:
                prev[i] = x

        if x == self._head:
            x = None
        else:
            x = x.forward[0]

        if need_prev:
            return x, prev
        else:
            return x, None

    def head_level(self):
        return self._head.level


class Iterator:
    def __init__(self, s: Skiplist):
        self._skiplist = s
        self._current = s._head

    def __next__(self):
        if self._current.forward[0] is None:
            raise StopIteration
        self._current = self._current.forward[0]
        return self._current

    def valid(self) -> bool:
        return self._current is not None

    def next(self):
        assert(self.valid())
        self._current = self._current.forward[0]

    def prev(self):
        assert(self.valid())
        self._current = self._skiplist.find_less_than(key=self._current.key)
        if self._current == self._skiplist._head:
            self._current = None

    def __iter__(self):
        x = self._skiplist._head.forward[0]
        while x:
            yield x
            x = x.forward[0]

    def current(self) -> Node:
        return self._current

    def key(self):
        assert self.valid()
        return self._current.key

    def seek(self, key):
        self._current, _ = self._skiplist.find_greater_or_equal(
            key, need_prev=False)

    def seek_to_first(self):
        self._current = self._skiplist._head.forward[0]

    def seek_to_last(self):
        self._current = self._skiplist.find_last()
        if self._current == self._skiplist._head:
            self._current = None
