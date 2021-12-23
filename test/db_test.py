import unittest
from option import DBOption, WriteOption, ReadOption
from db import DB
from status import Status
from test.utils import random_user_str
from typing import Dict, Set
import random
from write_batch import WriteBatch


class DBTest(unittest.TestCase):
    def test_basic(self):
        db, s = DB.open("Foo", DBOption())
        self.assertEqual(s, Status.OK())

        s = db.put(WriteOption(), "foo", "bar")
        self.assertEqual(s, Status.OK())
        value = []
        s = db.get(ReadOption(), "foo", value)
        self.assertEqual(s, Status.OK())
        self.assertEqual(len(value), 1)
        self.assertEqual(value[0], 'bar')

        db.delete(WriteOption(), 'foo')
        value = []
        s = db.get(ReadOption(), "foo", value)
        self.assertEqual(s, Status.NotFound())

        self.assertEqual(db._versions.get_last_sequence(), 2)

    def test_basic_2(self):
        db, s = DB.open("Foo", DBOption())
        self.assertEqual(s, Status.OK())
        map: Dict[str, str] = {}
        for round in range(50):
            round_put_map: Dict[str, str] = {}
            round_delete_set: Set[str] = set()
            for i in range(20):
                if len(map) > 0 and random.random() < 0.2:
                    # Delete a key from the map
                    key = random.choice(list(map.keys()))

                    round_delete_set.add(key)
                    del map[key]
                    if key in round_put_map:
                        del round_put_map[key]

                    s = db.delete(WriteOption(), key)
                    self.assertEqual(s, Status.OK())
                elif len(map) > 0 and random.random() < 0.5:
                    # Update a key in the map
                    key = random.choice(list(map.keys()))

                    value = random_user_str(10)
                    map[key] = value
                    round_put_map[key] = value
                    if key in round_delete_set:
                        round_delete_set.remove(key)

                    s = db.put(WriteOption(), key, value)
                    self.assertEqual(s, Status.OK())
                else:
                    key = random_user_str(10)
                    value = random_user_str(10)

                    map[key] = value
                    round_put_map[key] = value
                    if key in round_delete_set:
                        round_delete_set.remove(key)

                    s = db.put(WriteOption(), key, value)
                    self.assertEqual(s, Status.OK())

            for (k, v) in round_put_map.items():
                value = []
                s = db.get(ReadOption(), k, value)
                self.assertEqual(s, Status.OK())
                self.assertEqual(len(value), 1)
                self.assertEqual(value[0], v)

            for k in round_delete_set:
                value = []
                s = db.get(ReadOption(), k, value)
                self.assertEqual(s, Status.NotFound())

        for (k, v) in map.items():
            value = []
            s = db.get(ReadOption(), k, value)
            self.assertEqual(s, Status.OK())
            self.assertEqual(len(value), 1)
            self.assertEqual(value[0], v)

    def test_batch(self):
        db, s = DB.open("Foo", DBOption())
        self.assertEqual(s, Status.OK())
        map: Dict[str, str] = {}
        for round in range(50):
            round_put_map: Dict[str, str] = {}
            round_delete_set: Set[str] = set()
            batch = WriteBatch()
            for i in range(20):
                if len(map) > 0 and random.random() < 0.2:
                    # Delete a key from the map
                    key = random.choice(list(map.keys()))

                    round_delete_set.add(key)
                    del map[key]
                    if key in round_put_map:
                        del round_put_map[key]

                    batch.delete(key)
                elif len(map) > 0 and random.random() < 0.33:
                    # Update a key in the map
                    key = random.choice(list(map.keys()))

                    value = random_user_str(10)
                    map[key] = value
                    round_put_map[key] = value
                    if key in round_delete_set:
                        round_delete_set.remove(key)

                    batch.put(key, value)
                else:
                    key = random_user_str(10)
                    value = random_user_str(10)

                    map[key] = value
                    round_put_map[key] = value
                    if key in round_delete_set:
                        round_delete_set.remove(key)

                    batch.put(key, value)

            s = db.write(WriteOption(), batch)
            self.assertEqual(s, Status.OK())

            for (k, v) in round_put_map.items():
                value = []
                s = db.get(ReadOption(), k, value)
                self.assertEqual(s, Status.OK())
                self.assertEqual(len(value), 1)
                self.assertEqual(value[0], v)

            for k in round_delete_set:
                value = []
                s = db.get(ReadOption(), k, value)
                self.assertEqual(s, Status.NotFound())

        for (k, v) in map.items():
            value = []
            s = db.get(ReadOption(), k, value)
            self.assertEqual(s, Status.OK())
            self.assertEqual(len(value), 1)
            self.assertEqual(value[0], v)
