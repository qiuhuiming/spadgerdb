import os.path
import unittest

import utils
from option import DBOption, WriteOption, ReadOption
from db import DB
from typing import List
from status import Status
from test.test_utils import random_user_str
from typing import Dict, Set
import random
from write_batch import WriteBatch
from utils import current_file_name, manifest_file_name


class DBTest(unittest.TestCase):
    def test_new(self):
        db_name = f'tmp_{random_user_str(10)}'
        db_option = DBOption()
        db_option.create_if_missing = True
        db = DB(db_name, db_option)
        s = db.new_db()
        self.assertEqual(s, Status.OK())

        self.assertTrue(os.path.exists(current_file_name(db_name)))
        self.assertTrue(os.path.exists(manifest_file_name(db_name, 1)))

    def test_open_new(self):
        db_name = f'tmp_{random_user_str(10)}'
        db_option = DBOption()
        db_option.create_if_missing = True
        db, s = DB.open(db_name, db_option)
        self.assertEqual(s, Status.OK())

        self.assertEqual(db.versions.last_sequence(), 0)
        self.assertEqual(db.versions.log_number(), 0)
        self.assertEqual(db._log_file_num, 3)
        self.assertEqual(db.versions.manifest_file_number(), 2)
        self.assertEqual(db.versions.next_file_number(), 4)
        self.assertEqual(db.versions.prev_log_number(), 0)

        self.assertIsNotNone(db._mem)

        db.close()

    def test_basic_mem(self):
        option = DBOption()
        option.create_if_missing = True
        db, s = DB.open(f'tmp_{random_user_str(10)}', option)
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

        self.assertEqual(db.versions.last_sequence(), 2)
        db.close()

    def test_basic_2_mem(self):
        option = DBOption()
        option.create_if_missing = True
        option.write_buffer_size = 1024 * 1024 * 1024 * 4
        db, s = DB.open(f'tmp_{random_user_str(10)}', option)
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

        db.close()

    def test_batch_mem(self):
        option = DBOption()
        option.create_if_missing = True
        db, s = DB.open(f'tmp_{random_user_str(10)}', option)
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
        db.close()

    def test_recover_basic(self):
        db_name = f'tmp_{random_user_str(10)}'
        db_option = DBOption()
        db_option.create_if_missing = True
        db, s = DB.open(db_name, db_option)
        self.assertEqual(s, Status.OK())
        self.assertEqual(db.versions.manifest_file_number(), 2)
        self.assertEqual(db.versions.log_number(), 0)
        self.assertEqual(db._log_file_num, 3)
        db.close()

        db2, s = DB.open(db_name, db_option)
        self.assertEqual(s, Status.OK())
        self.assertEqual(db2.versions.manifest_file_number(), 2)
        self.assertEqual(db2.versions.log_number(), 0)
        self.assertEqual(db2._log_file_num, 4)
        db2.close()

    def test_recover_wal(self):
        db_name = f'tmp_{random_user_str(10)}'
        db_option = DBOption()
        db_option.create_if_missing = True
        db_option.write_buffer_size = 1024 * 1024 * 1024 * 4
        db, s = DB.open(db_name, db_option)
        self.assertEqual(s, Status.OK())

        data = {}
        for i in range(10):
            key = random_user_str(10)
            value = random_user_str(10)
            db.put(WriteOption(), key, value)
            data[key] = value

        db.close()

        db2, s = DB.open(db_name, db_option)
        self.assertEqual(s, Status.OK())
        for (k, v) in data.items():
            value = []
            s = db2.get(ReadOption(), k, value)
            self.assertEqual(s, Status.OK())
            self.assertEqual(len(value), 1)
            self.assertEqual(value[0], v)

        db2.close()

    def test_recover_wal_many_times(self):
        db_name = f'tmp_{random_user_str(10)}'
        db_option = DBOption()
        db_option.create_if_missing = True
        db_option.log_level = utils.read_logging_level_from_env()
        db_option.write_buffer_size = 1024 * 1024 * 1024 * 4
        crash_times = 100
        dbs: List[DB] = [None] * crash_times

        data = {}
        for i in range(crash_times):
            dbs[i], s = DB.open(db_name, db_option)
            self.assertEqual(s, Status.OK())

            for j in range(20):
                key = random_user_str(20)
                value = random_user_str(20)
                data[key] = value
                dbs[i].put(WriteOption(), key, value)
                self.assertIsNone(dbs[i]._imm)

            if i < crash_times - 1:
                dbs[i].close()

        db = dbs[crash_times - 1]
        for (k, v) in data.items():
            value = []
            s = db.get(ReadOption(), k, value)
            self.assertEqual(s, Status.OK())
            self.assertEqual(len(value), 1)
            self.assertEqual(value[0], v)
        db.close()

    def test_recover_wal_many_times_with_batches(self):
        db_name = f'tmp_{random_user_str(10)}'
        db_option = DBOption()
        db_option.create_if_missing = True
        db_option.write_buffer_size = 1024 * 1024 * 1024 * 4
        crash_times = 100
        dbs: List[DB] = [None] * crash_times

        data = {}
        for i in range(crash_times):
            dbs[i], s = DB.open(db_name, db_option)
            self.assertEqual(s, Status.OK())

            for batch_index in range(5):
                batch_size = random.randint(1, 5)
                batch = WriteBatch()
                for operation_index in range(batch_size):
                    if False and len(data) > 0 and random.random() < 0.2:
                        key = random.choice(list(data.keys()))
                        batch.delete(key)
                        del data[key]
                    else:
                        key = random_user_str(20)
                        value = random_user_str(20)
                        data[key] = value
                        batch.put(key, value)
                dbs[i].write(WriteOption(), batch)
                self.assertIsNone(dbs[i]._imm)
            if i < crash_times - 1:
                dbs[i].close()

        db = dbs[crash_times - 1]
        for (k, v) in data.items():
            value = []
            s = db.get(ReadOption(), k, value)
            self.assertEqual(s, Status.OK())
            self.assertEqual(len(value), 1)
            self.assertEqual(value[0], v)
        db.close()

    def test_only_mem(self):
        db_name = f'tmp_{random_user_str(10)}'
        db_option = DBOption()
        db_option.create_if_missing = True
        db_option.write_buffer_size = 1024
        db_option.only_mem = True
        db, s = DB.open(db_name, db_option)
        self.assertTrue(s.ok())

        def mem_table_key_len(key, value):
            return len(key) + len(value) + 4 + 8 + 4

        len_sum = 0
        data = {}
        while True:
            key = random_user_str(100)
            value = random_user_str(100)
            db.put(WriteOption(), key, value)
            data[key] = value
            len_sum += mem_table_key_len(key, value)
            if len_sum >= 1200:
                break

        self.assertFalse(db._has_imm)
        self.assertIsNone(db._imm)

        for (k, v) in data.items():
            value = []
            s = db.get(ReadOption(), k, value)
            self.assertTrue(s.ok())
            self.assertEqual(len(value), 1)
            self.assertEqual(value[0], v)

        db.close()


    def test_switch_to_imm(self):
        db_name = f'tmp_{random_user_str(10)}'
        db_option = DBOption()
        db_option.create_if_missing = True
        db_option.write_buffer_size = 1024
        db, s = DB.open(db_name, db_option)
        self.assertTrue(s.ok())

        def mem_table_key_len(key, value):
            return len(key) + len(value) + 4 + 8 + 4

        len_sum = 0
        data = {}
        while True:
            key = random_user_str(100)
            value = random_user_str(100)
            db.put(WriteOption(), key, value)
            data[key] = value
            len_sum += mem_table_key_len(key, value)
            if len_sum >= 1200:
                break

        self.assertTrue(db._has_imm)
        self.assertIsNotNone(db._imm)

        for (k, v) in data.items():
            value = []
            s = db.get(ReadOption(), k, value)
            self.assertTrue(s.ok())
            self.assertEqual(len(value), 1)
            self.assertEqual(value[0], v)

        db.close()
