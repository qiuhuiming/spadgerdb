import os.path
import shutil
import unittest

from option import DBOption
from version_edit import VersionEdit
from version_set import VersionSet
from test.test_utils import random_user_str


class VersionSetTest(unittest.TestCase):
    def test_basic(self):
        db_name = 'tmp_' + random_user_str(10)
        if os.path.exists(db_name):
            shutil.rmtree(db_name)
        os.mkdir(db_name)

        vs = VersionSet(db_name, DBOption())

        vs.set_last_sequence(100)
        vs.set_log_number(1)
        vs.set_manifest_file_number(2)
        vs.set_prev_log_number(0)
        vs.set_next_file_number(3)

        edit = VersionEdit()
        edit.set_log_number(vs.new_file_number())
        # edit.log_number == 3, and vs.next_file_number == 4

        vs.log_and_apply(edit)

        vs2 = VersionSet(db_name, DBOption())
        vs2.recover()
        self.assertEqual(vs2.last_sequence(), 100)
        self.assertEqual(vs2.log_number(), 3)
        self.assertEqual(vs2.manifest_file_number(), 4)
        self.assertEqual(vs2.prev_log_number(), 0)
        self.assertEqual(vs2.next_file_number(), 5)


if __name__ == '__main__':
    unittest.main()
