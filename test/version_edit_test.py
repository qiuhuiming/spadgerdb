import unittest
from version_edit import VersionEdit, FileMetaData


class VersionEditTest(unittest.TestCase):
    def test_serialize(self):
        edit = VersionEdit()
        edit.log_number = 1
        edit.has_log_number = True
        edit.comparator = 'bar'
        edit.has_comparator = True
        edit.deleted_files.add((1, 1))
        edit.new_files.append((1, FileMetaData()))
        edit.compact_pointers.append((1, bytearray('far'.encode('utf-8'))))

        bin_data = edit.serialize()
        edit2 = VersionEdit.deserialize(bin_data)

        self.assertEqual(edit.log_number, edit2.log_number)
        self.assertEqual(edit.comparator, edit2.comparator)
        self.assertEqual(edit.has_log_number, edit2.has_log_number)
        self.assertEqual(edit.has_comparator, edit2.has_comparator)
        self.assertEqual(len(edit2.deleted_files), 1)
        self.assertTrue((1, 1) in edit2.deleted_files)
        self.assertTrue((1, bytearray('far'.encode('utf-8'))) in edit2.compact_pointers)


if __name__ == '__main__':
    unittest.main()
