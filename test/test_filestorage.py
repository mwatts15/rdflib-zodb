import unittest
from pow_zodb.FileStorageZODB import FileStorageZODBStore


class FileStorageOpenTest(unittest.TestCase):
    def setUp(self):
        self.cut = FileStorageZODBStore()

    def test_dict_configuration_no_url_fail(self):
        with self.assertRaisesRegex(ValueError, 'url'):
            self.cut.open(dict())

    def test_wrong_type_type_error(self):
        class NotAConfiguration(object):
            def __str__(self):
                return 'not a config'

        with self.assertRaisesRegex(TypeError, 'not a config'):
            self.cut.open(NotAConfiguration())
