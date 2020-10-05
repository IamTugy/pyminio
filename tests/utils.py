from os import environ

from minio import Minio
from pyminio.main import Pyminio

TEST_DIR = "/test/"


class PyminioTest(Pyminio):
    """Create minio connection at your local minio,
    but all paths will be in bucket: test."""

    ENDPOINT = environ.get('MINIO_TEST_CONNECTION')
    ACCESS_KEY = environ.get('MINIO_TEST_ACCESS_KEY')
    SECRET_KEY = environ.get('MINIO_TEST_SECRET_KEY')

    def __init__(self):
        super().__init__(minio_obj=Minio(
            endpoint=self.ENDPOINT,
            access_key=self.ACCESS_KEY,
            secret_key=self.SECRET_KEY,
            secure=False
        ))


def test_with_pyminio(test_class):
    class NewTestClass(test_class):
        def __init__(self, *args, **kwargs):
            super(NewTestClass, self).__init__(*args, **kwargs)
            self._class.name_ = test_class.__name__
            self.pyminio = PyminioTest()

        def setUp(self):
            super(NewTestClass, self).setUp()
            self.pyminio.mkdirs(TEST_DIR)

        def tearDown(self):
            super(NewTestClass, self).tearDown()
            self.pyminio.rmdir(TEST_DIR, recursive=True)

    return NewTestClass
