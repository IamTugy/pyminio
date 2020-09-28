import os
from .main import Pyminio

TEST_DIR = "/test/"


class PyminioTest(Pyminio):
    """Create minio connection at your local minio,
    but all paths will be in bucket: test."""
    ENDPOINT = os.environ.get('MINIO_CONNECTION_TEST')
    # for example "localhost:9000"
    # make sure your test minIO docker is using this endpoint as-well.


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
