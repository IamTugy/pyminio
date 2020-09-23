import os
from os.path import normpath, join
from main import Pyminio


def test_with_pyminio(test_class):
    test_dir = "/test/"

    class TestPyminio(Pyminio):
        """Create minio connection at your local minio,
        but all paths will be in bucket: test."""
        ENDPOINT = os.environ.get('MINIO_CONNECTION_TEST')  
        # for example "localhost:9000"
        # make sure your test minIO docker is using this endpoint as-well.

        def _get_match(self, path: str):
            """Return arguments while bucket is TEST_DIR
                and all the path gets shifted.
            """
            #  if path already starts with: /test/ don't add it.
            if path.startswith(test_dir):
                return super(TestPyminio, self)._get_match(path)

            return super(TestPyminio, self)._get_match(
                join(normpath(test_dir) + path))

    class NewTestClass(test_class):
        def __init__(self, *args, **kwargs):
            super(NewTestClass, self).__init__(*args, **kwargs)
            self._class.name_ = test_class.__name__
            self.pyminio = TestPyminio()

        def setUp(self):
            super(NewTestClass, self).setUp()
            self.pyminio.mkdirs(test_dir)

        def tearDown(self):
            super(NewTestClass, self).tearDown()
            self.pyminio.rmdir(test_dir, recursive=True)

    return NewTestClass

