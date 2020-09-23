from os.path import normpath, join
from minioOS import ObjectStorage


def test_with_object_storage(test_class):
    test_dir = "/test/"

    class TestObjectStorage(ObjectStorage):
        """Create minio connection at your local minio,
        but all paths will be in bucket: test."""
        ENDPOINT = os.environ.get('MINIO_CONNECTION_TEST')  # for example "localhost:9000"
        # make sure your test minIO docker is using this endpoint as-well.

        def _get_match(self, path):
            """Return arguments while bucket is TEST_DIR
                and all the path gets shifted.
            """
            #  if path already starts with: /test/ don't add it.
            if path.startswith(test_dir):
                return super(TestObjectStorage, self)._get_match(path)

            return super(TestObjectStorage, self)._get_match(
                join(normpath(test_dir) + path))

    class NewTestClass(test_class):
        def __init__(self, *args, **kwargs):
            super(NewTestClass, self).__init__(*args, **kwargs)
            self._class.name_ = test_class.__name__
            self.object_storage = TestObjectStorage()

        def setUp(self):
            super(NewTestClass, self).setUp()
            self.object_storage.mkdirs(test_dir)

        def tearDown(self):
            super(NewTestClass, self).tearDown()
            self.object_storage.rmdir(test_dir, recursive=True)

    return NewTestClas
