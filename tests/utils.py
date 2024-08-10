from os import environ

from minio import Minio

from pyminio.main import Pyminio

TEST_DIR = "/test/"


class PyminioTest(Pyminio):
    """Create minio connection at your local minio,
    but all paths will be in bucket: test."""

    ENDPOINT = environ["MINIO_TEST_CONNECTION"]
    ACCESS_KEY = environ["MINIO_TEST_ACCESS_KEY"]
    SECRET_KEY = environ["MINIO_TEST_SECRET_KEY"]

    def __init__(self: "PyminioTest") -> None:
        if None in (self.ENDPOINT, self.ACCESS_KEY, self.SECRET_KEY):
            raise ValueError(
                "Must define 'MINIO_TEST_CONNECTION', 'MINIO_TEST_ACCESS_KEY', "
                "'MINIO_TEST_SECRET_KEY' to run tests"
            )

        super().__init__(
            minio_obj=Minio(
                endpoint=self.ENDPOINT,
                access_key=self.ACCESS_KEY,
                secret_key=self.SECRET_KEY,
                secure=False,
            )
        )
