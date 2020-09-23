class ObjectStorage(object):

    ENDPOINT = os.environ.get('MINIO_CONNECTION')  # for example "localhost:9000"
    ACCESS_KEY = os.environ.get('MINIO_ACCESS_KEY')
    SECRET_KEY = os.environ.get('MINIO_SECRET_KEY')

    def __init__(self, endpoint=None, access_key=None,
                 secret_key=None):

        self._endpoint = endpoint or self.ENDPOINT
        self._access_key = access_key or self.ACCESS_KEY
        self._secret_key = secret_key or self.SECRET_KEY

        self.minio_obj = Minio(endpoint=self._endpoint,
                               access_key=self._access_key,
                               secret_key=self._secret_key,
                               secure=False)