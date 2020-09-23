import os
import re
from datetime import datetime
from io import StringIO
from collections import namedtuple
from os.path import normpath, join, basename


import pytz
from minio import Minio
from attrdict import AttrDict
from minio.error import NoSuchKey, BucketNotEmpty

ROOT = "/"

TIMEZONE = pytz.timezone("Asia/Jerusalem")

File = namedtuple('File', ['name', 'full_path', 'data', 'metadata'])
Match = namedtuple('Match', ['bucket', 'prefix', 'filename'])


def _validate_directory(func):
    def decorated_method(self, path, *args, **kwargs):
        if path != ROOT:
            match = self.PATH_STRUCTURE.match(path)
            if match is None:
                raise ValueError(f'{path} is not a valid path')

            if match.group('filename') is not None:
                raise ValueError(
                    f"{path} is not a valid directory path. must be absolute and"
                    " end with /"
                )

        return func(self, path, *args, **kwargs)

    return decorated_method


def get_last_modified(obj):
    if obj.last_modified is None:
        return TIMEZONE.localize(datetime.fromtimestamp(0))
    return obj.last_modified


def get_creation_date(obj):
    if obj.creation_date is None:
        return TIMEZONE.localize(datetime.fromtimestamp(0))
    return obj.creation_date


class ObjectStorage(object):
    # get from file
    PATH_STRUCTURE = \
        re.compile(r"/(?P<bucket>.*?)/(?P<prefix>.*/)?(?P<filename>.+[^/])?")

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

    def _get_match(self, path):
        """Get the bucket name, path prefix and file's name from path.

        Returns:
            tuple(str, str, str) ->
                bucket name, path without filename and bucket name, file's name
        """
        if path == ROOT:
            return Match(bucket='', prefix=None, filename=None)

        match = self.PATH_STRUCTURE.match(path)
        if not match:
            raise ValueError(f'{path} is not a valid path')

        return Match(bucket=match.group("bucket"),
                     prefix=match.group("prefix"),
                     filename=match.group("filename"))

    @_validate_directory
    def mkdirs(self, path):
        bucket, directory_path, _ = self._get_match(path)

        if bucket == '':
            raise ValueError("cannot create / directory")

        #  make bucket
        if not self.minio_obj.bucket_exists(bucket_name=bucket):
            self.minio_obj.make_bucket(bucket_name=bucket)

        if directory_path is None:
            #  path is only bucket
            return

        #  make sub directories (minio is making all path)
        empty_file = StringIO()
        self.minio_obj.put_object(bucket_name=bucket,
                                  object_name=directory_path,
                                  data=empty_file, length=0)

    def _get_objects_at(self, bucket, directory_path):
        return sorted(self.minio_obj.list_objects(bucket_name=bucket,
                                                  prefix=directory_path),
                      key=get_last_modified, reverse=True)

    def _get_buckets(self):
        return sorted(self.minio_obj.list_buckets(),
                      key=get_creation_date, reverse=True)

    @classmethod
    def _get_relative_path(cls, directory_path, file_name):
        if directory_path is None:
            if file_name is None:
                return ''

            return file_name

        if file_name is None:
            return directory_path

        return join(directory_path, file_name)

    @classmethod
    def _extract_metadata(cls, detailed_metadata):
        """Remove 'X-Amz-Meta-' from al the keys, and lowercase them.
            When metadata is pushed in the minio, the minio is adding
            those details that screw us. this is an unscrewing function.
        """
        return {key.replace('X-Amz-Meta-', '').lower(): value
                for key, value in detailed_metadata.items()}

    @_validate_directory
    def listdir(self, path, only_files=False):
        """Return all files and directories absolute paths
            within the directory path.

        Args:
            path(str): path of a directory.
            only_files(bool): return only files name and not directories.

        Returns:
            list. files and directories in path.
        """
        bucket, directory_path, _ = self._get_match(path)

        if directory_path is None:
            directory_path = ''

        if bucket == '':
            if only_files:
                return []

            return [f"{b.name}/" for b in self._get_buckets()]

        return [obj.object_name.replace(directory_path, "")
                for obj in self._get_objects_at(bucket, directory_path)
                if not only_files or not obj.is_dir]

    def isdir(self, path):
        _, _, filename = self._get_match(path)
        return self.exists(path) and filename is None

    def exists(self, path):
        bucket, directory_path, filename = self._get_match(path)

        if bucket == '':
            return True

        bucket_exists = self.minio_obj.bucket_exists(bucket)
        if not bucket_exists:
            return False

        relative_path = self._get_relative_path(directory_path, filename)
        try:
            self.minio_obj.get_object(bucket, relative_path)

        except NoSuchKey:
            return False

        return True

    @_validate_directory
    def rmdir(self, path, recursive=False):
        bucket, directory_path, _ = self._get_match(path)

        if bucket == '':
            raise ValueError(f"cannot remove root ('{ROOT}')")

        file_objects = self._get_objects_at(bucket, directory_path)

        if len(file_objects) > 0:
            if not recursive:
                raise RuntimeError("Directory is not empty")

            files = [file_obj.object_name
                     for file_obj in file_objects if not file_obj.is_dir]

            # list activates remove
            list(self.minio_obj.remove_objects(bucket, files))

            dirs = [file_obj.object_name
                    for file_obj in file_objects if file_obj.is_dir]

            for directory in dirs:
                self.rmdir(join(ROOT, bucket, directory), recursive=True)

        if directory_path is None:
            try:
                self.minio_obj.remove_bucket(bucket)

            except BucketNotEmpty:
                raise RuntimeError("Directory is not empty")

        else:
            self.minio_obj.remove_object(bucket, directory_path)

    def rm(self, path, recursive=False):
        if recursive:
            return self.rmdir(path, recursive=True)

        if self.isdir(path):
            raise ValueError(f"cannot remove {path}: "
                             "Is a directory")

        bucket, directory_path, filename = self._get_match(path)

        relative_path = self._get_relative_path(directory_path, filename)
        self.minio_obj.remove_object(bucket, relative_path)

    def cp(self, from_path, to_path):
        if self.isdir(from_path):
            raise NotImplementedError("currently not supported")

        from_bucket, from_directory_path, from_filename = self._get_match(
            from_path
        )
        to_bucket, to_directory_path, to_filename = self._get_match(to_path)

        relative_from_path = self._get_relative_path(from_directory_path,
                                                     from_filename)

        if to_filename is None:
            to_filename = from_filename

        relative_to_path = self._get_relative_path(to_directory_path,
                                                   to_filename)

        self.minio_obj.copy_object(to_bucket, relative_to_path,
                                   join(from_bucket,
                                        relative_from_path))

    def mv(self, from_path, to_path):
        self.cp(from_path, to_path)
        self.rm(from_path)

    def get(self, path):
        bucket, directory_path, filename = self._get_match(path)
        relative_path = self._get_relative_path(directory_path, filename)

        try:

            data = self.minio_obj.get_object(bucket, relative_path).data
            details = self.minio_obj.stat_object(bucket, relative_path)
            details_metadata = self._extract_metadata(details.metadata)

            metadata = {
                "is_dir": details.is_dir,
                "last_modified": details.last_modified,
                "size": details.size
            }
            metadata.update(details_metadata)

            return File(name=filename, full_path=path, data=data,
                        metadata=AttrDict(metadata))

        except NoSuchKey:
            raise RuntimeError(f"cannot access {path}: "
                               "No such file or directory")

    def put_data(self, path, data, metadata=None):
        bucket, prefix, filename = self._get_match(path)
        data_file = data
        if isinstance(data, basestring):
            data_file = StringIO()
            data_file.write(data)

        file_metadata = self._get_metadata(data_file, metadata)

        self.minio_obj.put_object(bucket_name=bucket,
                                  object_name=f'{prefix}{filename}',
                                  data=data_file,
                                  length=len(data),
                                  metadata=file_metadata)
        data_file.close()

    def put_file(self, path, file_path, metadata=None):
        bucket, prefix, filename = self._get_match(path)

        if filename is None:
            filename = basename(file_path)

        with open(file_path, 'rb') as file_pointer:
            file_metadata = self._get_metadata(file_pointer, metadata)

        relative_path = self._get_relative_path(prefix, filename)
        self.minio_obj.fput_object(bucket, relative_path,
                                   file_path, metadata=file_metadata)

    @classmethod
    def _get_metadata(cls, file_pointer, metadata):
        file_metadata = {}  #TODO: review this.

        if metadata is not None:
            file_metadata.update(metadata)

        return file_metadata

    @_validate_directory
    def get_last_object(self, path):
        bucket, directory_path, _ = self._get_match(path)
        objects_names_in_dir = self.listdir(path, only_files=True)
        if len(objects_names_in_dir) == 0:
            return None

        last_object_name = objects_names_in_dir[0]
        relative_path = self._get_relative_path(directory_path,
                                                last_object_name)
        new_path = join(ROOT, bucket, relative_path)

        return self.get(new_path)
