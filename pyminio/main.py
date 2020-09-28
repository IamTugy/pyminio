import re

from os import environ
from io import BytesIO
from datetime import datetime
from dataclasses import dataclass
from collections import namedtuple
from typing import List, Dict, Union, Any
from os.path import join, basename, dirname, normpath

import pytz

from minio import Minio
from attrdict import AttrDict
from minio.error import NoSuchKey, BucketNotEmpty

ROOT = "/"

PATH_STRUCTURE = \
        re.compile(r"/(?P<bucket>.*?)/+(?P<prefix>.*/+)?(?P<filename>.+[^/])?")


@dataclass
class ObjectData:
    name: str
    full_path: str
    metadata: Dict[str, Any]


@dataclass
class File(ObjectData):
    data: bytes


@dataclass
class Folder(ObjectData):
    pass


Match = namedtuple('Match', ['bucket', 'prefix', 'filename'])


def extract_match(path: str) -> Match:
    """Get the bucket name, path prefix and file's name from path.

    Returns:
        Match. bucket name, path without filename and bucket name,
               file's name.
    """
    path = re.sub(r'/+', r'/', path)

    if path == ROOT:
        return Match(bucket='', prefix=None, filename=None)

    match = PATH_STRUCTURE.match(path)

    if match is None:
        raise ValueError(f'{path} is not a valid path')

    return Match(bucket=match.group("bucket"),
                 prefix=match.group("prefix"),
                 filename=match.group("filename"))


def _validate_directory(func):
    """Check if directory path is valid. """
    def decorated_method(self, path: str, *args, **kwargs):
        if path != ROOT:
            match = extract_match(path)

            if match.filename is not None:
                raise ValueError(f"{path} is not a valid directory path."
                                 " must be absolute and end with /")

        return func(self, path, *args, **kwargs)

    return decorated_method


def get_last_modified(obj):
    """Return object's last modified time. """
    if obj.last_modified is None:
        return pytz.UTC.localize(datetime.fromtimestamp(0))

    return obj.last_modified


def get_creation_date(obj):
    """Return object's creation date. """
    if obj.creation_date is None:
        return pytz.UTC.localize(datetime.fromtimestamp(0))
    return obj.creation_date


class Pyminio:
    ENDPOINT = environ.get('MINIO_CONNECTION')
    # for example "localhost:9000"
    ACCESS_KEY = environ.get('MINIO_ACCESS_KEY')
    SECRET_KEY = environ.get('MINIO_SECRET_KEY')

    def __init__(self, endpoint=None, access_key=None,
                 secret_key=None, minio_client=Minio):

        self._endpoint = endpoint or self.ENDPOINT
        self._access_key = access_key or self.ACCESS_KEY
        self._secret_key = secret_key or self.SECRET_KEY

        self.minio_obj = minio_client(endpoint=self._endpoint,
                                      access_key=self._access_key,
                                      secret_key=self._secret_key,
                                      secure=False)

    @_validate_directory
    def mkdirs(self, path: str):
        """Create path of directories.

            Works like linux's: 'mkdir -p'.

        Args:
            path: The absolute path to create.
        """
        bucket, directory_path, _ = extract_match(path)

        if bucket == '':
            raise ValueError("cannot create / directory")

        #  make bucket
        if not self.minio_obj.bucket_exists(bucket_name=bucket):
            self.minio_obj.make_bucket(bucket_name=bucket)

        if directory_path is None:
            #  path is only bucket
            return

        #  make sub directories (minio is making all path)
        empty_file = BytesIO()
        self.minio_obj.put_object(bucket_name=bucket,
                                  object_name=directory_path,
                                  data=empty_file, length=0)

    def _get_objects_at(self, bucket: str, directory_path: str):
        """Return all objects in the specified bucket and directory path.

        Args:
            bucket: The bucket desired in minio.
            directory_path: full directory path inside the bucket.
        """
        return sorted(self.minio_obj.list_objects(bucket_name=bucket,
                                                  prefix=directory_path),
                      key=get_last_modified, reverse=True)

    @_validate_directory
    def get_objects_at(self, path: str):
        """Return all objects in the specified path.

        Args:
            path: path of a directory.
        """
        bucket, directory_path, _ = extract_match(path)
        return self._get_objects_at(bucket, directory_path)

    def _get_buckets(self):
        """Return all existed buckets. """
        return sorted(self.minio_obj.list_buckets(),
                      key=get_creation_date, reverse=True)

    @classmethod
    def _get_relative_path(cls, directory_path: str, file_name: str):
        """Return as relative path.

        Args:
            directory_path: full directory path inside the bucket.
            file_name: The desired file's name.
        """
        directory_path = directory_path or ''
        file_name = file_name or ''
        return join(directory_path, file_name)

    @classmethod
    def _extract_metadata(cls, detailed_metadata: Dict):
        """Remove 'X-Amz-Meta-' from all the keys, and lowercase them.
            When metadata is pushed in the minio, the minio is adding
            those details that screw us. this is an unscrewing function.
        """
        detailed_metadata = detailed_metadata or {}
        return {key.replace('X-Amz-Meta-', '').lower(): value
                for key, value in detailed_metadata.items()}

    @_validate_directory
    def listdir(self, path: str, only_files: bool = False) -> List[str]:
        """Return all files and directories absolute paths
            within the directory path.

            Works like os.listdir, just only with absolute path.

        Args:
            path: path of a directory.
            only_files: return only files name and not directories.

        Returns:
            files and directories in path.
        """
        bucket, directory_path, _ = extract_match(path)

        directory_path = directory_path or ''
        if bucket == '':
            if only_files:
                return []

            return [f"{b.name}/" for b in self._get_buckets()]

        return [obj.object_name.replace(directory_path, '')
                for obj in self._get_objects_at(bucket, directory_path)
                if not only_files or not obj.is_dir]

    def exists(self, path: str) -> bool:
        """Check if the specified path exists.

            Works like os.path.exists.
        """
        try:
            bucket, directory_path, filename = extract_match(path)

        except ValueError:
            return False

        if bucket == '':
            return True

        bucket_exists = self.minio_obj.bucket_exists(bucket)
        if not bucket_exists:
            return False

        relative_path = self._get_relative_path(directory_path, filename)

        if relative_path == '':
            return True
        try:
            self.get(path)

        except RuntimeError:
            return False

        return True

    def isdir(self, path: str):
        """Check if the specified path is a directory.

            Works like os.path.isdir
        """
        _, _, filename = extract_match(path)
        return self.exists(path) and filename is None

    @_validate_directory
    def rmdir(self, path: str, recursive: bool = False):
        """Remove specified directory.
            If recursive flag is used, remove all content recursively.

            Works like linux's rmdir (-r).

        Args:
            path: path of a directory.
            recursive: remove content recursively.
        """
        bucket, directory_path, _ = extract_match(path)

        if bucket == '':
            for bucket in self.listdir(ROOT):
                self.rmdir(join(ROOT, bucket), recursive)
            return

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

    def rm(self, path: str, recursive: bool = False):
        """Remove specified directory or file.
            If recursive flag is used, remove all content recursively.

            Works like linux's rm (-r).

        Args:
            path: path of a directory or a file.
            recursive: remove content recursively.
        """
        if self.isdir(path):
            return self.rmdir(path, recursive=recursive)

        bucket, directory_path, filename = extract_match(path)
        relative_path = self._get_relative_path(directory_path, filename)
        self.minio_obj.remove_object(bucket, relative_path)

    # def _copy_directory(self, from_path: str, to_path: str):
    #     objects = self.get_objects_at(from_path)
    #     files = []
    #     directories = []
    #     for obj in objects:
    #         if obj.is_dir:
    #             directories.append(obj)
    #         else:
    #             files.append(obj)
    #     if directories == []:
    #         pass  # make path
    #         # self.mkdirs(join(to_path, dirname(from_path), ''))
    #     else:
    #         #  recursive run with next directories
    #         for directory in directories:
    #             self._copy_directory(
    #                 from_path=,
    #                 to_path=join(to_path, basename(directory))
    #     for file_to_copy in files:
    #         #  create file

    def cp(self, from_path: str, to_path: str, recursive: bool = False):
        """Copy files from one directory to another.

            If to_path will be a path to a dictionary, the name will be
            the copied file name. if it will be a path with a file name,
            the name of the file will be this file's name.


            Works like linux's cp (-r).

        Args:
            from_path: source path to a file.
            to_path: destination path.
            recursive: copy content recursively.
        """
        from_bucket, from_directory_path, from_filename = extract_match(
            from_path
        )

        if from_filename is None and not recursive:
            raise RuntimeError("cannot copy folder unrecursively")

        to_bucket, to_directory_path, to_filename = extract_match(to_path)

        relative_from_path = self._get_relative_path(from_directory_path,
                                                     from_filename)

        if to_filename is None:
            to_filename = from_filename

        relative_to_path = self._get_relative_path(to_directory_path,
                                                   to_filename)

        self.minio_obj.copy_object(to_bucket, relative_to_path,
                                   join(from_bucket,
                                        relative_from_path))

    def mv(self, from_path: str, to_path: str):
        """Move files from one directory to another.

            Works like linux's mv.

        Args:
            from_path: source path.
            to_path: destination path.
        """
        self.cp(from_path, to_path)
        self.rm(from_path)

    def get(self, path: str) -> Union[File, Folder]:
        """Get file or directory from minio.

        Args:
            path: path of a directory or a file.
        """
        bucket, directory_path, filename = extract_match(path)
        relative_path = self._get_relative_path(directory_path, filename)
        kwargs = AttrDict()

        if relative_path == '':
            raise ValueError('cannot get first folder (bucket) '
                             'duo to minio limitations.')
        try:
            if filename is not None:
                kwargs.data = self.minio_obj.get_object(
                    bucket, relative_path).data

                details = self.minio_obj.stat_object(bucket, relative_path)
                name = filename
                return_obj = File

            else:
                parent_directory = join(dirname(normpath(directory_path)), '')
                objects = self.minio_obj.list_objects(
                    bucket_name=bucket,
                    prefix=parent_directory
                )

                details = next(filter(
                    lambda obj: obj.object_name == relative_path, objects))
                name = join(basename(normpath(details.object_name)), '')
                return_obj = Folder

        except (NoSuchKey, StopIteration):
            raise RuntimeError(f"cannot access {path}: "
                               "No such file or directory")

        details_metadata = \
            self._extract_metadata(details.metadata)

        metadata = {
            "is_dir": details.is_dir,
            "last_modified": details.last_modified,
            "size": details.size
        }
        metadata.update(details_metadata)

        return return_obj(name=name, full_path=path,
                          metadata=AttrDict(metadata), **kwargs)

    def put_data(self, path: str, data: bytes,
                 metadata: Dict = None):
        """Put data in file inside a minio folder.

        Args:
            path: destination of the new file with its name in minio.
            data: the data that the file will contain in bytes.
            metadata: metadata dictionary to append the file.
        """
        bucket, prefix, filename = extract_match(path)
        data_file = BytesIO(data)

        file_metadata = self._get_metadata(data_file, metadata)

        self.minio_obj.put_object(
            bucket_name=bucket,
            object_name=self._get_relative_path(prefix, filename),
            data=data_file,
            length=len(data),
            metadata=file_metadata
        )
        data_file.close()

    def put_file(self, path: str, file_path: str, metadata: Dict = None):
        """Put file inside a minio folder.

            If file_path will be a path to a dictionary, the name will be
            the copied file name. if it will be a path with a file name,
            the name of the file will be this file's name.

        Args:
            path: destination of the new file in minio.
            file_path: the path to the file.
            metadata: metadata dictionary to append the file.
        """
        bucket, prefix, filename = extract_match(path)

        filename = filename or basename(file_path)

        with open(file_path, 'rb') as file_pointer:
            file_metadata = self._get_metadata(file_pointer, metadata)

        relative_path = self._get_relative_path(prefix, filename)
        self.minio_obj.fput_object(bucket, relative_path,
                                   file_path, metadata=file_metadata)

    @classmethod
    def _get_metadata(cls, file_pointer: File, metadata: Dict):
        file_metadata = {}  # TODO: review this.

        if metadata is not None:
            file_metadata.update(metadata)

        return file_metadata

    @_validate_directory
    def get_last_object(self, path: str) -> File:
        """Return the last modified object.

        Args:
            path: path of a directory.
        """
        bucket, directory_path, _ = extract_match(path)
        objects_names_in_dir = self.listdir(path, only_files=True)
        if len(objects_names_in_dir) == 0:
            return None

        last_object_name = objects_names_in_dir[0]
        relative_path = self._get_relative_path(directory_path,
                                                last_object_name)
        new_path = join(ROOT, bucket, relative_path)

        return self.get(new_path)
