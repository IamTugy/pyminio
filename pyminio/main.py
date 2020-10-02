import re

from os import environ
from io import BytesIO
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Union, Any
from os.path import join, basename, dirname, normpath

import pytz

from minio import Minio, definitions
from attrdict import AttrDict
from minio.error import NoSuchKey, BucketNotEmpty

ROOT = "/"


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


class Match:
    PATH_STRUCTURE = \
        re.compile(r"/(?P<bucket>.*?)/+(?P<prefix>.*/+)?(?P<filename>.+[^/])?")

    def extract_match(self) -> List[str]:
        """Get the bucket name, path prefix and file's name from path.

        Returns:
            Match. bucket name, path without filename and bucket name,
                file's name.
        """
        if self.path == ROOT:
            return ('', '', '')

        match = self.PATH_STRUCTURE.match(self.path)

        if match is None:
            raise ValueError(f'{self.path} is not a valid path')

        return (
            match.group("bucket"),
            match.group("prefix") or '',
            match.group("filename") or '',
        )

    def __init__(self, path: str):
        self.path = re.sub(r'/+', r'/', path)
        self.bucket, self.prefix, self.filename = self.extract_match()

    def is_root(self):
        return self.path == ROOT

    @property
    def relative_path(self):
        return join(self.prefix, self.filename)

    def is_bucket(self):
        return self.relative_path == ''

    def is_dir(self):
        return self.filename == ''

    def is_file(self):
        return not self.is_dir()

    def calculate_match(self, other):
        if self.is_file:
            return self
        else:
            return Match(join(self.path, other.filename))


def _validate_directory(func):
    """Check if directory path is valid. """
    def decorated_method(self, path: str, *args, **kwargs):
        match = Match(path)
        if match.is_file():
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
    """Pyminio is an os-like cover to minio."""
    def __init__(self, minio_obj: Minio):
        self.minio_obj = minio_obj

    @_validate_directory
    def mkdirs(self, path: str):
        """Create path of directories.

        Works like linux's: 'mkdir -p'.

        Args:
            path: The absolute path to create.
        """
        match = Match(path)

        if match.is_root():
            raise ValueError("cannot create / directory")

        #  make bucket
        if not self.minio_obj.bucket_exists(bucket_name=match.bucket):
            self.minio_obj.make_bucket(bucket_name=match.bucket)

        if match.is_bucket():
            return

        # TODO: check if all directories has metadata and stuff.
        #  make sub directories (minio is making all path)
        empty_file = BytesIO()
        self.minio_obj.put_object(bucket_name=match.bucket,
                                  object_name=match.prefix,
                                  data=empty_file, length=0)

    def _get_objects_at(self, match: Match) -> List[definitions.Object]:
        """Return all objects in the specified bucket and directory path.

        Args:
            bucket: The bucket desired in minio.
            directory_path: full directory path inside the bucket.
        """
        return sorted(self.minio_obj.list_objects(bucket_name=match.bucket,
                                                  prefix=match.prefix),
                      key=get_last_modified, reverse=True)

    def _get_buckets(self):
        """Return all existed buckets. """
        return sorted(self.minio_obj.list_buckets(),
                      key=get_creation_date, reverse=True)

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
        match = Match(path)

        if match.is_root():
            if only_files:
                return []

            return [f"{b.name}/" for b in self._get_buckets()]

        return [obj.object_name.replace(match.prefix, '')
                for obj in self._get_objects_at(match)
                if not only_files or not obj.is_dir]

    def exists(self, path: str) -> bool:
        """Check if the specified path exists.

        Works like os.path.exists.
        """
        try:
            match = Match(path)

        except ValueError:
            return False

        if match.is_root():
            return True

        bucket_exists = self.minio_obj.bucket_exists(match.bucket)
        if not bucket_exists:
            return False

        if match.is_bucket():
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
        match = Match(path)
        return self.exists(path) and match.is_dir()

    @_validate_directory
    def rmdir(self, path: str, recursive: bool = False):
        """Remove specified directory.

        If recursive flag is used, remove all content recursively.
        Works like linux's rmdir (-r).

        Args:
            path: path of a directory.
            recursive: remove content recursively.
        """
        match = Match(path)

        if match.is_root():
            for bucket in self.listdir(ROOT):
                self.rmdir(join(ROOT, bucket), recursive)
            return

        file_objects = self._get_objects_at(match)

        if len(file_objects) > 0:
            if not recursive:
                raise RuntimeError("Directory is not empty")

            files = [file_obj.object_name
                     for file_obj in file_objects if not file_obj.is_dir]

            # list activates remove
            list(self.minio_obj.remove_objects(match.bucket, files))

            dirs = [file_obj.object_name
                    for file_obj in file_objects if file_obj.is_dir]

            for directory in dirs:
                self.rmdir(join(ROOT, match.bucket, directory), recursive=True)

        if match.is_bucket():
            try:
                self.minio_obj.remove_bucket(match.bucket)

            except BucketNotEmpty:
                raise RuntimeError("Directory is not empty")

        else:
            self.minio_obj.remove_object(match.bucket, match.prefix)

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

        match = Match(path)
        self.minio_obj.remove_object(match.bucket, match.relative_path)

    # TODO: implement copy recursive (Task #12)
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
        from_match = Match(from_path)

        if from_match.is_dir() and not recursive:
            raise RuntimeError("cannot copy folder unrecursively")

        to_match = Match(to_path).calculate_match(from_match)

        self.minio_obj.copy_object(to_match.bucket, to_match.relative_path,
                                   join(from_match.bucket,
                                        from_match.relative_path))

    def mv(self, from_path: str, to_path: str):
        """Move files from one directory to another.

        Works like linux's mv.

        Args:
            from_path: source path.
            to_path: destination path.
        """
        from_match = Match(from_path)
        to_match = Match(to_path).calculate_match(from_match)

        try:
            self.cp(from_match.path, to_match.path)

        finally:
            if(self.exists(from_match.path) and self.exists(to_match.path)):
                self.rm(from_match.path)

    def get(self, path: str) -> ObjectData:
        """Get file or directory from minio.

        Args:
            path: path of a directory or a file.
        """
        match = Match(path)
        kwargs = AttrDict()

        if match.is_bucket():
            raise ValueError('Minio bucket has no representable object.')
        try:
            if match.is_file():
                kwargs.data = self.minio_obj.get_object(
                    match.bucket, match.relative_path).data

                details = self.minio_obj.stat_object(
                    match.bucket, match.relative_path)
                name = match.filename
                return_obj = File

            else:
                parent_directory = \
                    join(dirname(normpath(match.prefix)), '')
                objects = self.minio_obj.list_objects(
                    bucket_name=match.bucket,
                    prefix=parent_directory
                )

                details = next(filter(
                    lambda obj: obj.object_name == match.relative_path,
                    objects))
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
        match = Match(path)
        data_file = BytesIO(data)

        file_metadata = self._get_metadata(data_file, metadata)

        self.minio_obj.put_object(
            bucket_name=match.bucket,
            object_name=match.relative_path,
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
        match = Match(path)

        if match.is_dir():
            match = Match(join(path, basename(file_path)))

        with open(file_path, 'rb') as file_pointer:
            file_metadata = self._get_metadata(file_pointer, metadata)

        self.minio_obj.fput_object(match.bucket, match.relative_path,
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
        match = Match(path)
        objects_names_in_dir = self.listdir(path, only_files=True)
        if len(objects_names_in_dir) == 0:
            return None

        last_object_name = objects_names_in_dir[0]
        relative_path = join(match.prefix, last_object_name)
        new_path = join(ROOT, match.bucket, relative_path)

        return self.get(new_path)


if __name__ == "__main__":
    minio_obj = Minio(
        endpoint=environ.get('MINIO_CONNECTION'),
        access_key=environ.get('MINIO_ACCESS_KEY'),
        secret_key=environ.get('MINIO_SECRET_KEY'),
        secure=False
    )

    pyminio = Pyminio(minio_obj=minio_obj)

    import ipdb
    ipdb.set_trace()
