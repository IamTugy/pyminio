from collections import deque
from datetime import datetime
from functools import wraps
from io import BytesIO
from os.path import basename, join, normpath
from posixpath import dirname
from typing import Iterable

import pytz
from minio import Minio, datatypes
from minio.commonconfig import CopySource
from minio.error import S3Error

from .exceptions import DirectoryNotEmptyError
from .structures import ROOT, File, Folder, Match, ObjectData


def _validate_directory(func):
    """Check if directory path is valid."""

    @wraps(func)
    def decorated_method(self, path: str, *args, **kwargs):
        match = Match(path)
        if match.is_file():
            raise ValueError(
                f"{path!r} is not a valid directory path."
                " must be absolute and end with /"
            )

        return func(self, path, *args, **kwargs)

    return decorated_method


def get_last_modified(obj):
    """Return object's last modified time."""
    if obj.last_modified is None:
        return pytz.UTC.localize(datetime.fromtimestamp(0))

    return obj.last_modified


def get_creation_date(obj):
    """Return object's creation date."""
    if obj.creation_date is None:
        return pytz.UTC.localize(datetime.fromtimestamp(0))
    return obj.creation_date


class Pyminio:
    """Pyminio is an os-like cover to minio."""

    def __init__(self, minio_obj: Minio):
        self.minio_obj = minio_obj

    @classmethod
    def from_credentials(
        cls, endpoint: str, access_key: str, secret_key: str, **kwargs
    ) -> "Pyminio":
        return cls(
            minio_obj=Minio(
                endpoint=endpoint,
                access_key=access_key,
                secret_key=secret_key,
                **kwargs,
            )
        )

    @_validate_directory
    def mkdirs(self, path: str):
        """Create path of directories.

        Works like linux's: 'mkdir -p'.

        Args:
            path: The absolute path to create.
        """
        match = Match(path)

        if match.is_root():
            raise ValueError("cannot create '/' directory")

        #  make bucket
        if not self.minio_obj.bucket_exists(bucket_name=match.bucket):
            self.minio_obj.make_bucket(bucket_name=match.bucket)

        if match.is_bucket():
            return

        # TODO: check if all directories has metadata and stuff.
        #  make sub directories (minio is making all path)
        empty_file = BytesIO()
        self.minio_obj.put_object(
            bucket_name=match.bucket,
            object_name=match.prefix,
            data=empty_file,
            length=0,
        )

    @staticmethod
    def _remove_current_from_object_list(
        objects: Iterable[datatypes.Object], match: Match
    ) -> list[datatypes.Object]:
        """When finding results list_objects return the given path aswell."""
        return [
            obj
            for obj in objects
            if match.path != f"/{obj.bucket_name}/{obj.object_name}"
        ]

    def _get_objects_at(self, match: Match) -> list[datatypes.Object]:
        """Return all objects in the specified bucket and directory path.

        Args:
            bucket: The bucket desired in minio.
            directory_path: full directory path inside the bucket.
        """
        return sorted(
            self._remove_current_from_object_list(
                self.minio_obj.list_objects(
                    bucket_name=match.bucket, prefix=match.prefix
                ),
                match,
            ),
            key=get_last_modified,
            reverse=True,
        )

    def _get_buckets(self):
        """Return all existed buckets."""
        return sorted(
            self.minio_obj.list_buckets(), key=get_creation_date, reverse=True
        )

    @classmethod
    def _extract_metadata(cls, detailed_metadata: dict) -> dict[str, str]:
        """Remove 'X-Amz-Meta-' from all the keys, and lowercase them.
        When metadata is pushed in the minio, the minio is adding
        those details that screw us. this is an unscrewing function.
        """
        detailed_metadata = detailed_metadata or {}
        return {
            key.replace("X-Amz-Meta-", "").lower(): value
            for key, value in detailed_metadata.items()
        }

    @_validate_directory
    def listdir(
        self, path: str, files_only: bool = False, dirs_only: bool = False
    ) -> tuple[str]:
        """Return all files and directories within the directory path.

        Works like os.listdir.

        Args:
            path: path of a directory.
            files_only: return only files name and not directories.
            dirs_only: return only directories name and not files.

        Returns:
            files and directories in path.
        """
        match = Match(path)

        if match.is_root():
            if files_only:
                return tuple()

            return tuple(f"{b.name}/" for b in self._get_buckets())

        return tuple(
            obj.object_name.replace(match.prefix, "")
            for obj in self._get_objects_at(match)
            if not (files_only and obj.is_dir or dirs_only and not obj.is_dir)
        )

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
        except ValueError:
            return False

        return True

    def isdir(self, path: str):
        """Check if the specified path is a directory.

        Works like os.path.isdir
        """
        match = Match(path)
        return self.exists(path) and match.is_dir()

    def truncate(self) -> "Pyminio":
        for bucket in self.listdir(ROOT):
            self.rmdir(join(ROOT, bucket), recursive=True)
        return self

    def _remove_root(self, recursive: bool) -> None:
        if recursive:
            return self.truncate()
        raise DirectoryNotEmptyError(
            "can not recursively delete non-empty root directory, use the recursive flag."
        )

    def _remove_content(self, objects: list[datatypes.Object]) -> None:
        for obj in objects:
            self.minio_obj.remove_object(obj.bucket_name, obj.object_name)

    def _remove_bucket(self, bucket: str) -> None:
        try:
            self.minio_obj.remove_bucket(bucket)

        except S3Error as e:
            if e.code in ["BucketNotEmpty"]:
                raise DirectoryNotEmptyError(
                    "can not recursively delete non-empty directory"
                )
            raise

    @_validate_directory
    def rmdir(self, path: str, recursive: bool = False) -> "Pyminio":
        """Remove specified directory.

        If recursive flag is used, remove all content recursively
        like linux's rm -r.

        Args:
            path: path of a directory.
            recursive: remove content recursively.
        """
        match = Match(path)

        if match.is_root():
            self._remove_root(recursive)
            return self

        if match.is_dir():
            content_objects = self._remove_current_from_object_list(
                self.minio_obj.list_objects(
                    bucket_name=match.bucket, prefix=match.prefix, recursive=True
                ),
                match,
            )
            if content_objects and not recursive:
                raise DirectoryNotEmptyError(
                    "can not recursively delete non-empty directory, use the recursive flag."
                )
            self._remove_content(content_objects)

        if match.is_bucket():
            self._remove_bucket(match.bucket)
        else:
            self.minio_obj.remove_object(match.bucket, match.prefix)

        return self

    def rm(self, path: str, recursive: bool = False) -> "Pyminio":
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
        return self

    def _get_destination(self, from_path: str, to_path: str):
        from_match = Match(from_path)
        to_match = Match(to_path)

        if from_match.is_file():
            return Match.infer_operation_destination(from_match, to_match)

        if to_match.is_dir():
            if self.exists(to_match.path):
                return Match(
                    join(
                        to_match.path,
                        basename(join(from_match.bucket, from_match.prefix)[:-1]),
                        "",
                    )
                )

        else:
            raise ValueError("can not activate this method from directory to a file.")

        return to_match

    @_validate_directory
    def copy_recursively(self, from_path: str, to_path: str) -> "Pyminio":
        """Copy recursively the content of from_path, to to_path.

        If you acctually wanted to copy from_path as a folder,
        add that folder's name to to_path and that new path
        will be created for you.

        Args:
            from_path: source path to a file.
            to_path: destination path.
            recursive: copy content recursively.
        """
        files_to_copy = []
        dirs_to_copy = deque([from_path])

        while len(dirs_to_copy) > 0:
            current_dir_match = Match(dirs_to_copy.popleft())
            objects_in_directory = self._get_objects_at(current_dir_match)
            dirs = []

            for obj in objects_in_directory:
                obj_path = f"/{obj.bucket_name}/{obj.object_name}"
                if obj.is_dir:
                    dirs.append(obj_path)
                else:
                    files_to_copy.append(
                        dict(
                            from_path=obj_path,
                            to_path=join(to_path, obj_path.replace(from_path, "")),
                        )
                    )

            if len(dirs) == 0:
                self.mkdirs(
                    join(to_path, current_dir_match.path.replace(from_path, ""))
                )

            dirs_to_copy.extend(dirs)

        for obj_to_copy in files_to_copy:
            self.cp(**obj_to_copy)

        return self

    def cp(self, from_path: str, to_path: str, recursive: bool = False) -> "Pyminio":
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
        to_match = self._get_destination(from_path, to_path)

        if from_match.is_dir():
            if recursive:
                return self.copy_recursively(from_match.path, to_match.path)

            else:
                raise ValueError("copying a directory must be done recursively")

        self.minio_obj.copy_object(
            bucket_name=to_match.bucket,
            object_name=to_match.relative_path,
            source=CopySource(
                bucket_name=from_match.bucket,
                object_name=from_match.relative_path,
            ),
        )
        return self

    def mv(self, from_path: str, to_path: str, recursive: bool = False) -> "Pyminio":
        """Move files from one directory to another.

        Works like linux's mv.

        Args:
            from_path: source path.
            to_path: destination path.
        """
        # must be before cp because of the destination picking logic:
        to_match = self._get_destination(from_path, to_path)
        try:
            self.cp(from_path, to_path, recursive)

        finally:
            if self.exists(from_path) and self.exists(to_match.path):
                self.rm(from_path, recursive)
        return self

    def get(self, path: str) -> ObjectData:
        """Get file or directory from minio.

        Args:
            path: path of a directory or a file.
        """
        match = Match(path)
        kwargs = {}

        if match.is_bucket():
            raise ValueError("Minio bucket has no representable object.")
        try:
            if match.is_file():
                kwargs["data"] = self.minio_obj.get_object(
                    match.bucket, match.relative_path
                ).data

                details = self.minio_obj.stat_object(match.bucket, match.relative_path)
                name = match.filename
                return_obj = File

            else:
                parent_directory = join(dirname(normpath(match.prefix)), "")
                objects = self.minio_obj.list_objects(
                    bucket_name=match.bucket, prefix=parent_directory
                )

                details = next(
                    obj for obj in objects if obj.object_name == match.relative_path
                )
                name = join(basename(normpath(details.object_name)), "")
                return_obj = Folder

        except StopIteration:
            raise ValueError(f"cannot access {path!r}: " "No such file or directory")
        except S3Error as e:
            if e.code in ["NoSuchKey"]:
                raise ValueError(
                    f"cannot access {path!r}: " "No such file or directory"
                )

        details_metadata = self._extract_metadata(details.metadata)

        metadata = {
            "is_dir": details.is_dir,
            "last_modified": details.last_modified,
            "size": details.size,
        }
        metadata.update(details_metadata)

        return return_obj(name=name, full_path=path, metadata=metadata, **kwargs)

    def put_data(self, path: str, data: bytes, metadata: dict = None):
        """Put data in file inside a minio folder.

        Args:
            path: destination of the new file with its name in minio.
            data: the data that the file will contain in bytes.
            metadata: metadata dictionary to append the file.
        """
        match = Match(path)
        data_file = BytesIO(data)

        self.minio_obj.put_object(
            bucket_name=match.bucket,
            object_name=match.relative_path,
            data=data_file,
            length=len(data),
            metadata=metadata,
        )
        data_file.close()

    def put_file(self, file_path: str, to_path: str, metadata: dict = None):
        """Put file inside a minio folder.

        If file_path will be a path to a file, the name will be
        the to_path if it will be a path with a file name,
        if not, the name of the file will be this file's name.

        Args:
            file_path: the path to the file in local disk.
            to_path: destination of the new file in minio.
            metadata: metadata dictionary to append the file.
        """
        match = Match(to_path)

        if match.is_dir():
            match = Match(join(to_path, basename(file_path)))

        self.minio_obj.fput_object(
            match.bucket, match.relative_path, file_path, metadata=metadata
        )

    @_validate_directory
    def get_last_object(self, path: str) -> File:
        """Return the last modified object.

        Args:
            path: path of a directory.
        """
        match = Match(path)
        objects_names_in_dir = self.listdir(path, files_only=True)
        if len(objects_names_in_dir) == 0:
            return None

        last_object_name = objects_names_in_dir[0]
        relative_path = join(match.prefix, last_object_name)
        new_path = join(ROOT, match.bucket, relative_path)

        return self.get(new_path)
