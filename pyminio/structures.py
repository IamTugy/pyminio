import re
from dataclasses import dataclass
from functools import cached_property
from os.path import join
from typing import Any, Dict

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


@dataclass
class PathMatch:
    bucket: str
    prefix: str
    filename: str


class Match:
    PATH_STRUCTURE = re.compile(
        r"/(?P<bucket>.*?)/+(?P<prefix>.*/+)?(?P<filename>.+[^/])?"
    )

    def __init__(self, path: str):
        self._path = path
        self._match = self._get_match()

    @cached_property
    def path(self):
        return re.sub(r"/+", r"/", self._path)

    @property
    def bucket(self):
        return self._match.bucket

    @property
    def prefix(self):
        return self._match.prefix

    @property
    def filename(self):
        return self._match.filename

    def _get_match(self) -> PathMatch:
        """Get the bucket name, path prefix and file's name from path."""
        if self.is_root():
            return PathMatch(bucket="", prefix="", filename="")

        match = self.PATH_STRUCTURE.match(self.path)

        if match is None:
            raise ValueError(f"{self.path} is not a valid path")

        return PathMatch(
            bucket=match.group("bucket"),
            prefix=match.group("prefix") or "",
            filename=match.group("filename") or "",
        )

    def is_root(self):
        return self.path == ROOT

    @property
    def relative_path(self):
        return join(self.prefix, self.filename)

    def is_bucket(self):
        return self.bucket != "" and self.relative_path == ""

    def is_dir(self):
        return self.filename == ""

    def is_file(self):
        return not self.is_dir()

    @classmethod
    def infer_operation_destination(cls, src: "Match", dst: "Match") -> "Match":
        """Return a match with the dst path and filename if exists.
        If not, return dst path with src filename.

        Examples:
            >>> src = Match('/foo/bar1/baz')
            >>> dst = Match('/foo/bar2/')
            >>> Match.infer_file_operation_destination(src, dst)
            Match('/foo/bar2/baz')

            >>> src = Match('/foo/bar1/baz')
            >>> dst = Match('/foo/bar2/baz2')
            >>> Match.infer_file_operation_destination(src, dst)
            Match('/foo/bar2/baz2')

        Raises:
            ValueError: If src was not a valid file match.
        """
        if not src.is_file():
            raise ValueError("Src must be a valid match to a file")

        if dst.is_file():
            return dst

        else:
            return Match(join(dst.path, src.filename))

    def __repr__(self):
        return f"Match('{self.path}')"
