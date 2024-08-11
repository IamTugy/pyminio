from functools import wraps
from posixpath import join
from typing import Callable, Dict, Generator, List, Union

import pytest

from pyminio.exceptions import DirectoryNotEmptyError
from pyminio.structures import File

from .utils import PyminioTest

ROOT = "/"
FILE_CONTENT = b"test"

FS_TYPE = Dict[str, Union[List["FS_TYPE"], "FS_TYPE", None]]


def _mkdirs_recursively(client: PyminioTest, fs: FS_TYPE, relative_path: str) -> None:
    for key, value in fs.items():
        abspath = join(relative_path, key)
        if value is None:
            client.mkdirs(join(relative_path, ""))
            client.put_data(abspath, FILE_CONTENT)

        elif value == []:
            client.mkdirs(join(abspath, ""))

        elif isinstance(value, dict):
            _mkdirs_recursively(client=client, fs=value, relative_path=abspath)

        else:
            raise ValueError("Invalid FS structure")


def mock_fs(
    fs: FS_TYPE, relative_path: str = ROOT
) -> Callable[[Callable[[PyminioTest], None]], Callable[[PyminioTest], None]]:
    def decorator(func: Callable[[PyminioTest], None]) -> Callable[[PyminioTest], None]:
        @wraps(func)
        def wrapper(client: PyminioTest) -> None:
            _mkdirs_recursively(client=client, fs=fs, relative_path=relative_path)
            return func(client)

        return wrapper

    return decorator


@pytest.fixture  # type: ignore [misc]
def client() -> Generator[PyminioTest, None, None]:
    client = PyminioTest()
    yield client
    client.rm("/", recursive=True)


def test_basic_fixtures() -> None:
    """Test the basic fixture that is used in all tests."""
    _client = PyminioTest()
    file_system: FS_TYPE = {
        "foo1": {
            "bar": [],
        },
        "foo2": {
            "bar": {
                "baz1": [],
                "baz2": None,
            }
        },
        "foo3": {
            "bar": {
                "baz1": [],
                "baz2": [],
            }
        },
    }
    mock_fs(file_system)(lambda _: None)(_client)
    _client.rm("/", recursive=True)
    assert _client.listdir("/") == tuple()


@mock_fs({"foo": {"bar1": {"baz": None}, "bar2": []}})
def test_exists(client: PyminioTest) -> None:
    assert client.exists("/foo/")
    assert not client.exists("/foo")
    assert client.exists("/foo//")
    assert client.exists("/foo/bar1/")
    assert client.exists("/foo/bar2/")
    assert not client.exists("/foo/bar2/baz")
    assert not client.exists("/foo/bar2")
    assert client.exists("/foo/bar1/baz")
    assert not client.exists("/foo/bar1/baz/")


@mock_fs(
    {
        "foo1": {
            "bar": [],
        },
        "foo2": {
            "bar": {
                "baz": None,
            }
        },
        "foo3": {
            "bar": {
                "baz1": [],
                "baz2": [],
            }
        },
    }
)
def test_listdir(client: PyminioTest) -> None:
    assert set(client.listdir("/")) == {"foo1/", "foo2/", "foo3/"}
    assert set(client.listdir("/foo1/")) == {"bar/"}
    assert set(client.listdir("/foo2/")) == {"bar/"}
    # TODO: fix - client.listdir("/foo2/bar/") -> ('baz', '')
    assert set(client.listdir("/foo2/bar/")) == {"baz"}
    assert set(client.listdir("/foo3/bar/")) == {"baz1/", "baz2/"}


@mock_fs(
    {
        "foo1": {
            "bar": [],
        },
        "foo2": {
            "bar": {
                "baz1": [],
                "baz2": None,
            }
        },
        "foo3": {
            "bar": {
                "baz1": [],
                "baz2": [],
            }
        },
    }
)
def test_listdir_files_only(client: PyminioTest) -> None:
    assert set(client.listdir("/", files_only=True)) == set()
    assert set(client.listdir("/foo1/", files_only=True)) == set()
    assert set(client.listdir("/foo2/", files_only=True)) == set()
    assert set(client.listdir("/foo2/bar/", files_only=True)) == {"baz2"}
    assert set(client.listdir("/foo3/bar/", files_only=True)) == set()


@mock_fs(
    {
        "foo1": {
            "bar": [],
        },
        "foo2": {
            "bar": {
                "baz1": [],
                "baz2": None,
            }
        },
        "foo3": {
            "bar": {
                "baz1": [],
                "baz2": [],
            }
        },
    }
)
def test_listdir_dirs_only(client: PyminioTest) -> None:
    assert set(client.listdir("/")) == {"foo1/", "foo2/", "foo3/"}
    assert set(client.listdir("/foo1/", dirs_only=True)) == {"bar/"}
    assert set(client.listdir("/foo2/", dirs_only=True)) == {"bar/"}
    assert set(client.listdir("/foo2/bar/", dirs_only=True)) == {"baz1/"}
    assert set(client.listdir("/foo3/bar/", dirs_only=True)) == {"baz1/", "baz2/"}


@mock_fs({"foo": {"bar": {"baz": None}}})
def test_isdir(client: PyminioTest) -> None:
    assert client.isdir("/foo/")
    assert client.isdir("/foo/bar/")
    assert not client.isdir("/foo/bar/baz")


@mock_fs(
    {
        "foo1": {
            "bar": [],
        },
        "foo2": {
            "bar": {
                "baz1": [],
                "baz2": None,
            }
        },
        "foo3": {
            "bar": {
                "baz1": [],
                "baz2": [],
            }
        },
    }
)
def test_rmdir(client: PyminioTest) -> None:
    client.rmdir("/foo1/bar/")
    assert not client.exists("/foo1/bar/")

    with pytest.raises(ValueError):
        client.rmdir("/foo2/bar/baz2")

    with pytest.raises(DirectoryNotEmptyError):
        client.rmdir("/foo2/bar/")
    client.rmdir("/foo2/bar/", recursive=True)
    assert not client.exists("/foo2/bar/")

    with pytest.raises(DirectoryNotEmptyError):
        client.rmdir("/foo3/")
    client.rmdir("/foo3/", recursive=True)
    assert not client.exists("/foo3/")

    client.rmdir("/", recursive=True)
    assert not client.exists("/foo1/")


@mock_fs(
    {
        "foo1": {
            "bar": [],
        },
        "foo2": {
            "bar": {
                "baz1": [],
                "baz2": None,
            }
        },
        "foo3": {
            "bar": {
                "baz1": [],
                "baz2": [],
            }
        },
    }
)
def test_rm(client: PyminioTest) -> None:
    client.rm("/foo1/bar/")
    assert not client.exists("/foo1/bar/")

    client.rm("/foo2/bar/baz2")
    assert not client.exists("/foo1/bar/baz2")

    with pytest.raises(DirectoryNotEmptyError):
        client.rm("/foo2/bar/")
    client.rm("/foo2/bar/", recursive=True)
    assert not client.exists("/foo2/bar/")

    with pytest.raises(DirectoryNotEmptyError):
        client.rm("/foo3/")
    client.rm("/foo3/", recursive=True)
    assert not client.exists("/foo3/")

    client.rm("/", recursive=True)
    assert not client.exists("/foo1/")


@mock_fs({"foo": {"bar": None}})
def test_get_data(client: PyminioTest) -> None:
    file_obj = client.get("/foo/bar")
    assert isinstance(file_obj, File)
    assert file_obj.name == "bar"
    assert file_obj.full_path == "/foo/bar"
    assert file_obj.data == FILE_CONTENT


@mock_fs(
    {
        "foo": {
            "bar1": [],
            "bar2": {
                "baz": [],
            },
        }
    }
)
def test_get_folder(client: PyminioTest) -> None:
    folder_obj1 = client.get("/foo/bar1/")
    assert folder_obj1.name == "bar1/"
    assert folder_obj1.metadata["is_dir"]

    folder_obj2 = client.get("/foo/bar2/baz/")
    assert folder_obj2.name == "baz/"
    assert folder_obj2.metadata["is_dir"]


@mock_fs(
    {
        "foo": {
            "bar1": [],
            "baz": None,
            "bar2": [],
        }
    }
)
def test_cp(client: PyminioTest) -> None:
    client.cp("/foo/baz", "/foo/bar2/")
    assert client.exists("/foo/bar2/baz")

    client.cp("/foo/bar2/", "/foo/bar1/", recursive=True)
    assert client.exists("/foo/bar1/bar2/baz")


@mock_fs(
    {
        "foo": {
            "bar1": [],
            "baz": None,
            "bar2": [],
        }
    }
)
def test_mv(client: PyminioTest) -> None:
    client.mv("/foo/baz", "/foo/bar2/")
    assert client.exists("/foo/bar2/baz")
    assert not client.exists("/foo/baz")

    client.mv("/foo/bar2/", "/foo/bar1/", recursive=True)
    assert client.exists("/foo/bar1/bar2/baz")
    assert not client.exists("/foo/bar2/")

    client.mv("/foo/", "/foo1/", recursive=True)
    assert client.exists("/foo1/bar1/bar2/baz")
    assert not client.exists("/foo/")


@mock_fs(
    {
        "foo1": {
            "bar": {
                "baz1": [],
                "baz2": None,
            }
        },
        "foo3": [],
    }
)
def test_recursive_mv_buckets(client: PyminioTest) -> None:
    client.mv("/foo1/", "/foo2/", recursive=True)
    assert not client.exists("/foo1/")
    assert client.exists("/foo2/bar/baz1/")
    assert client.exists("/foo2/bar/baz2")

    client.mv("/foo2/", "/foo3/", recursive=True)
    assert not client.exists("/foo2/")
    assert client.exists("/foo3/foo2/bar/")
    assert client.exists("/foo3/foo2/bar/baz1/")
    assert client.exists("/foo3/foo2/bar/baz2")


@mock_fs(
    {
        "foo": {
            "bar1": [],
            "baz": None,
            "bar2": [],
        },
        "foo1": [],
    }
)
def test_mv_to_exists_bucket(client: PyminioTest) -> None:
    client.mv("/foo/", "/foo1/", recursive=True)
    assert client.exists("/foo1/foo/bar1/")
    assert client.exists("/foo1/foo/bar2/")
    assert client.exists("/foo1/foo/baz")
    assert not client.exists("/foo/")


@mock_fs(
    {
        "foo": {
            "bar1": None,
            "bar2": None,
            "bar3": None,
        },
        "baz": [],
    }
)
def test_get_last_object(client: PyminioTest) -> None:
    client.put_data("/foo/bar4", FILE_CONTENT)
    obj = client.get_last_object("/foo/")
    assert obj is not None and obj.name == "bar4"
    assert client.get_last_object("/baz/") is None
