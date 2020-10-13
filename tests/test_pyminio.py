from os.path import join

import pytest

from .utils import PyminioTest
from pyminio.exceptions import DirectoryNotEmptyError

ROOT = '/'
FILE_CONTENT = b'test'


def _mkdirs_recursively(client, fs, relative_path):
    for key, value in fs.items():
        abspath = join(relative_path, key)
        if value is None:
            client.mkdirs(join(relative_path, ''))
            client.put_data(abspath, FILE_CONTENT)

        elif value == []:
            client.mkdirs(join(abspath, ''))

        else:
            _mkdirs_recursively(
                client=client,
                fs=fs[key],
                relative_path=abspath)


def mock_fs(fs, relative_path=ROOT):
    def decorator(func):
        def wrapper(client):
            _mkdirs_recursively(
                client=client,
                fs=fs,
                relative_path=relative_path)
            return func(client)
        return wrapper
    return decorator


@pytest.fixture
def client():
    client = PyminioTest()
    yield client
    client.rm('/', recursive=True)


@mock_fs({
    'foo': {
        'bar': {
            'baz': None
        }
    }
})
def test_exists(client):
    assert client.exists('/foo/')
    assert not client.exists('/foo')
    assert client.exists('/foo//')
    assert client.exists('/foo/bar/')
    assert client.exists('/foo/bar/baz')
    assert not client.exists('/foo/bar/baz/')


@mock_fs({
    'foo1': {
        'bar': [],
    },
    'foo2': {
        'bar': {
            'baz': None,
        }
    },
    'foo3': {
        'bar': {
            'baz1': [],
            'baz2': [],
        }
    }
})
def test_listdir(client):
    assert set(client.listdir('/')) == {'foo1/', 'foo2/', 'foo3/'}
    assert set(client.listdir('/foo1/')) == {'bar/'}
    assert set(client.listdir('/foo2/')) == {'bar/'}
    assert set(client.listdir('/foo2/bar/')) == {'baz'}
    assert set(client.listdir('/foo3/bar/')) == {'baz1/', 'baz2/'}


@mock_fs({
    'foo1': {
        'bar': [],
    },
    'foo2': {
        'bar': {
            'baz1': [],
            'baz2': None,
        }
    },
    'foo3': {
        'bar': {
            'baz1': [],
            'baz2': [],
        }
    }
})
def test_listdir_files_only(client):
    assert set(client.listdir('/', files_only=True)) == set()
    assert set(client.listdir('/foo1/', files_only=True)) == set()
    assert set(client.listdir('/foo2/', files_only=True)) == set()
    assert set(client.listdir('/foo2/bar/', files_only=True)) == {'baz2'}
    assert set(client.listdir('/foo3/bar/', files_only=True)) == set()


@mock_fs({
    'foo1': {
        'bar': [],
    },
    'foo2': {
        'bar': {
            'baz1': [],
            'baz2': None,
        }
    },
    'foo3': {
        'bar': {
            'baz1': [],
            'baz2': [],
        }
    }
})
def test_listdir_dirs_only(client):
    assert set(client.listdir('/')) == {'foo1/', 'foo2/', 'foo3/'}
    assert set(client.listdir('/foo1/', dirs_only=True)) == {'bar/'}
    assert set(client.listdir('/foo2/', dirs_only=True)) == {'bar/'}
    assert set(client.listdir('/foo2/bar/', dirs_only=True)) == {'baz1/'}
    assert set(client.listdir('/foo3/bar/', dirs_only=True)) == {'baz1/',
                                                                 'baz2/'}


@mock_fs({
    'foo': {
        'bar': {
            'baz': None
        }
    }
})
def test_isdir(client):
    assert client.isdir('/foo/')
    assert client.isdir('/foo/bar/')
    assert not client.isdir('/foo/bar/baz')


@mock_fs({
    'foo1': {
        'bar': [],
    },
    'foo2': {
        'bar': {
            'baz1': [],
            'baz2': None,
        }
    },
    'foo3': {
        'bar': {
            'baz1': [],
            'baz2': [],
        }
    }
})
def test_rmdir(client):
    client.rmdir('/foo1/bar/')
    assert not client.exists('/foo1/bar/')

    with pytest.raises(ValueError):
        client.rmdir('/foo2/bar/baz2')

    with pytest.raises(DirectoryNotEmptyError):
        client.rmdir('/foo2/bar/')
    client.rmdir('/foo2/bar/', recursive=True)
    assert not client.exists('/foo2/bar/')

    with pytest.raises(DirectoryNotEmptyError):
        client.rmdir('/foo3/')
    client.rmdir('/foo3/', recursive=True)
    assert not client.exists('/foo3/')

    client.rmdir('/', recursive=True)
    assert not client.exists('/foo1/')


@mock_fs({
    'foo1': {
        'bar': [],
    },
    'foo2': {
        'bar': {
            'baz1': [],
            'baz2': None,
        }
    },
    'foo3': {
        'bar': {
            'baz1': [],
            'baz2': [],
        }
    }
})
def test_rm(client):
    client.rm('/foo1/bar/')
    assert not client.exists('/foo1/bar/')

    client.rm('/foo2/bar/baz2')
    assert not client.exists('/foo1/bar/baz2')

    with pytest.raises(DirectoryNotEmptyError):
        client.rm('/foo2/bar/')
    client.rm('/foo2/bar/', recursive=True)
    assert not client.exists('/foo2/bar/')

    with pytest.raises(DirectoryNotEmptyError):
        client.rm('/foo3/')
    client.rm('/foo3/', recursive=True)
    assert not client.exists('/foo3/')

    client.rm('/', recursive=True)
    assert not client.exists('/foo1/')


@mock_fs({
    'foo': {
        'bar': None
    }
})
def test_get_data(client):
    file_obj = client.get('/foo/bar')
    assert file_obj.name == 'bar'
    assert file_obj.full_path == '/foo/bar'
    assert file_obj.data == FILE_CONTENT


@mock_fs({
    'foo': {
        'bar1': [],
        'bar2': {
            'baz': [],
        },
    }
})
def test_get_folder(client):
    folder_obj1 = client.get('/foo/bar1/')
    assert folder_obj1.name == 'bar1/'
    assert folder_obj1.metadata.is_dir

    folder_obj2 = client.get('/foo/bar2/baz/')
    assert folder_obj2.name == 'baz/'
    assert folder_obj2.metadata.is_dir


@mock_fs({
    'foo': {
        'bar1': [],
        'baz': None,
        'bar2': [],
    }
})
def test_cp(client):
    client.cp('/foo/baz', '/foo/bar2/')
    assert client.exists('/foo/bar2/baz')

    client.cp('/foo/bar2/', '/foo/bar1/', recursive=True)
    assert client.exists('/foo/bar1/bar2/baz')


@mock_fs({
    'foo': {
        'bar1': [],
        'baz': None,
        'bar2': [],
    }
})
def test_mv(client):
    client.mv('/foo/baz', '/foo/bar2/')
    assert client.exists('/foo/bar2/baz')
    assert not client.exists('/foo/baz')

    client.mv('/foo/bar2/', '/foo/bar1/', recursive=True)
    assert client.exists('/foo/bar1/bar2/baz')
    assert not client.exists('/foo/bar2/')

    client.mv('/foo/', '/foo1/', recursive=True)
    assert client.exists('/foo1/bar1/bar2/baz')
    assert not client.exists('/foo/')


@mock_fs({
    'foo1': {
        'bar': {
            'baz1': [],
            'baz2': None,
        }
    },
    'foo3': []
})
def test_recursive_mv_buckets(client):
    client.mv('/foo1/', '/foo2/', recursive=True)
    assert not client.exists('/foo1/')
    assert client.exists('/foo2/bar/baz1/')
    assert client.exists('/foo2/bar/baz2')

    client.mv('/foo2/', '/foo3/', recursive=True)
    assert not client.exists('/foo2/')
    assert client.exists('/foo3/foo2/bar/')
    assert client.exists('/foo3/foo2/bar/baz1/')
    assert client.exists('/foo3/foo2/bar/baz2')


@mock_fs({
    'foo': {
        'bar1': [],
        'baz': None,
        'bar2': [],
    },
    'foo1': []
})
def test_mv_to_exists_bucket(client):
    client.mv('/foo/', '/foo1/', recursive=True)
    assert client.exists('/foo1/foo/bar1/')
    assert client.exists('/foo1/foo/bar2/')
    assert client.exists('/foo1/foo/baz')
    assert not client.exists('/foo/')


@mock_fs({
    'foo': {
        'bar1': None,
        'bar2': None,
        'bar3': None,
    },
    'baz': [],
})
def test_get_last_object(client):
    client.put_data('/foo/bar4', FILE_CONTENT)
    assert client.get_last_object('/foo/').name == 'bar4'

    assert client.get_last_object('/baz/') is None
