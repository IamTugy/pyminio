from os.path import join

import pytest

from .utils import PyminioTest

ROOT = '/'
FILE_CONTENT = bytes('test', 'UTF-8')


def _mkdirs_recursively(client, tree, relative_path):
    for key, value in tree.items():
        abspath = join(relative_path, key)
        if value is None:
            client.mkdirs(join(relative_path, ''))
            client.put_data(abspath, FILE_CONTENT)

        elif value == []:
            client.mkdirs(join(abspath, ''))

        else:
            _mkdirs_recursively(
                client=client,
                tree=tree[key],
                relative_path=abspath)


def mkdirs_recursively(tree, relative_path=ROOT):
    def decorator(func):
        def wrapper(client):
            _mkdirs_recursively(
                client=client,
                tree=tree,
                relative_path=relative_path)
            return func(client)
        return wrapper
    return decorator


@pytest.fixture
def client():
    client = PyminioTest()
    yield client
    client.rm('/', recursive=True)


@mkdirs_recursively(
    tree={
        'foo': {
            'bar': {
                'baz': None
            }
        }
    }
)
def test_exists(client):
    assert client.exists('/foo/')
    assert not client.exists('/foo')
    assert client.exists('/foo//')
    assert client.exists('/foo/bar/')
    assert client.exists('/foo/bar/baz')
    assert not client.exists('/foo/bar/baz/')


@pytest.mark.skip()
@mkdirs_recursively(
    tree={
        'foo': {
            'bar': None
        }
    }
)
def test_put_data(client):
    assert client.exists('/foo/bar')


@pytest.mark.skip()
@mkdirs_recursively(
    tree={
        'foo': {
            'bar': None
        }
    }
)
def test_put_file(client):
    assert client.exists('/foo/bar')


@mkdirs_recursively(
    tree={
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
        },
    }
)
def test_listdir(client):
    assert client.listdir('/').sort() == ['foo1/', 'foo2/', 'foo3/'].sort()
    assert client.listdir('/foo1/') == ['bar/']
    assert client.listdir('/foo2/') == ['bar/']
    assert client.listdir('/foo2/bar/') == ['baz']
    assert client.listdir('/foo3/bar/').sort() == ['baz1/', 'baz2/'].sort()


@mkdirs_recursively(
    tree={
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
    }
)
def test_listdir_only_files(client):
    assert client.listdir('/', only_files=True) == []
    assert client.listdir('/foo1/', only_files=True) == []
    assert client.listdir('/foo2/', only_files=True) == []
    assert client.listdir('/foo2/bar/', only_files=True) == ['baz2']
    assert client.listdir('/foo3/bar/', only_files=True) == []


@mkdirs_recursively(
    tree={
        'foo': {
            'bar': {
                'baz': None
            }
        }
    }
)
def test_isdir(client):
    assert client.isdir('/foo/')
    assert client.isdir('/foo/bar/')
    assert not client.isdir('/foo/bar/baz')


@mkdirs_recursively(
    tree={
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
    }
)
def test_rmdir(client):
    client.rmdir('/foo1/bar/')
    assert not client.exists('/foo1/bar/')

    pytest.raises(ValueError, client.rmdir, '/foo2/bar/baz2')

    pytest.raises(RuntimeError, client.rmdir, '/foo2/bar/')
    client.rmdir('/foo2/bar/', recursive=True)
    assert not client.exists('/foo2/bar/')

    pytest.raises(RuntimeError, client.rmdir, '/foo3/')
    client.rmdir('/foo3/', recursive=True)
    assert not client.exists('/foo3/')

    client.rmdir('/', recursive=True)
    assert not client.exists('/foo1/')


@mkdirs_recursively(
    tree={
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
    }
)
def test_rm(client):
    client.rm('/foo1/bar/')
    assert not client.exists('/foo1/bar/')

    client.rm('/foo2/bar/baz2')
    assert not client.exists('/foo1/bar/baz2')

    pytest.raises(RuntimeError, client.rm, '/foo2/bar/')
    client.rm('/foo2/bar/', recursive=True)
    assert not client.exists('/foo2/bar/')

    pytest.raises(RuntimeError, client.rm, '/foo3/')
    client.rm('/foo3/', recursive=True)
    assert not client.exists('/foo3/')

    client.rm('/', recursive=True)
    assert not client.exists('/foo1/')


@mkdirs_recursively(
    tree={
        'foo': {
            'bar': None
        }
    }
)
def test_get_data(client):
    file_obj = client.get('/foo/bar')
    assert file_obj.name == 'bar'
    assert file_obj.full_path == '/foo/bar'
    assert file_obj.data == FILE_CONTENT


@mkdirs_recursively(
    tree={
        'foo': {
            'bar1': [],
            'bar2': {
                'baz': [],
            },
        }
    }
)
def test_get_folder(client):
    folder_obj1 = client.get('/foo/bar1/')
    assert folder_obj1.name == 'bar1/'
    assert folder_obj1.metadata.is_dir

    folder_obj2 = client.get('/foo/bar2/baz/')
    assert folder_obj2.name == 'baz/'
    assert folder_obj2.metadata.is_dir


@pytest.mark.skip()
@mkdirs_recursively(
    tree={
        'foo': {
            'bar1': [],
            'baz': None,
            'bar2': [],
        }
    }
)
def test_cp(client):
    client.cp('/foo/baz', '/foo/bar2/')
    assert client.exists('/foo/bar2/baz')

    client.cp('/foo/bar2/', '/foo/bar1/')
    assert client.exists('/foo/bar1/bar2/baz')


@pytest.mark.skip()
@mkdirs_recursively(
    tree={
        'foo': {
            'bar1': [],
            'baz': None,
            'bar2': [],
        }
    }
)
def test_mv(client):
    pass  # like cp but check if not exists afterwards


@mkdirs_recursively(
    tree={
        'foo': {
            'bar1': None,
            'bar2': None,
            'bar3': None,
        },
        'baz': [],
    }
)
def test_get_last_object(client):
    client.put_data('/foo/bar4', FILE_CONTENT)
    assert client.get_last_object('/foo/').name == 'bar4'

    assert client.get_last_object('/baz/') is None
