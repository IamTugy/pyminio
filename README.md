# Pyminio
Minio is a python client wrapped like os to control minio server.

[![PyPI](https://img.shields.io/pypi/v/pyminio?color=blue&label=pypi%20version)]()
[![PyPI](https://img.shields.io/pypi/pyversions/pyminio.svg?style=plastic)]()
[![Downloads](https://pepy.tech/badge/pyminio)](https://pepy.tech/project/pyminio)

## Content
1. [Installation](#Installation)
2. [Setting up Pyminio](#Setting-up-Pyminio)
3. [Usage](#Usage)
   - [mkdirs](#mkdirs)
   - [listdir](#listdir)
   - [exists](#exists)
   - [isdir](#isdir)
   - [truncate](#truncate)
   - [rmdir](#rmdir)
   - [rm](#rm)
   - [cp](#cp)
   - [mv](#mv)
   - [get](#get)
   - [get_last_object](#get_last_object)
   - [put_data](#put_data)
   - [put_file](#put_file)
4. [Contribute](#Contribute)

## Installation
Use the package manager [pip](https://pypi.org/project/pyminio/) to install pyminio.
```bash
pip install pyminio
```

## Setting up Pyminio:
In case you want to add your own minio object you can pass it in the constructor like so:

```python
from minio import Minio
from pyminio import Pyminio

ENDPOINT = environ.get('MINIO_CONNECTION')
ACCESS_KEY = environ.get('MINIO_ACCESS_KEY')
SECRET_KEY = environ.get('MINIO_SECRET_KEY')

minio_obj = Minio(
            endpoint=self.ENDPOINT,
            access_key=self.ACCESS_KEY,
            secret_key=self.SECRET_KEY
        )
pyminio_client = Pyminio(minio_obj=minio_obj)
```

if you dont want to handle with minio, you cand do this instead:

```python
from pyminio import Pyminio

ENDPOINT = environ.get('MINIO_CONNECTION')
ACCESS_KEY = environ.get('MINIO_ACCESS_KEY')
SECRET_KEY = environ.get('MINIO_SECRET_KEY')

pyminio_client = Pyminio.from_credentials(
    endpoint=self.ENDPOINT,
    access_key=self.ACCESS_KEY,
    secret_key=self.SECRET_KEY
)
```

You can storage your minio credentials in environment varibles like in these examples or pass them to Pyminio in any other way.

## Usage:

### mkdirs
Pyminio.mkdirs will create the given full path if not exists like linux's 'mkdir -p'.

This function must get a directory path or it will raise a ValueError.

```python
>>> pyminio_client.mkdirs('/foo/bar/baz/')
```

### listdir
Pyminio.listdir will return the directory's content as a list of directorys and files name. Works like os's 'listdir'.

This function must get a directory path or it will raise a ValueError.

```python
>>> pyminio_client.listdir('/foo/bar/baz/')
['file_name_1', 'file_name_2', 'directory_name/']
```

There is an option to use the only_files flag to get only files from listdir.

```python
>>> pyminio_client.listdir('/foo/bar/baz/', only_files=True)
['file_name_1', 'file_name_2']
```

### exists
Pyminio.exists will return a boolean that confirm reather this path exists or not in the server. Works like os.path.exists.

```python
>>> pyminio_client.exists('/foo/bar/baz/file_name_1')  # existed file
True
>>> pyminio_client.exists('/foo/bar/baz/file_name_3')  # not existed file
False
>>> pyminio_client.exists('/all/path/wrong/')  # not existed path
False
```

### isdir
Pyminio.isdir will return True only if the given path exists and is a directory. Works like os.path.isdir

```python
>>> pyminio_client.isdir('/foo/bar/baz/file_name_1')  # existed file
False
>>> pyminio_client.isdir('/foo/bar/baz/directory_name/')  # existed directory
True
>>> pyminio_client.isdir('/all/path/wrong/but/directory/')  # not existed directory
False
```

### truncate
Pyminio.truncate will delete all minio's content.

```python
>>> pyminio_client.truncate()
```

### rmdir
Pyminio.rmdir will delete the specified directory. Works like linux's 'rmdir (-r)'.

It will raise a DirectoryNotEmptyError if given directory is not empty, except if the recursive flag is on and then it will delete given directory's path recursively.

This function must get a directory path or it will raise a ValueError.

```python
>>> pyminio_client.rmdir('/foo/bar/baz/directory_name/')  # empty directory
>>> pyminio_client.rmdir('/foo/bar/')  # unempty directory
DirectoryNotEmptyError: can not recursively delete unempty directory
>>> pyminio_client.rmdir('/foo/bar/', recursive=True)
```

### rm
Pyminio.rm works like [rmdir](#rmdir) only that it can delete files too. Works like linux's rm (-r).

```python
>>> pyminio_client.rm('/foo/bar/baz/file_name')
```

### cp
Pyminio.cp will copy one file or directory to given destination. Works like linux's 'cp (-r)'.

This func can only copy recursively when the recursive flag is True. If not, it will raise a ValueError.

#### How will the copy accure? (all directories are copied recursively in this examples)
| src path   | dst path  | dst exists | new dst      | Explain                                             |
| ---------- | --------- | ---------- | ------------ | --------------------------------------------------- |
| /foo/bar   | /foo/baz  |    ---     | /foo/baz     | The file's name will be changed from bar to baz.                            |
| /foo1/bar  | /foo2/    |    True    | /foo/bar     | The file will be copied to '/foo2/'                                         |
| /foo/bar/  | /foo/     |    True    | /foo/        | The content of '/foo/bar/' will be copied to '/foo/'                     |
| /foo1/bar/ | /foo2/    |   False    | /foo2/bar/   | '/foo1/bar/' will be copied recursively to '/foo2/bar/'                  |
| /foo1/bar/ | /foo2/baz |    ---     |     ---      | ValueError will be raised in attempting to copy directory in to a file |

```python
>>> pyminio_client.cp('/foo/bar', '/foo/baz')
>>> pyminio_client.cp('/foo1/bar', '/foo2/')
>>> pyminio_client.cp('/foo/bar/', '/foo/', recursive=True)
>>> pyminio_client.cp('/foo1/bar/', '/foo2/', recursive=True)
>>> pyminio_client.cp('/foo1/bar/', '/foo2/baz', recursive=True)
ValueError: can not activate this method from directory to a file.
```

### mv
Pyminio.mv works like [cp](#cp) only that it remove the source after complete the transfare. Works like linux's 'mv (-r)'.

This func can only move recursively when the recursive flag is True. If not, it will raise a ValueError.

```python
>>> pyminio_client.mv('/foo/bar/', '/foo/baz/')
```

### get
Pyminio.get return an object from given path. This object will return as a pyminio.File object or an pyminio.Folder object, that both inherit from pyminio.ObjectData

This objects will contain metadata, their path and name.
If its a File object it will contains it's data.

```python
>>> pyminio_client.get('/foo/bar/baz')
File(name='baz', full_path='/foo/bar/baz', metadata=AttrDict({'is_dir': False, 'last_modified': time.struct_time(...), 'size': ..., 'content-type': ...}), data=...)
```

### get_last_object
Pyminio.get_last_object will return the last modified object inside a given directory.

This function must get a directory path or it will raise a ValueError.

```python
>>> pyminio_client.get_last_object('/foo/bar/')
File(name='baz', full_path='/foo/bar/baz', metadata=AttrDict({'is_dir': False, 'last_modified': time.struct_time(...), 'size': ..., 'content-type': ...}), data=...)
```

### put_data
Pyminio.put_data gets a path, data in bytes, and some metadata, and create an object inside the given path.

```python
>>> data = b'test'
>>> metadata = {'Pyminio-is': 'Awesome'}
>>> pyminio_client.put_data(path='/foo/bar/baz', data=data, metadata=metadata)
```

### put_file
Pyminio.put_file works like [put_data](#put_data) only that instead of data it gets a path to a file in you computer. Then it will copy this file to the given location.

```python
>>> metadata = {'Pyminio-is': 'Awesome'}
>>> pyminio_client.put_data(path='/foo/bar/baz', file_path='/mnt/some_file', metadata=metadata)
```

## Contribute

All contributions are welcome:

- Read the [issues](https://github.com/mmm1513/pyminio/issues), Fork the [project](https://github.com/mmm1513/pyminio) and do a Pull Request.
- Request a new topic creating a `New issue` with the `enhancement` tag.
- Find any kind of errors in the code and create a `New issue` with the details and the `bug enhancement` or fork the project and do a Pull Request.
- Suggest a better or more pythonic way for existing examples.

### Work environment:

After forking the project and installing the dependencies,
download the [minio docker](https://hub.docker.com/r/minio/minio/) and start an instance in your computer for development and testing.
#### Dont forget to write tests, and to run all the tests before making a pull request.