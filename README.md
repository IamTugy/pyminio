# Pyminio
[![PyPI](https://img.shields.io/pypi/v/pyminio?color=blue&label=pypi%20version)]()
[![PyPI](https://img.shields.io/pypi/pyversions/pyminio.svg?style=plastic)]()
[![Downloads](https://pepy.tech/badge/pyminio)](https://pepy.tech/project/pyminio)
#### Pyminio is a python client wrapped like the `os` module to control minio server.

I have developed pyminio while trying to work with the minio's original python client with a lot of struggles. I had to read and understand minio's implementations to preform the most simple tasks.

Pyminio is a wrapper to minio, that is more indecative for the user.
It works like `os` module, so you don't need to understand minio's concepts, and just using regular paths.

## Content
1. [Installation](#installation)
2. [Setting up Pyminio](#setting-up-pyminio)
3. [Usage](#usage)
4. [Contribute](#contribute)

## Installation
Use the package manager [pip](https://pypi.org/project/pyminio/) to install pyminio.

```bash
pip install pyminio
```

## Setting up Pyminio

Firstly you need to set up your  [Minio Docker](https://hub.docker.com/r/minio/minio/), and acquire an ENDPOINT (URL), ACCESS_KEY, and a SECRET_KEY.

- If you want to add your own minio object you can pass it in the constructor like so:

    Install python's [Minio](https://docs.min.io/docs/python-client-quickstart-guide.html) module.

    ```python
    from minio import Minio
    from pyminio import Pyminio

    minio_obj = Minio(
        endpoint='<your-minio-endpoint>',  # e.g. "localhost:9000/"
        access_key='<your-minio-access-key>',
        secret_key='<your-minio-secret-key>'
    )
    pyminio_client = Pyminio(minio_obj=minio_obj)
    ```

- If you don't want to handle with minio, you can do this instead:

    ```python
    from pyminio import Pyminio

    pyminio_client = Pyminio.from_credentials(
        endpoint='<your-minio-endpoint>',  # e.g. "localhost:9000/"
        access_key='<your-minio-access-key>',
        secret_key='<your-minio-secret-key>'
    )
    ```

## Usage
- [mkdirs](#mkdirsself-path-str)
- [listdir](#listdirself-path-str-files_only-bool--false-dirs_only-bool--false---tuplestr)
- [exists](#existsself-path-str---bool)
- [isdir](#isdirself-path-str)
- [truncate](#truncateself---pyminio)
- [rmdir](#rmdirself-path-str-recursive-bool--false---pyminio)
- [rm](#rmself-path-str-recursive-bool--false---pyminio)
- [cp](#cpself-from_path-str-to_path-str-recursive-bool--false---pyminio)
- [mv](#mvself-from_path-str-to_path-str-recursive-bool--false---pyminio)
- [get](#getself-path-str---objectdata)
- [get_last_object](#get_last_objectself-path-str---file)
- [put_data](#put_dataself-path-str-data-bytes-metadata-dict--none)
- [put_file](#put_fileself-file_path-str-to_path-str-metadata-dict--none)

### <a name="mkdirs"></a>mkdirs(self, path: str)
`Pyminio.mkdirs` will create the given full path if not exists like linux's `mkdir -p`.

This method must get a directory path or it will raise a ValueError.

```python
>>> pyminio_client.mkdirs('/foo/bar/baz/')
>>> pyminio_client.mkdirs('/foo/bar/baz')
ValueError /foo/bar/baz is not a valid directory path. must be absolute and end with /
```

### <a name="listdir"></a>listdir(self, path: str, files_only: bool = False, dirs_only: bool = False) -> Tuple[str]
`Pyminio.listdir` will return the directory's content as a tuple of directories and file names. Works like os's `listdir`.

This method must get a directory path or it will raise a ValueError.

```python
>>> pyminio_client.listdir('/foo/bar/baz/')
('file_name_1', 'file_name_2', 'directory_name/')
```

There is an option to use the files_only flag to get only files and dirs_only to get only directories from listdir.

```python
>>> pyminio_client.listdir('/foo/bar/baz/', files_only=True)
('file_name_1', 'file_name_2')
>>> pyminio_client.listdir('/foo/bar/baz/', dirs_only=True)
('directory_name/', )
```

### <a name="exists"></a>exists(self, path: str) -> bool
`Pyminio.exists` will return a boolean that confirm rather this path exists or not in the server. Works like os's `path.exists`.

```bash
/
├── foo
│   └── bar
│       └── baz
│           ├── file_name_1
│           └── file_name_2
```

```python
>>> pyminio_client.exists('/foo/bar/baz/file_name_1')
True
>>> pyminio_client.exists('/foo/bar/baz/file_name_3')
False
>>> pyminio_client.exists('/all/path/wrong/')  # not existing path
False
```

### <a name="isdir"></a>isdir(self, path: str)
`Pyminio.isdir` will return True only if the given path exists and is a directory. Works like `os.path.isdir`.

```python
>>> pyminio_client.isdir('/foo/bar/baz/file_name_1')  # existed file
False
>>> pyminio_client.isdir('/foo/bar/baz/directory_name/')  # existed directory
True
>>> pyminio_client.isdir('/all/path/wrong/but/directory/')  # not existed directory
False
```

### <a name="truncate"></a>truncate(self) -> Pyminio
`Pyminio.truncate` will delete all minio's content from the root directory.

```python
>>> pyminio_client.truncate()
```

### <a name="rmdir"></a>rmdir(self, path: str, recursive: bool = False) -> Pyminio
`Pyminio.rmdir` will delete the specified directory. Works like linux's `rmdir` / `rm (-r)`.

It will raise a `DirectoryNotEmptyError` if given directory is not empty, except if the recursive flag is set and then it will delete given directory's path recursively.

This method must get a directory path or it will raise a ValueError.

```python
>>> pyminio_client.rmdir('/foo/bar/baz/directory_name/')  # empty directory
>>> pyminio_client.rmdir('/foo/bar/')  # non-empty directory
DirectoryNotEmptyError: can not recursively delete non-empty directory
>>> pyminio_client.rmdir('/foo/bar/', recursive=True)
```

### <a name="rm"></a>rm(self, path: str, recursive: bool = False) -> Pyminio
`Pyminio.rm` works like [rmdir](#rmdirself-path-str-recursive-bool--false---pyminio) only that it can delete files too. Works like linux's `rm (-r)`.

```python
>>> pyminio_client.rm('/foo/bar/baz/file_name')
```

### <a name="cp"></a>cp(self, from_path: str, to_path: str, recursive: bool = False) -> Pyminio
`Pyminio.cp` will copy one file or directory to given destination. Works like linux's `cp (-r)`.

This method can only copy recursively when the recursive flag is True. If not, it will raise a ValueError.

### How will the copy accure? (all directories are copied recursively in this examples)
| src path   | dst path  | dst exists | new dst      | Explain                                                                |
| ---------- | --------- | ---------- | ------------ | ---------------------------------------------------------------------- |
| /foo/bar   | /foo/baz  |    ---     | /foo/baz     | The file's name will be copied from bar to baz as well.                |
| /foo1/bar  | /foo2/    |    True    | /foo/bar     | The file will be copied to '/foo2/bar'                                 |
| /foo/bar/  | /foo/     |    True    | /foo/        | The content of '/foo/bar/' will be copied to '/foo/'                   |
| /foo1/bar/ | /foo2/    |   False    | /foo2/bar/   | '/foo1/bar/' will be copied recursively to '/foo2/bar/'                |
| /foo1/bar/ | /foo2/baz |    ---     |     ---      | ValueError will be raised in attempting to copy directory in to a file |

```python
>>> pyminio_client.cp('/foo/bar', '/foo/baz')
>>> pyminio_client.cp('/foo1/bar', '/foo2/')
>>> pyminio_client.cp('/foo/bar/', '/foo/', recursive=True)
>>> pyminio_client.cp('/foo1/bar/', '/foo2/', recursive=True)
>>> pyminio_client.cp('/foo1/bar/', '/foo2/baz', recursive=True)
ValueError: can not activate this method from directory to a file.
```

### <a name="mv"></a>mv(self, from_path: str, to_path: str, recursive: bool = False) -> Pyminio
`Pyminio.mv` works like [cp](#cpself-from_path-str-to_path-str-recursive-bool--false---pyminio) only that it removes the source after the transfer has been completed. Works like linux's `mv`.

This method can only move recursively when the recursive flag is True. If not, it will raise a ValueError.

```python
>>> pyminio_client.mv('/foo/bar/', '/foo/baz/')
```

### <a name="get"></a>get(self, path: str) -> ObjectData
`Pyminio.get` return an object from given path. This object will be returned as a `pyminio.File` object or an `pyminio.Folder` object, that both inherit from `pyminio.ObjectData`.

This objects will contain metadata, their path and name.

```python
>>> pyminio_client.get('/foo/bar/baz')
File(name='baz', 
     full_path='/foo/bar/baz', 
     metadata={
         'is_dir': False, 
         'last_modified': time.struct_time(...), 
         'size': ..., 
         'content-type': ...
     }, 
     data=...)
```

### <a name="get_last_object"></a>get_last_object(self, path: str) -> File
`Pyminio.get_last_object` will return the last modified object inside a given directory.

This method must get a directory path or it will raise a ValueError.

```python
>>> pyminio_client.get_last_object('/foo/bar/')
File(name='baz', 
     full_path='/foo/bar/baz', 
     metadata={
         'is_dir': False, 
         'last_modified': time.struct_time(...), 
         'size': ..., 
         'content-type': ...
     }, 
     data=...)
```

### <a name="put_data"></a>put_data(self, path: str, data: bytes, metadata: Dict = None)
`Pyminio.put_data` gets a path, data in bytes, and some metadata, and create an object inside the given path.

```python
>>> data = b'test'
>>> metadata = {'Pyminio-is': 'Awesome'}
>>> pyminio_client.put_data(path='/foo/bar/baz', data=data, metadata=metadata)
```

### <a name="put_file"></a>put_file(self, file_path: str, to_path: str, metadata: Dict = None)
`Pyminio.put_file` works like [put_data](#put_dataself-path-str-data-bytes-metadata-dict--none) only that instead of data it gets a path to a file in you computer. Then it will copy this file to the given location.

```python
>>> metadata = {'Pyminio-is': 'Awesome'}
>>> pyminio_client.put_file(to_path='/foo/bar/baz', file_path='/mnt/some_file', metadata=metadata)
```

## Contribute

All contributions are welcome:

- Read the [issues](https://github.com/IamTugy/pyminio/issues), Fork the [project](https://github.com/IamTugy/pyminio) and create a new Pull Request.
- Request a new feature creating a `New issue` with the `enhancement` tag.
- Find any kind of errors in the code and create a `New issue` with the details, or fork the project and do a Pull Request.
- Suggest a better or more pythonic way for existing examples.

### Work environment

After forking the project and installing the dependencies, (like specified in the [installations](#installation) in part 2)
download the [minio docker](https://hub.docker.com/r/minio/minio/) and start an instance in your computer for development and testing.

Export The same environment variables you've used to set up your local minio:
```bash
export MINIO_TEST_CONNECTION="<your API host>" # example: 127.0.0.1:9000
export MINIO_TEST_ACCESS_KEY="<your user>" # example: ROOTNAME
export MINIO_TEST_SECRET_KEY="<your password>" # example: CHANGEME123
```

to run the tests run:
```bash
poetry run pytest tests
```
#### Don't forget to write tests, and to run all the tests before making a pull request.
