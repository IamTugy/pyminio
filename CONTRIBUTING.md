## Contribute
All contributions are welcome:

- Read the [issues](https://github.com/IamTugy/pyminio/issues), Fork the [project](https://github.com/IamTugy/pyminio) and create a new Pull Request.
- Request a new feature creating a `New issue` with the `enhancement` tag.
- Find any kind of errors in the code and create a `New issue` with the details, or fork the project and do a Pull Request.
- Suggest a better or more pythonic way for existing examples.

### Work environment

After forking the project, make sure you have poetry installed, 
than install the dependencies using
```bash
poetry install
```

Also install pre-commit and activate it:
```bash
pip install pre-commit
pre-commit install
```

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