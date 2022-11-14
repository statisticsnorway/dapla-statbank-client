### Poetry venv
Installing the dev-dependencies in a new environment can be done with the following command:
```bash
poetry install --with dev
```

### Pytest suites
```bash
poetry run pytest --cov -v -m "not integration_dapla"
```
The marker "integration_dapla" is for running specific tests locally on a dapla. They are placed in tests/test_integration_dapla.py .\
Use this flag in the command-line call to avoid running them on other platforms.


### Pytest coverage
```bash
poetry run pytest --cov=statbank --cov-report term-missing
# or
poetry run pytest -v -m "not integration_dapla" --cov=statbank --cov-report term-missing
```
Run this when developing tests.
If you achieve a higher testing coverage make sure to increase the threshold in the workflow.
.github/workflows/tests.yml
(at the bottom)


### Running the pre-commit hooks locally
```bash
poetry run pre-commit run --all-files
```
Several of the pre-commit hooks will try to modify the files on a fail. Re-running the command might therefore result in a different result the second time.

### Type-checking with Mypy
```bash
poetry run mypy .
```


### Configuration
pflake8 has its config in pyproject.toml, not in .flake8


### CI/CD - set up actions


### Publish to Pypi
The action to publish to Pypi is connected with a workflow to releases from Github.
So to publish to Pypi, make sure everything is done, and in the main branch, bump the version in pyproject.toml, add a tag and release on Github.
