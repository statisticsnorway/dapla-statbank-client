### Before commit


### Running the pre-commit hooks locally
```bash
poetry run pre-commit run --all-files
```

### Set up Github actions


### Publish to Pypi
The action to publish to Pypi is connected with a workflow to releases from Github.
So to publish to Pypi, make sure everything is done, and in the main branch, bump the version in pyproject.toml, add a tag and release on Github.
