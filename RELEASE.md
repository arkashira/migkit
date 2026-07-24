# Releasing migkit to PyPI

Everything up to the upload is automated and CI-checked (`package` job builds
the distribution, validates it with `twine check`, and installs the wheel in a
clean env). The final upload is the only manual step - it publishes under your
PyPI identity and needs your token, so run it yourself.

## One-time
- Create a PyPI account, then an **API token**: pypi.org -> Account settings ->
  API tokens -> *Add API token* (scope it to the `migkit` project after the
  first upload). The token looks like `pypi-AgEI...`.

## Each release
```bash
# 1. bump version in pyproject.toml + add a CHANGELOG entry, commit
# 2. build a clean distribution
rm -rf dist && python -m build
# 3. validate
twine check dist/*
# 4. upload (paste your token as the password; username is literally __token__)
twine upload -u __token__ -p pypi-XXXXXXXX dist/*
# or put the token in ~/.pypirc / the TWINE_PASSWORD env var instead of -p
```

Then `pip install migkit` works for everyone.

Tip: test against TestPyPI first -
`twine upload -r testpypi -u __token__ -p pypi-TEST... dist/*`.
