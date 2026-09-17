# Publishing migkit

The goal is that a stranger types one line and has a working `migkit`:

```bash
uv tool install migkit     # or pipx install migkit, or pip install migkit
brew install migkit
```

Everything in the repository is already set up for that. What is left needs
credentials and is therefore a human step.

## Verified locally

A wheel built from this tree installs into an empty environment and works with
no checkout, no venv activation and no `PYTHONPATH`:

```bash
python -m build --wheel
pip install dist/migkit-*.whl
cd /anywhere && migkit doctor     # capabilities, no configuration required
migkit init                       # writes ~/.config/migkit/hops.yaml, mode 600
```

All nine engines import from that install, and the version comes from
`migkit.__version__` alone - `pyproject.toml` reads it from there, so the two
cannot drift.

## Step 1: PyPI

```bash
python -m build                       # wheel + sdist
python -m twine upload dist/*         # needs a PyPI token
```

Until this is done `pip install migkit` cannot work, and the Homebrew formula
has nothing to point at. Check the name is free first - the PyPI project page
was unreachable behind a bot challenge when this was last looked at, so it has
not been confirmed either way.

## Step 2: Homebrew

`homebrew/migkit.rb` is the formula. Two things happen after the PyPI upload:

```bash
# fill in url + sha256 for the published sdist, then generate the resource
# stanzas - do not hand-write them, migkit requires every engine driver and
# the list has to match pyproject.toml exactly
brew update-python-resources homebrew/migkit.rb
```

Then either submit it to homebrew-core or publish a tap
(`arkashira/homebrew-tap`), which makes the install line
`brew install arkashira/tap/migkit`.

`brew install --HEAD` against the repository works before any of this.

## What bootstrap.sh is for now

Contributors only - it creates a development virtualenv and installs the test
extra. Nobody installing migkit needs it, and the README no longer mentions it.
