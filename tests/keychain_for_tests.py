"""A keychain kept in a file, for the tests: `migkit init` stores
passwords with `keyring`, and a test must never write this machine's own.

In a test process `keyring.set_keyring(FileKeyring())`; for a `migkit`
it starts, `PYTHON_KEYRING_BACKEND=tests.keychain_for_tests.FileKeyring`
with the checkout on `PYTHONPATH`. The file is `MIGKIT_TEST_KEYCHAIN`.
"""
import json
import os

from keyring.backend import KeyringBackend


class FileKeyring(KeyringBackend):
    priority = 1

    def _path(self):
        return os.environ["MIGKIT_TEST_KEYCHAIN"]

    def _load(self):
        try:
            with open(self._path()) as f:
                return json.load(f)
        except FileNotFoundError:
            return {}

    def get_password(self, service, username):
        return self._load().get(f"{service}/{username}")

    def set_password(self, service, username, password):
        data = self._load()
        data[f"{service}/{username}"] = password
        with open(self._path(), "w") as f:
            json.dump(data, f)

    def delete_password(self, service, username):
        data = self._load()
        data.pop(f"{service}/{username}", None)
        with open(self._path(), "w") as f:
            json.dump(data, f)
