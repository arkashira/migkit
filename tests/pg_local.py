"""A throwaway PostgreSQL server for the tests that stand it in for a
server that speaks PostgreSQL's wire (CockroachDB, Aurora DSQL), started
from the PostgreSQL binaries on this machine (no container): a cluster in a
temporary directory on a free local port, its directory removed when the
module is done. A test module using it skips cleanly where there are no
binaries or no `psycopg2`.

It is PostgreSQL, not the server the engine is for: what a test proves
against it is what only relies on the surface they share - `information_schema`,
`pg_catalog` and standard SQL - and a statement of the engine's own
(`UPSERT`, `AS OF SYSTEM TIME`, `crdb_internal`, `sys.jobs`, `CREATE INDEX
ASYNC`) is never proved by it.
"""
import glob
import os
import pathlib
import pwd
import shutil
import socket
import subprocess
import tempfile
import time

import pytest


def _binaries():
    """The directory of PostgreSQL's server programs, or None."""
    for found in sorted(glob.glob("/usr/lib/postgresql/*/bin/initdb"),
                        reverse=True):
        return pathlib.Path(found).parent
    on_path = shutil.which("initdb")
    return pathlib.Path(on_path).parent if on_path else None


BIN = _binaries()


def _free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _account():
    """The account the server runs as: this one, or `postgres` where this
    one is root, which a PostgreSQL server will not run as."""
    if os.geteuid() != 0:
        return None
    try:
        return pwd.getpwnam("postgres").pw_name
    except KeyError:
        return None


def available():
    """Why a server cannot be started here, or ""."""
    if BIN is None:
        return "no PostgreSQL server programs on this machine"
    if os.geteuid() == 0 and _account() is None:
        return "PostgreSQL will not run as root, and there is no postgres account"
    try:
        import psycopg2  # noqa: F401
    except ImportError:
        return "the psycopg2 package is not installed"
    return ""


class Local:
    """One cluster: `port`, `connect()`, `run()` and `stop()`."""

    def __init__(self):
        self.user = _account()
        self.port = _free_port()
        self.dir = pathlib.Path(tempfile.mkdtemp(
            prefix="migkit-pg-", dir="/var/tmp"
            if pathlib.Path("/var/tmp").is_dir() else None))
        self.dir.chmod(0o755)
        self.data = self.dir / "data"
        if self.user:
            shutil.chown(self.dir, self.user)
        try:
            self._sh(f"{BIN}/initdb -D {self.data} -A trust -U postgres"
                     " -E UTF8 --locale=C.UTF-8")
            self._sh(f"{BIN}/pg_ctl -D {self.data} -w -t 60 -l"
                     f" {self.dir}/log start -o '-p {self.port} -c"
                     f" listen_addresses=127.0.0.1 -c"
                     f" unix_socket_directories={self.dir} -c fsync=off'")
        except Exception:
            self.stop()
            raise

    def _sh(self, command):
        if self.user:
            command = ["runuser", "-u", self.user, "--", "bash", "-c", command]
        else:
            command = ["bash", "-c", command]
        subprocess.run(command, check=True, capture_output=True, timeout=120)

    def connect(self, dbname="postgres"):
        import psycopg2
        end = time.time() + 20
        while True:
            try:
                conn = psycopg2.connect(host="127.0.0.1", port=self.port,
                                        user="postgres", dbname=dbname,
                                        connect_timeout=5)
                conn.autocommit = True
                return conn
            except psycopg2.OperationalError:
                if time.time() > end:
                    raise
                time.sleep(0.2)

    def run(self, sql, dbname="postgres"):
        """Every row `sql` answers (a list), or None."""
        conn = self.connect(dbname)
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
                return cur.fetchall() if cur.description else None
        finally:
            conn.close()

    def make(self, *names):
        for n in names:
            self.run(f'create database "{n}"')

    def stop(self):
        try:
            if self.data.exists():
                self._sh(f"{BIN}/pg_ctl -D {self.data} -m immediate -w stop")
        except Exception as e:  # noqa: BLE001 - it is going anyway
            self.stopped = str(e)
        shutil.rmtree(self.dir, ignore_errors=True)


needs_server = pytest.mark.skipif(bool(available()), reason=available())


@pytest.fixture(scope="module")
def server():
    """One throwaway cluster for a test module, removed after it."""
    if available():
        pytest.skip(available())
    one = Local()
    try:
        yield one
    finally:
        one.stop()
