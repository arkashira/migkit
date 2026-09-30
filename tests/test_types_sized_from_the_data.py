"""Columns sized from the data they hold (leapfrog research P14, the hop
option `types_from_data`): a MySQL `TEXT` holding codes of at most 38
characters is built on PostgreSQL as a text of at most 64, the narrowing
recorded with its evidence, and proved on the sandbox's fixture - the
longest value seen goes in on both sides, one past the new length goes
in on the source and is refused on the target, as recorded. A hop that
does not turn the option on builds what it always built.
"""
import json
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop

pytestmark = [pytest.mark.docker]

MY, PG = "migkit-test-size-my", "migkit-test-size-pg"
SB_MY, SB_PG = "migkit-test-size-sbmy", "migkit-test-size-sbpg"
MY_PORT, PG_PORT, SB_MY_PORT, SB_PG_PORT = 16294, 16295, 16296, 16297
PG_MAJOR = 17
MY_SMALL = ["--innodb-buffer-pool-size=16M", "--performance-schema=OFF",
            "--log-bin-trust-function-creators=1"]
LONGEST = "A" * 38


def _sh(*a, **kw):
    return subprocess.run(list(a), capture_output=True, text=True, **kw)


def _docker():
    try:
        return _sh("docker", "info", timeout=10).returncode == 0
    except Exception:
        return False


def _pg(sql, db="sz"):
    got = _sh("docker", "exec", "-i", PG, "psql", "-U", "postgres", "-d",
              db, "-At", "-v", "ON_ERROR_STOP=1", input=sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _my(*statements, db="sz"):
    import pymysql
    conn = pymysql.connect(host="127.0.0.1", port=MY_PORT, user="root",
                           password="test", database=db, autocommit=True)
    try:
        with conn.cursor() as cur:
            for s in statements:
                cur.execute(s)
            return cur.fetchall()
    finally:
        conn.close()


@pytest.fixture(scope="module")
def servers():
    if not _docker():
        pytest.skip("docker not available")
    from migkit.storedcode.sandbox import PG_DOCKERFILE, pg_tag
    tag = pg_tag(PG_MAJOR)
    if _sh("docker", "image", "inspect", tag).returncode != 0:
        assert _sh("docker", "build", "-t", tag, "-", timeout=900,
                   input=PG_DOCKERFILE.format(v=PG_MAJOR)).returncode == 0
    for name, port in ((MY, MY_PORT), (SB_MY, SB_MY_PORT)):
        _sh("docker", "rm", "-f", "-v", name)
        _sh("docker", "run", "-d", "--name", name, "-e",
            "MYSQL_ROOT_PASSWORD=test", "-p", f"127.0.0.1:{port}:3306",
            "mysql:8.4", *MY_SMALL)
    for name, port, image in ((PG, PG_PORT, f"postgres:{PG_MAJOR}"),
                              (SB_PG, SB_PG_PORT, tag)):
        _sh("docker", "rm", "-f", "-v", name)
        _sh("docker", "run", "-d", "--name", name, "-e",
            "POSTGRES_PASSWORD=test", "-p", f"127.0.0.1:{port}:5432", image)
    try:
        for name in (MY, SB_MY):
            for _ in range(90):
                if _sh("docker", "exec", name, "mysql", "-uroot", "-ptest",
                       "-h127.0.0.1", "--protocol=tcp", "-e",
                       "select 1").returncode == 0:
                    break
                time.sleep(2)
        for name in (PG, SB_PG):
            for _ in range(60):
                if _sh("docker", "exec", name, "psql", "-U", "postgres",
                       "-c", "select 1").returncode == 0:
                    break
                time.sleep(1)
        for port in (MY_PORT, PG_PORT, SB_MY_PORT, SB_PG_PORT):
            with socket.socket() as s:
                s.settimeout(5)
                assert s.connect_ex(("127.0.0.1", port)) == 0
        _my("create database sz", db=None)
        _my("create table codes (id int primary key, code text, note"
            " text)",
            f"insert into codes values (1, 'ab-1', null), (2, '{LONGEST}',"
            " null), (3, 'q', null)")
        _pg("create database sz", db="postgres")
        yield
    finally:
        for name in (MY, PG, SB_MY, SB_PG):
            _sh("docker", "rm", "-f", "-v", name)


def _engine(tmp_path, sized=True):
    from migkit.engines.hetero import HeteroEngine
    options = {"source_engine": "mysql", "target_engine": "postgres",
               "proof_sandbox": {
                   "source": {"host": "127.0.0.1", "port": SB_MY_PORT,
                              "user": "root", "password": "test"},
                   "target": {"host": "127.0.0.1", "port": SB_PG_PORT,
                              "user": "postgres", "password": "test"}}}
    if sized:
        options["types_from_data"] = True
    hop = Hop(name="sz", engine="hetero", options=options,
              source=Endpoint(host="127.0.0.1", port=MY_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=PG_PORT,
                              user="postgres", password="test"),
              databases=["sz"])
    hop.report_dir = lambda db=None: tmp_path
    return HeteroEngine(hop)


def _length(column):
    return _pg("select coalesce(character_maximum_length::text, data_type)"
               " from information_schema.columns where table_name ="
               f" 'codes' and column_name = '{column}'")


def test_without_the_option_the_text_is_built_as_it_always_was(servers,
                                                               tmp_path):
    _pg("drop table if exists codes")
    _engine(tmp_path, sized=False).create_missing("sz")
    assert _length("code") == "text"
    assert not (tmp_path / "type-sizing.json").exists()


def test_a_text_is_sized_from_its_data_recorded_and_proved(servers,
                                                           tmp_path):
    _pg("drop table if exists codes")
    eng = _engine(tmp_path)
    eng.create_missing("sz")
    # the longest code is 38 characters: built as 64; a column holding
    # nothing gives no evidence and is built as it always was
    assert _length("code") == "64"
    assert _length("note") == "text"
    kept = json.loads((tmp_path / "type-sizing.json").read_text())
    assert set(kept) == {"codes.code"}, kept
    assert kept["codes.code"]["evidence"]["most_chars"] == 38, kept
    assert kept["codes.code"]["evidence"]["values"] == 3, kept
    schema = []
    eng.prove_converted("sz", schema)
    got = {r.scope: r for r in schema}["sz sized types"]
    # proved as recorded, and said: the owner's to accept
    assert got.status == "warn", got.detail
    assert got.detail.startswith("codes.code built as text of at most 64"
                                 " characters"), got.detail
    assert "not proved" not in got.detail, got.detail
    # the hop's own source was only read
    assert _my("select count(*) from codes") == ((3,),)


def test_a_target_that_refuses_a_value_the_source_holds_is_a_difference(
        servers, tmp_path):
    _pg("drop table if exists codes")
    eng = _engine(tmp_path)
    eng.create_missing("sz")
    # the target's column made narrower by hand than the record says, and
    # the source's long row gone so the fixture still arrives whole: the
    # record's longest value is refused there
    _pg("alter table codes alter column code type varchar(20)")
    _my("delete from codes where id = 2")
    try:
        schema = []
        eng.prove_converted("sz", schema)
        got = {r.scope: r for r in schema}["sz sized types"]
        assert got.status == "diff", got.detail
        assert got.detail.startswith("codes.code: a value the source holds"),\
            got.detail
    finally:
        _my(f"insert into codes values (2, '{LONGEST}', null)")
