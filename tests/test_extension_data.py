"""The rows an extension brings with it.

An extension is three things: functions, a version, and sometimes data.
migkit already compared the first two - a live pair with `hstore 1.8` on one
side and `1.6` on the other reports `DIFF version mismatch: hstore 1.8`, and
always did. The third had nobody looking at it.

PostGIS is the case that makes it matter. Coordinate systems live in
`spatial_ref_sys`, registered with `pg_extension_config_dump` so that a
custom SRID is dumped at all - and a restore does **not** overwrite rows the
target already has, so a target with the stock table keeps its own copy and
the custom entry is quietly absent. `pg_upgrade` has been reported failing
with `Cannot find SRID (4283) in spatial_ref_sys` for exactly this.

The check reads `pg_extension.extconfig`, which is the general mechanism
rather than a PostGIS special case, so any extension that registers data is
covered. No contrib extension in the stock image registers any - measured,
`extconfig` is NULL for `hstore`, `citext`, `pg_trgm` and `plpgsql` - so
these tests hand a table to an extension through the catalog, the same way
the collation tests age a collation. What is exercised is the check's
ability to find and compare a table an extension owns, which is the part
that was missing.
"""
import socket
import subprocess
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker

pytestmark = needs_docker

SRC, DST = 15527, 15528
NAMES = {SRC: "migkit-test-ext-src", DST: "migkit-test-ext-dst"}


def q(port, sql):
    return subprocess.run(
        ["docker", "exec", NAMES[port], "psql", "-U", "postgres", "-d",
         "postgres", "-At", "-q", "-v", "ON_ERROR_STOP=1", "-c", sql],
        capture_output=True, text=True)


@pytest.fixture(scope="module")
def ext_pair():
    for port, name in NAMES.items():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", name, "-e",
                        "POSTGRES_PASSWORD=test", "-p", f"{port}:5432",
                        "postgres:16"], check=True, capture_output=True)
    for port in NAMES:
        end = time.time() + 120
        while time.time() < end:
            with socket.socket() as s:
                s.settimeout(2)
                if s.connect_ex(("127.0.0.1", port)) == 0:
                    break
            time.sleep(1)
        for _ in range(60):
            if subprocess.run(["docker", "exec", NAMES[port], "pg_isready",
                               "-U", "postgres"],
                              capture_output=True).returncode == 0:
                break
            time.sleep(1)
        else:
            pytest.fail(f"postgres on {port} never answered")
        assert q(port, "create extension hstore").returncode == 0
    yield {"src": SRC, "dst": DST}
    for name in NAMES.values():
        subprocess.run(["docker", "rm", "-f", "-v", name],
                       capture_output=True)


def _own(port, rows):
    """Give `srids` to hstore, the way PostGIS registers spatial_ref_sys."""
    assert q(port, "drop table if exists srids;"
                   " create table srids (srid int primary key, proj text);"
                   f" insert into srids values {rows};").returncode == 0
    assert q(port, "update pg_extension set extconfig ="
                   " array['srids'::regclass]::oid[],"
                   " extcondition = array['']::text[]"
                   " where extname = 'hstore'").returncode == 0


def _engine(ext_pair, tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="ex", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=ext_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=ext_pair["dst"],
                              user="postgres", password="test"),
              databases=["postgres"], workers=1)
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


def test_no_stock_extension_registers_data(ext_pair):
    """Why these tests wire the catalog by hand rather than installing
    something - and a check that the mechanism is the one being used."""
    out = q(ext_pair["src"],
            "select extname||'='||coalesce(extconfig::text,'NULL')"
            " from pg_extension order by 1").stdout.split()
    assert out, out
    assert all(o.endswith("=NULL") for o in out), out


def test_a_row_the_target_is_missing_is_reported(ext_pair, tmp_path):
    _own(ext_pair["src"], "(4326,'wgs84'),(4283,'gda94')")
    _own(ext_pair["dst"], "(4326,'wgs84')")
    got = _engine(ext_pair, tmp_path)._extension_data("postgres")
    assert got.status == "diff", got.detail
    assert "hstore public.srids" in got.detail, got.detail
    assert "src=2|" in got.detail and "dst=1|" in got.detail, got.detail
    assert "does not overwrite" in got.fix_hint, got.fix_hint


def test_the_same_row_count_with_different_contents_is_caught(ext_pair,
                                                               tmp_path):
    """A count alone would call these equal, which is the whole reason the
    comparison is a checksum - a target carrying the stock definition of an
    SRID under the right number is the realistic shape of this."""
    _own(ext_pair["src"], "(4283,'gda94')")
    _own(ext_pair["dst"], "(4283,'something else')")
    counts = [q(p, "select count(*) from srids").stdout.strip()
              for p in (ext_pair["src"], ext_pair["dst"])]
    assert counts == ["1", "1"], counts

    got = _engine(ext_pair, tmp_path)._extension_data("postgres")
    assert got.status == "diff", got.detail
    assert "public.srids" in got.detail, got.detail


def test_matching_data_is_not_a_finding(ext_pair, tmp_path):
    _own(ext_pair["src"], "(4326,'wgs84'),(4283,'gda94')")
    _own(ext_pair["dst"], "(4326,'wgs84'),(4283,'gda94')")
    got = _engine(ext_pair, tmp_path)._extension_data("postgres")
    assert got.status == "ok", got.detail
    assert "1 tables owned by extensions" in got.detail, got.detail


def test_a_table_the_target_does_not_have_at_all(ext_pair, tmp_path):
    _own(ext_pair["src"], "(4326,'wgs84')")
    assert q(ext_pair["dst"], "drop table if exists srids").returncode == 0
    try:
        got = _engine(ext_pair, tmp_path)._extension_data("postgres")
        assert got.status == "diff", got.detail
        assert "missing on target" in got.detail, got.detail
    finally:
        _own(ext_pair["dst"], "(4326,'wgs84')")


def test_an_extension_owning_nothing_is_not_a_finding(ext_pair, tmp_path):
    for port in NAMES:
        assert q(port, "update pg_extension set extconfig = null,"
                       " extcondition = null where extname = 'hstore'"
                 ).returncode == 0
    try:
        got = _engine(ext_pair, tmp_path)._extension_data("postgres")
        assert got.status == "ok", got.detail
        assert "no extension on the source registers data" in got.detail
    finally:
        _own(ext_pair["src"], "(4326,'wgs84')")
        _own(ext_pair["dst"], "(4326,'wgs84')")


def test_versions_were_already_compared(ext_pair, tmp_path):
    """Recorded because the catalogue claimed otherwise. Installing an
    older hstore on the target makes the existing check speak up, and it
    needed no change."""
    assert q(ext_pair["dst"], "drop extension hstore cascade;"
                              " create extension hstore version '1.6'"
             ).returncode == 0
    try:
        got = [r for r in _engine(ext_pair, tmp_path).check_deep("postgres")
               if r.scope.endswith("extensions")]
        assert got[0].status == "diff", got[0].detail
        assert "version mismatch" in got[0].detail, got[0].detail
        assert "hstore 1.8" in got[0].detail, got[0].detail
    finally:
        q(ext_pair["dst"], "drop extension hstore cascade;"
                           " create extension hstore")
        _own(ext_pair["dst"], "(4326,'wgs84')")


def test_the_verdict_needs_no_server(tmp_path):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="e", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"),
              target=Endpoint(host="127.0.0.1", port=1, user="u",
                              password="p"), databases=["x"])
    hop.report_dir = lambda db=None: tmp_path
    eng = PostgresEngine(hop)

    bad = eng._extension_data_result(
        "x", [("postgis", "public.spatial_ref_sys", "src=2|9 dst=1|4")], 1,
        "hint")
    assert bad.status == "diff"
    assert "spatial_ref_sys" in bad.detail

    fine = eng._extension_data_result("x", [], 2, "hint")
    assert fine.status == "ok" and "2 tables owned" in fine.detail

    none = eng._extension_data_result("x", [], 0, "hint")
    assert none.status == "ok"
    assert "no extension on the source registers data" in none.detail
