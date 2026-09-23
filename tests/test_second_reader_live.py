"""The second reader against a live pair: what it sees, and what it leaves.

Measured before it is trusted, like every program migkit drives: it must
write nothing to either side, and its verdict must match what was seeded.
"""
import pytest

from migkit import second_reader as sr
from migkit.config import Endpoint
from tests.conftest import needs_docker, psql

pytestmark = [needs_docker,
              pytest.mark.skipif(not sr.interpreter(),
                                 reason="the second reader is not installed")]

CATALOG = ("select coalesce(string_agg(n.nspname || '.' || c.relname, ','"
           " order by 1), '') from pg_class c join pg_namespace n"
           " on n.oid = c.relnamespace where n.nspname not in"
           " ('pg_catalog', 'information_schema', 'pg_toast')")


def _pg(port, sql):
    got = psql(port, sql)
    assert got.returncode == 0, got.stderr
    return got.stdout.strip()


def _job(pg_pair, kind, args):
    ep = {side: Endpoint(host="127.0.0.1", port=pg_pair[side],
                         user="postgres", password="test")
          for side in ("src", "dst")}
    return {"source": sr.connection("postgres", ep["src"], "postgres"),
            "target": sr.connection("postgres", ep["dst"], "postgres"),
            "tables": ["public.orders"], "kind": kind, "args": args}


def _seed(pg_pair, changed=False):
    for port in (pg_pair["src"], pg_pair["dst"]):
        _pg(port, "create table public.orders (id int primary key, v text);"
                  " insert into public.orders select g, 'v' || g"
                  " from generate_series(1, 50) g")
    if changed:
        _pg(pg_pair["dst"], "update public.orders set v = 'X' where id = 7")


@pytest.mark.parametrize("kind,args", [
    ("column", ["--count", "*"]),
    ("row", ["--hash", "*", "--primary-keys", "id"])])
def test_it_writes_nothing_to_either_side(pg_pair, kind, args):
    _seed(pg_pair)
    before = {s: _pg(pg_pair[s], CATALOG) for s in ("src", "dst")}
    answer = sr.run(_job(pg_pair, kind, args))
    assert answer.get("ok"), answer
    after = {s: _pg(pg_pair[s], CATALOG) for s in ("src", "dst")}
    assert before == after


def test_equal_data_reads_equal_the_second_way(pg_pair):
    _seed(pg_pair)
    got = sr.findings(sr.run(_job(pg_pair, "row",
                                  ["--hash", "*", "--primary-keys", "id"])),
                      "postgres")
    assert [r.status for r in got] == ["ok"], [(r.status, r.detail)
                                               for r in got]


def test_a_changed_value_is_seen_the_second_way(pg_pair):
    _seed(pg_pair, changed=True)
    got = sr.findings(sr.run(_job(pg_pair, "row",
                                  ["--hash", "*", "--primary-keys", "id"])),
                      "postgres")
    assert [r.status for r in got] == ["diff"], [(r.status, r.detail)
                                                 for r in got]


def test_it_leaves_nothing_in_the_users_home(pg_pair):
    """Measured: with the state-directory variable named wrong, the reader
    made its default directory in the user's home and looked there. It
    must keep everything in the directory made for the run."""
    from pathlib import Path
    home_dir = Path.home() / ".config" / "google-pso-data-validator"
    existed = home_dir.exists()
    _seed(pg_pair)
    assert sr.run(_job(pg_pair, "column", ["--count", "*"])).get("ok")
    if not existed:
        assert not home_dir.exists()
