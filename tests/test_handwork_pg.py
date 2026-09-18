"""The manual-work inventory, against a real PostgreSQL pair.

The entries here are the ones no DDL can fix. migkit's structural check
already diffs functions, views and constraints and writes the SQL to create
them, so listing those as manual work would send someone to redo finished
work. What survives that filter is contents living outside any table, objects
the WAL stream never mentions, and prerequisites that have to exist on the
target host before a load starts.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-hwpg-src", "migkit-test-hwpg-dst"
SRC_PORT, DST_PORT = 15495, 15496


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


pytestmark = [pytest.mark.docker,
              pytest.mark.skipif(not _docker(),
                                 reason="docker not available")]


def _wait(port, timeout=120):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _sql(name, db, sql):
    r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", name,
                        "psql", "-U", "postgres", "-d", db, "-At", "-c", sql],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout.strip()


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                        "POSTGRES_PASSWORD=test", "-p", f"{p}:5432",
                        "postgres:16"], check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    for n in (SRC, DST):
        for _ in range(45):
            r = subprocess.run(["docker", "exec", "-e", "PGPASSWORD=test", n,
                                "psql", "-U", "postgres", "-c", "select 1"],
                               capture_output=True)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail(f"{n} never accepted a connection")
    for n in (SRC, DST):
        _sql(n, "postgres", "create database shop")
    _sql(SRC, "shop", """
        create table keyed (id int primary key, v int);
        create table nokey (a int, b int);
        create unlogged table scratch (a int);
        create materialized view mv as select * from keyed;
        create view plain as select * from keyed;
        create function f() returns int language sql as 'select 1';
        select lo_from_bytea(0, '\\x00112233'::bytea);
    """)
    # an empty seed would let every assertion below pass for the wrong reason
    assert _sql(SRC, "shop", "select count(*) from pg_class c join"
                             " pg_namespace n on n.oid = c.relnamespace"
                             " where n.nspname = 'public'"
                             " and c.relkind in ('r','m')") == "4"
    assert _sql(SRC, "shop",
                "select count(*) from pg_largeobject_metadata") == "1"
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", n], capture_output=True)


def _engine(tmp_path, dst_port=DST_PORT):
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="p", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT,
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=dst_port,
                              user="postgres", password="test"),
              db_map={"shop": "shop"})
    hop.report_dir = lambda db=None: tmp_path
    return PostgresEngine(hop)


@pytest.fixture(scope="module")
def inv(pair, tmp_path_factory):
    eng = _engine(tmp_path_factory.mktemp("hwpg"))
    avail = set(_sql(DST, "postgres",
                     "select name from pg_available_extensions").splitlines())
    assert avail, "could not read the target's available extensions"
    return eng._handwork(avail)


def _row(inv, kind):
    from migkit.handwork import KINDS
    hits = [r for r in inv.rows() if r["item"] == KINDS[kind][1]]
    assert hits, f"no row for {kind}: {[r['item'] for r in inv.rows()]}"
    return hits[0]


def test_a_table_with_no_primary_key_is_listed(inv):
    d = _row(inv, "no-row-key")["detail"]
    assert "public.nokey" in d, d
    assert "public.keyed" not in d, d


def test_an_unlogged_table_is_reported_as_not_carried(inv):
    """It produces no WAL at all, so a WAL-based mover cannot see one row."""
    d = _row(inv, "not-carried")["detail"]
    assert "public.scratch" in d, d


def test_a_materialized_view_is_listed_as_needing_a_refresh(inv):
    """Its definition travels with the structural fix; its contents do not,
    and it reads as stale rather than failing."""
    d = _row(inv, "not-carried")["detail"]
    assert "public.mv" in d and "REFRESH" in d, d


def test_large_objects_are_counted_and_the_finding_names_the_leg(inv):
    """They live outside every table, so a mover working table by table never
    sees them - but migkit's own pg_dump path does carry them (measured with
    the exact flags `pgdump_move` uses). The finding has to say which, or it
    sends someone to move objects that already arrived."""
    d = _row(inv, "not-carried")["detail"]
    assert "pg_largeobject" in d, d
    assert "carries them" in d, d


def test_plain_views_and_functions_are_not_claimed(inv):
    """The structural check writes DDL for both, so listing them would be
    work that is already done."""
    text = " ".join(r["detail"] for r in inv.rows())
    assert "public.plain" not in text, text
    assert "public.f" not in text, text


def test_replica_identity_full_is_accepted_as_a_key(pair, tmp_path):
    """REPLICA IDENTITY FULL gives logical replication something to match an
    UPDATE against, so the table is not the operator's problem any more -
    and saying otherwise would send them to fix a table they already fixed."""
    _sql(SRC, "shop", "create table fixed (a int, b int);"
                      " alter table fixed replica identity full")
    try:
        avail = set(_sql(DST, "postgres",
                         "select name from pg_available_extensions"
                         ).splitlines())
        d = _row(_engine(tmp_path)._handwork(avail), "no-row-key")["detail"]
        assert "public.fixed" not in d, d
        assert "public.nokey" in d, d       # the probe still works at all
    finally:
        _sql(SRC, "shop", "drop table fixed")


def test_an_unreadable_target_reports_unknown_not_a_clean_bill(pair, tmp_path):
    """Pointed at a port with nothing behind it, the extension probe cannot
    run. Reporting zero missing extensions there would turn an unreachable
    target into a pass."""
    inv2 = _engine(tmp_path, dst_port=1)._handwork(set())
    rows = [r for r in inv2.rows() if "UNKNOWN" in r["detail"]]
    assert rows, [r["detail"] for r in inv2.rows()]
    assert any("Not zero" in r["detail"] for r in rows)
    assert inv2.unknowns() >= 1


def test_assess_ends_with_the_inventory_and_no_estimate(pair, tmp_path):
    from migkit.handwork import KINDS
    items = _engine(tmp_path).assess()
    titles = {i["item"] for i in items}
    assert KINDS["no-row-key"][1] in titles
    assert KINDS["not-carried"][1] in titles
    closing = [i for i in items if i["scope"] == "manual work"]
    assert closing and "does not estimate" in closing[0]["detail"]
    text = " ".join(i["item"] + i["detail"] for i in items).lower()
    for word in ("person-day", "man-day", "effort score"):
        assert word not in text, word
