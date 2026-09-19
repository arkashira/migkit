"""The manual-work inventory, against a real MySQL pair.

Everything asserted here was measured on the server first, and one entry had
to be corrected after measuring: events are NOT missing from migkit's dump
(`_dump_schema` passes `--events`) and the object check names a missing one.
They are listed as left behind because atlas - which generates the fix DDL -
does not model MySQL events and reports such a pair as clean. Detected, never
repaired. A wrong entry in this inventory sends someone to redo finished work,
and a missing one loses an object, so each one is pinned to a measurement.
"""
import socket
import subprocess
import time

import pytest

SRC, DST = "migkit-test-handwork-src", "migkit-test-handwork-dst"
SRC_PORT, DST_PORT = 13495, 13496


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


def _wait(port, timeout=180):
    end = time.time() + timeout
    while time.time() < end:
        with socket.socket() as s:
            s.settimeout(2)
            if s.connect_ex(("127.0.0.1", port)) == 0:
                return True
        time.sleep(2)
    return False


def _mysql(name, sql, db=""):
    args = ["docker", "exec", name, "mysql", "-uroot", "-ptest", "-N"]
    if db:
        args += [db]
    r = subprocess.run(args + ["-e", sql], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr
    return r.stdout


@pytest.fixture(scope="module")
def pair():
    for n, p in ((SRC, SRC_PORT), (DST, DST_PORT)):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)
        subprocess.run(["docker", "run", "-d", "--name", n, "-e",
                        "MYSQL_ROOT_PASSWORD=test", "-p", f"{p}:3306",
                        "mysql:8"], check=True, capture_output=True)
    for p in (SRC_PORT, DST_PORT):
        assert _wait(p)
    for n in (SRC, DST):
        for _ in range(90):
            r = subprocess.run(["docker", "exec", n, "mysql", "-uroot",
                                "-ptest", "-e", "select 1"],
                               capture_output=True)
            if r.returncode == 0:
                break
            time.sleep(2)
        else:
            pytest.fail(f"{n} never accepted a connection")
    _mysql(SRC, "create database shop")
    _mysql(DST, "create database shop")
    # one of each kind of residue
    _mysql(SRC, """
        create table keyed (id int primary key, v int);
        create table uniq (a int not null, b int, unique key u (a));
        create table nokey (a int, b int);
        create table mem (a int) engine=MEMORY;
        create view vw as select * from keyed;
        create event ev on schedule every 1 day do delete from keyed
            where id < 0;
        create user 'gone'@'%' identified by 'x';
    """, "shop")
    # a definer that exists on the source and not on the target
    subprocess.run(["docker", "exec", SRC, "mysql", "-uroot", "-ptest",
                    "shop", "-e",
                    "create definer='gone'@'%' procedure p() select 1"],
                   check=True, capture_output=True)
    # a silently empty seed would make every assertion below pass for nothing
    assert _mysql(SRC, "select count(*) from information_schema.tables"
                       " where table_schema='shop'").strip() == "5"
    assert _mysql(SRC, "select count(*) from information_schema.events"
                       " where event_schema='shop'").strip() == "1"
    yield
    for n in (SRC, DST):
        subprocess.run(["docker", "rm", "-f", "-v", n], capture_output=True)


def _engine(tmp_path):
    from migkit.config import Endpoint, Hop
    from migkit.engines.mysql import MySQLEngine
    hop = Hop(name="m", engine="mysql",
              source=Endpoint(host="127.0.0.1", port=SRC_PORT, user="root",
                              password="test"),
              target=Endpoint(host="127.0.0.1", port=DST_PORT, user="root",
                              password="test"),
              db_map={"shop": "shop"})
    hop.report_dir = lambda db=None: tmp_path
    return MySQLEngine(hop)


@pytest.fixture(scope="module")
def inv(pair, tmp_path_factory):
    return _engine(tmp_path_factory.mktemp("hw"))._handwork()


def _row(inv, kind):
    from migkit.handwork import KINDS
    want = KINDS[kind][1]
    hits = [r for r in inv.rows() if r["item"] == want]
    assert hits, f"no row for {kind}: {[r['item'] for r in inv.rows()]}"
    return hits[0]


def test_a_table_with_neither_key_is_listed(inv):
    d = _row(inv, "no-row-key")["detail"]
    assert "nokey" in d and "mem" in d, d


def test_a_unique_not_null_index_counts_as_a_key(inv):
    """Row-based replication can find a row by one, so it is not manual work
    and listing it would send someone to fix a table that is fine."""
    d = _row(inv, "no-row-key")["detail"]
    assert "uniq" not in d, d
    assert "keyed" not in d, d


def test_an_event_is_reported_as_left_behind(inv):
    d = _row(inv, "not-carried")["detail"]
    assert "ev" in d and "scheduled events" in d, d
    # the reason has to travel with the finding, or the next reader guesses
    assert "no tool generates the DDL" in d, d


def test_migkit_sees_the_event_but_cannot_generate_its_ddl(pair, tmp_path):
    """The measurement the entry above rests on, and it is a narrow one.

    migkit's dump does include the event, and the object check names it. The
    gap is the repair: atlas writes the fix DDL and does not model MySQL
    events, so it reports the pair as clean. Detected, never fixed.

    An earlier version of this test asserted that migkit's dump omitted
    events, which was simply wrong - `_dump_schema` passes `--events`. If
    atlas ever learns about events, this test goes red and the inventory entry
    has to go with it.
    """
    eng = _engine(tmp_path)
    dump = eng._dump_schema("src", "shop")
    assert "EVENT" in dump.upper(), "migkit's own dump lost the event"
    results = eng.check_schema("shop")
    named = [r for r in results if "event" in r.detail and "ev" in r.detail]
    assert named, [r.detail for r in results]
    # Ask the direct question rather than reading atlas's overall verdict:
    # this fixture has table differences too, so atlas says "diff" for
    # reasons that have nothing to do with the event.
    fix = tmp_path / "atlas-fix.sql"
    # The target is missing five tables and a view, so atlas has plenty to
    # write about. Asserting the file exists keeps the next assertion from
    # passing vacuously on an atlas that produced nothing at all.
    assert fix.exists(), sorted(f.name for f in tmp_path.iterdir())
    text = fix.read_text().upper()
    assert "CREATE TABLE" in text, text[:300]
    assert "EVENT" not in text, (
        "atlas now generates DDL for events - they are carried, and the "
        "inventory entry must be removed")


def test_a_memory_table_is_reported_as_not_carried(inv):
    d = _row(inv, "not-carried")["detail"]
    assert "mem (MEMORY)" in d, d


def test_an_enabled_event_needs_a_decision_before_cutover(inv):
    d = _row(inv, "decide-then-apply")["detail"]
    assert "ev" in d, d


def test_a_definer_missing_on_the_target_is_a_prerequisite(inv):
    r = _row(inv, "target-prereq")
    assert r["level"] == "fail"
    # named with the catalogue's own word for it, so the finding can be
    # matched back to information_schema without translation
    assert "gone" in r["detail"] and "routine p" in r["detail"], r["detail"]


def test_routines_and_triggers_are_not_claimed_as_left_behind(inv):
    """migkit dumps and diffs them, so listing them as uncarried would send
    someone to redo work that is already done.

    Scoped to the "not carried" row on purpose: the same routine legitimately
    appears under target-prereq, because its definer account is missing. That
    is a different claim about a different problem.
    """
    from migkit.handwork import KINDS
    left = [r["detail"] for r in inv.rows()
            if r["item"] == KINDS["not-carried"][1]]
    text = " ".join(left)
    assert text, "nothing was reported as left behind at all"
    assert "routine" not in text, text
    assert "vw" not in text, text


def test_the_inventory_counts_and_refuses_to_estimate(inv):
    assert inv.total() >= 4
    text = " ".join(r["detail"] + r["item"]
                    for r in inv.rows() + inv.summary()).lower()
    for word in ("person-day", "man-day", "estimated", " hours", " days"):
        assert word not in text, word
    assert "does not estimate" in text


def test_assess_includes_the_inventory_rows(pair, tmp_path):
    from migkit.handwork import KINDS
    items = _engine(tmp_path).assess()
    titles = {i["item"] for i in items}
    assert KINDS["no-row-key"][1] in titles
    assert any(i["scope"] == "manual work" for i in items)
