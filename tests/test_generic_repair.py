"""`migkit sync --kind rows` on the engine that had no driver of its own.

Generic was the last engine that could only tell you a table was wrong. The
check counted the differing rows and dropped which ones they were, so `sync`
found no drilldown and said there was nothing to repair about a hop it had
just called `diff` - an empty answer standing in for a no.

Two measurements shaped what is here. The rows are a second call, because
`--stats` and the rows do not come out of one:

    --stats --json   {"rows_A": 0, ... "exclusive_A": 1, "updated": 2, ...}
    --json           ["-", ["11", "v11"]]      the source's version
                     ["+", ["3", "CHANGED"]]   the target's
                     ["-", ["3", "v3"]]

And reladiff itself can write. The comparison is a subprocess, but the same
library opens a connection from python that executes statements - which is
the only writer this engine has, since migkit has no driver for snowflake or
clickhouse. It is a borrowed one, and the tests below pin the two places it
does not carry a value faithfully: bytes go in as the characters they spell,
and an update refuses a Decimal outright while an insert of the same value
renders it exactly.
"""
import json

import pytest

from migkit.config import Endpoint, Hop
from migkit.util import which
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

RELADIFF = which("reladiff")
needs_reladiff = pytest.mark.skipif(
    not RELADIFF, reason="reladiff is not where migkit would look for it")


def _engine(pg_pair, tmp_path, tables=("t",), key="id"):
    from migkit.engines.generic import GenericEngine

    def url(port):
        return f"postgresql://postgres:test@127.0.0.1:{port}/postgres"
    hop = Hop(name="g", engine="generic",
              source=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["src"])}),
              target=Endpoint(host="x", port=0, user="", password="",
                              options={"url": url(pg_pair["dst"])}),
              options={"tables": list(tables), "key": key})
    hop.report_dir = lambda db=None: tmp_path
    return GenericEngine(hop)


def _seed(pg_pair, ddl, rows, target_changes=""):
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, f"{ddl} {rows}")
    if target_changes:
        assert psql(pg_pair["dst"], target_changes).returncode == 0


def _repair(eng, db="-"):
    actions = eng.repair_plan(db, "rows")
    for action in actions:
        eng.apply(db, action)
    return actions


PLAIN = ("create table t (id bigint primary key, v text);",
         "insert into t select g, 'v'||g from generate_series(1,20) g;")


@needs_reladiff
def test_the_three_kinds_of_difference_are_named_and_put_right(pg_pair,
                                                               tmp_path):
    _seed(pg_pair, *PLAIN, target_changes=(
        "update t set v='CHANGED' where id in (3,7);"
        " delete from t where id=11;"
        " insert into t values (99,'only here');"))
    eng = _engine(pg_pair, tmp_path)
    before = eng.check_data("-")
    assert [r.status for r in before] == ["diff"], [r.detail for r in before]

    assert sorted(p.name for p in tmp_path.glob("data-t.*")) == [
        "data-t.changed", "data-t.extra", "data-t.missing"]
    read = {kind: [json.loads(l) for l in
                   (tmp_path / f"data-t.{kind}").read_text().splitlines()]
            for kind in ("missing", "changed", "extra")}
    assert read == {"missing": [["11"]], "changed": [["3"], ["7"]],
                    "extra": [["99"]]}, read

    actions = _repair(eng)
    assert len(actions) == 1, actions
    assert "1 missing, 2 changed, 1 extra" in actions[0].note

    after = eng.check_data("-")
    assert [r.status for r in after] == ["ok"], [r.detail for r in after]
    # and from a connection of its own, not the one that did the repair
    assert psql(pg_pair["dst"], "select count(*), min(v), max(id) from t;"
                ).stdout.strip() == "20|v1|20"
    assert psql(pg_pair["dst"], "select v from t where id=3;"
                ).stdout.strip() == "v3"


@needs_reladiff
def test_a_key_that_is_not_the_first_column_still_addresses_the_row(pg_pair,
                                                                    tmp_path):
    """reladiff puts the key first in its output whatever its position in
    the table, and the columns after it arrive in an order that is neither
    the table's nor alphabetical - so a repair that read the second value as
    the second column would write the wrong field into the wrong row."""
    _seed(pg_pair, "create table t (a text, id bigint primary key, z text);",
          "insert into t select 'a'||g, g, 'z'||g from"
          " generate_series(1,6) g;",
          "update t set z='CHANGED' where id=2;")
    eng = _engine(pg_pair, tmp_path)
    assert eng.check_data("-")[0].status == "diff"
    assert [json.loads(l) for l in
            (tmp_path / "data-t.changed").read_text().splitlines()] == [["2"]]
    _repair(eng)
    assert eng.check_data("-")[0].status == "ok"
    assert psql(pg_pair["dst"], "select a, z from t where id=2;"
                ).stdout.strip() == "a2|z2"


@needs_reladiff
def test_a_two_column_key_is_read_in_the_order_it_was_given(pg_pair,
                                                            tmp_path):
    _seed(pg_pair,
          "create table t (x text, k1 int, v text, k2 int,"
          " primary key (k1,k2));",
          "insert into t values ('x1',1,'v1',7),('x2',2,'v2',8);",
          "update t set v='CHANGED' where k1=2;"
          " insert into t values ('x9',9,'v9',9);")
    eng = _engine(pg_pair, tmp_path, key=["k1", "k2"])
    assert eng.check_data("-")[0].status == "diff"
    assert [json.loads(l) for l in
            (tmp_path / "data-t.changed").read_text().splitlines()] == [
        ["2", "8"]]
    assert [json.loads(l) for l in
            (tmp_path / "data-t.extra").read_text().splitlines()] == [
        ["9", "9"]]
    _repair(eng)
    assert eng.check_data("-")[0].status == "ok"
    assert psql(pg_pair["dst"], "select v from t where k1=2 and k2=8;"
                ).stdout.strip() == "v2"


@needs_reladiff
def test_an_exact_number_is_not_rounded_on_its_way_in(pg_pair, tmp_path):
    """The borrowed writer refuses a Decimal in an update, and the float it
    would otherwise take is not the number that was read. The digits go in
    instead, which is what the same writer renders for an insert."""
    _seed(pg_pair,
          "create table t (id bigint primary key, n numeric(30,10));",
          "insert into t values (1, 0.0000000001),"
          " (2, 12345678901234567890.0123456789), (3, -4.50);",
          "update t set n = 9.9 where id in (1,2);"
          " delete from t where id = 3;")
    eng = _engine(pg_pair, tmp_path)
    assert eng.check_data("-")[0].status == "diff"
    _repair(eng)
    assert eng.check_data("-")[0].status == "ok"
    got = psql(pg_pair["dst"], "select n from t order by id;").stdout.split()
    assert got == ["0.0000000001", "12345678901234567890.0123456789",
                   "-4.5000000000"], got


@needs_reladiff
def test_every_target_row_it_overwrites_or_removes_is_kept_first(pg_pair,
                                                                 tmp_path):
    _seed(pg_pair, *PLAIN, target_changes=(
        "update t set v='target value worth keeping' where id=4;"
        " insert into t values (77,'also worth keeping');"))
    eng = _engine(pg_pair, tmp_path)
    eng.check_data("-")
    _repair(eng)

    saved = [json.loads(l) for l in
             (tmp_path / "undo" / "t.rows.jsonl").read_text().splitlines()]
    assert {tuple(r["key"]) for r in saved} == {("4",), ("77",)}, saved
    by_key = {tuple(r["key"]): r["row"] for r in saved}
    assert by_key[("4",)]["v"] == "target value worth keeping"
    assert by_key[("77",)]["v"] == "also worth keeping"
    # a row that was only ever missing on the target had no old value, so
    # the file must not claim one
    assert all(r["key"] != ["11"] for r in saved), saved


@needs_reladiff
def test_a_clean_run_clears_what_the_last_one_listed(pg_pair, tmp_path):
    """The files are the repair's input, so a run that finds nothing has to
    remove them - otherwise `sync` would delete rows a later check found to
    be fine."""
    _seed(pg_pair, *PLAIN,
          target_changes="insert into t values (99,'stray');")
    eng = _engine(pg_pair, tmp_path)
    eng.check_data("-")
    assert (tmp_path / "data-t.extra").exists()

    psql(pg_pair["dst"], "delete from t where id=99;")   # put right by hand
    assert eng.check_data("-")[0].status == "ok"
    assert not list(tmp_path.glob("data-t.*")), list(tmp_path.iterdir())
    assert eng.repair_plan("-", "rows") == []


@needs_reladiff
def test_nothing_to_repair_when_the_two_sides_agree(pg_pair, tmp_path):
    _seed(pg_pair, *PLAIN)
    eng = _engine(pg_pair, tmp_path)
    assert [r.status for r in eng.check_data("-")] == ["ok"]
    assert eng.repair_plan("-", "rows") == []
    assert eng.repair_plan("-", "sequences") == []
    assert eng.repair_plan("-", "schema") == []


@needs_reladiff
def test_binary_is_refused_rather_than_written_as_the_letters_it_spells(
        pg_pair, tmp_path):
    """Measured on the borrowed writer: `b'\\x00\\x01'` renders as a
    two-character string, so a bytea column would come back as text. The
    target is left as the check found it."""
    _seed(pg_pair, "create table t (id bigint primary key, b bytea);",
          "insert into t values (1, '\\x0001'::bytea),"
          " (2, '\\xdeadbeef'::bytea);",
          "update t set b = '\\xffff'::bytea where id = 1;"
          " delete from t where id = 2;")
    eng = _engine(pg_pair, tmp_path)
    assert eng.check_data("-")[0].status == "diff"
    actions = eng.repair_plan("-", "rows")
    assert actions, "nothing to repair, so the refusal cannot be shown"

    with pytest.raises(SystemExit) as caught:
        eng.apply("-", actions[0])
    said = str(caught.value)
    assert "binary" in said and "t.b" in said, said
    assert "Nothing has been written" in said, said
    # and it really was not: the target is as it was
    assert psql(pg_pair["dst"], "select count(*) from t;").stdout.strip() == "1"
    assert psql(pg_pair["dst"], "select encode(b,'hex') from t where id=1;"
                ).stdout.strip() == "ffff"


@needs_reladiff
def test_the_rows_are_a_second_call_because_stats_does_not_carry_them(
        pg_pair, tmp_path):
    """The measurement the design rests on, run against the tool itself: ask
    for both and the rows are not there. If a later reladiff prints them
    together this test fails and the second call can go."""
    import subprocess
    _seed(pg_pair, *PLAIN,
          target_changes="update t set v='CHANGED' where id=5;")
    eng = _engine(pg_pair, tmp_path)
    from migkit.util import diff_run_config
    conf = tmp_path / "run.toml"
    conf.write_text(diff_run_config(eng._url("src"), "t", eng._url("dst"),
                                    "t"))

    def argv(**kw):
        # the bare name is not on PATH: reladiff lives beside the
        # interpreter, which is where migkit's own `run` looks for it; the
        # addresses go in the run's file, as `_reladiff` gives them
        cmd = eng._reladiff_cmd("t", ["-c", "%", "--json"], jobs=1, **kw)
        cmd[2] = str(conf)
        return [RELADIFF] + cmd[1:]

    both = subprocess.run(argv(), capture_output=True, text=True)
    assert "--stats" in eng._reladiff_cmd("t", [], jobs=1)
    rows = [l for l in both.stdout.splitlines() if l.startswith("[")]
    assert not rows, rows
    assert '"updated": 1' in both.stdout, both.stdout

    alone = subprocess.run(argv(stats=False), capture_output=True, text=True)
    assert "--stats" not in eng._reladiff_cmd("t", [], jobs=1, stats=False)
    assert [json.loads(l)[0] for l in alone.stdout.splitlines()
            if l.startswith("[")] == ["+", "-"], alone.stdout


@needs_reladiff
def test_differences_that_cannot_be_localised_say_so_and_leave_no_list(
        pg_pair, tmp_path, monkeypatch):
    """The counts found differences and the row pass came back with nothing.
    An empty drilldown would read as nothing to repair, so the check says
    the rows were not localised and clears what an earlier run left."""
    _seed(pg_pair, *PLAIN,
          target_changes="insert into t values (99,'stray');")
    eng = _engine(pg_pair, tmp_path)
    assert eng.check_data("-")[0].status == "diff"
    assert (tmp_path / "data-t.extra").exists()

    real = eng._reladiff

    def blind(table, extra, jobs=None, stats=True):
        p = real(table, extra, jobs, stats)
        if not stats:
            p.stdout = ""
            p.stderr = "connection reset while listing rows"
        return p
    monkeypatch.setattr(eng, "_reladiff", blind)
    got = eng.check_data("-")
    assert got[0].status == "diff", got[0].detail
    assert "rows not localised" in got[0].detail, got[0].detail
    assert "connection reset" in got[0].detail, got[0].detail
    assert not list(tmp_path.glob("data-t.*")), list(tmp_path.iterdir())
    assert eng.repair_plan("-", "rows") == []
