"""A PostgreSQL source at wal_level replica keeps no logical slot: its
changes are followed by the WAL's physical records - read by the server,
or from its files on this machine - whose touched blocks are read again
by place and told apart by key (backlog LT.R9)."""
import json
from types import SimpleNamespace

import pytest

from migkit import changedepth, decide
from migkit.engines import enterprise_connectors as ec
from migkit.engines.postgres import (PostgresEngine, wal_record,
                                     wal_touched, waldump_records)

DUMP = """\
rmgr: Heap        len (rec/tot):     59/    59, tx:        735, lsn: 0/01567A30, prev 0/015679F8, desc: INSERT off: 3, flags: 0x08, blkref #0: rel 1663/5/16385 blk 0
rmgr: Heap        len (rec/tot):     72/    72, tx:        735, lsn: 0/01567A70, prev 0/01567A30, desc: UPDATE old_xmax: 735, old_off: 2, old_infobits: [], flags: 0x00, new_xmax: 0, new_off: 4, blkref #0: rel 1663/5/16385 blk 7, blkref #1: rel 1663/5/16385 blk 0
rmgr: Btree       len (rec/tot):     64/    64, tx:        735, lsn: 0/01567AB8, prev 0/01567A70, desc: INSERT_LEAF off: 2, blkref #0: rel 1663/5/16390 blk 1
rmgr: Transaction len (rec/tot):     46/    46, tx:        735, lsn: 0/01567AF8, prev 0/01567AB8, desc: COMMIT 2026-10-16 10:00:00.000001 UTC; subxacts: 736
error: error in WAL record at 0/1567AF8: invalid record length
"""


def facts(**over):
    base = {"wal_level": "replica", "version": 160004,
            "wal functions": "installed", "wal read granted": True,
            "wal kept": True, "wal dirs": [], "wal program": None,
            "no key": False,
            "digest way": "asked of one table, not a database"}
    base.update(over)
    return decide.Facts(base)


def test_the_programs_lines_read_as_records_with_their_blocks():
    recs = waldump_records(DUMP)
    assert [r["kind"] for r in recs] == ["INSERT", "UPDATE", "INSERT_LEAF",
                                         "COMMIT"]
    assert recs[0]["blocks"] == ((1663, 5, 16385, "main", 0),)
    assert recs[1]["blocks"] == ((1663, 5, 16385, "main", 7),
                                 (1663, 5, 16385, "main", 0))
    assert recs[1]["desc"].startswith("old_xmax: 735")
    assert recs[3]["xid"] == 735 and recs[3]["lsn"] == "0/01567AF8"


def test_a_committed_update_marks_its_old_block_as_having_lost_a_row():
    touched, open_at = wal_touched(waldump_records(DUMP), 5)
    assert open_at is None
    assert touched == {(1663, 16385): {0: "0/01567A70", 7: None}}


def test_an_aborted_transaction_changes_nothing_and_an_open_one_cuts():
    recs = [wal_record("0/10", 900, "Heap", "INSERT", "off: 1",
                       "blkref #0: rel 1663/5/16385 blk 3"),
            wal_record("0/20", 901, "Heap", "DELETE", "off: 2",
                       "blkref #0: rel 1663/5/16385 blk 4"),
            wal_record("0/30", 900, "Transaction", "ABORT", "", ""),
            wal_record("0/40", 902, "Heap", "INSERT", "off: 1",
                       "blkref #0: rel 1663/9/16385 blk 1"),
            wal_record("0/50", 902, "Transaction", "COMMIT", "", "")]
    touched, open_at = wal_touched(recs, 5)
    assert touched == {}
    assert open_at == "0/20"
    # another database's rows are not this one's
    touched, open_at = wal_touched(recs[2:], 5)
    assert touched == {} and open_at is None


def test_a_logical_server_keeps_its_slot_whatever_the_records_cost():
    got = PostgresEngine.follow_choice(
        facts(wal_level="logical"),
        decide.Costs({"wal inspect": 0.0001, "logical slot": 5.0}))
    assert got.path == "logical slot"


def test_wal_level_minimal_is_refused_by_every_way():
    got = PostgresEngine.follow_choice(
        facts(wal_level="minimal", **{"wal program": "/bin/x",
                                      "wal dirs": ["/w"]}))
    assert got.rung is None
    why = {r.name: w for r, w in got.passed}
    assert all("minimal" in why[r] for r in
               ("logical slot", "wal inspect", "wal dump"))
    assert "has a key" in why["digest diff"]


def test_without_the_functions_the_files_are_read_on_this_machine():
    got = PostgresEngine.follow_choice(facts(**{
        "wal functions": "the source's server has no WAL reading functions",
        "wal program": "/usr/bin/reader", "wal dirs": ["/data/pg_wal"]}))
    assert got.path == "wal dump"
    assert any(r.name == "wal inspect" and "no WAL reading functions" in w
               for r, w in got.passed)


def test_a_server_before_15_reads_its_files_or_nothing():
    got = PostgresEngine.follow_choice(facts(version=140011))
    assert got.rung is None
    assert any("version 140011" in w for _, w in got.passed)


def test_wal_not_kept_is_refused_unless_an_archive_holds_it():
    gone = facts(**{"wal kept": "nothing keeps the source's WAL",
                    "wal program": "/r", "wal dirs": ["/w"]})
    assert PostgresEngine.follow_choice(gone).rung is None
    archived = facts(**{"wal kept": "nothing keeps the source's WAL",
                        "wal program": "/r", "wal dirs": ["/w", "/arch"]})
    assert PostgresEngine.follow_choice(archived).path == "wal dump"


def engine(rows=None, keys=None, scope=lambda db, t: True):
    e = PostgresEngine.__new__(PostgresEngine)
    import tempfile
    from pathlib import Path
    kept = Path(tempfile.mkdtemp())
    e.hop = SimpleNamespace(name="h", options={},
                            report_dir=lambda db="": kept)
    e.in_scope = scope
    e.neutral_key = lambda side, db, t: list((keys or {}).get(t, ["id"]))
    e._split = lambda t: tuple(t.split("."))
    e._wal_ask = rows or (lambda side, db, sql, args=None: [])
    return e


def test_a_refused_grant_is_the_blocker_and_names_it(monkeypatch):
    e = engine()
    monkeypatch.setattr(e, "_follow_facts", lambda *a, **k: facts(
        **{"wal read granted": False}), raising=False)
    monkeypatch.setattr(e, "_follow_costs", lambda: decide.Costs(),
                        raising=False)
    with pytest.raises(ec.MissingGrant) as got:
        e.follow_way("app")
    assert got.value.grant == "pg_read_server_files"


def test_minimal_stops_the_follow_with_the_statement(monkeypatch):
    e = engine()
    monkeypatch.setattr(e, "_follow_facts", lambda *a, **k: facts(
        wal_level="minimal"), raising=False)
    monkeypatch.setattr(e, "_follow_costs", lambda: decide.Costs(),
                        raising=False)
    with pytest.raises(SystemExit) as got:
        e.follow_way("app")
    assert "wal_level = 'replica'" in str(got.value)


class Server:
    """The catalogue and rows a fake source answers the follow with."""

    def __init__(self, blocks, filenodes=None):
        self.blocks = blocks          # {block: [row]}
        self.filenodes = filenodes if filenodes is not None else {
            16385: ("public.items", "r")}

    def __call__(self, side, db, sql, args=None):
        if "pg_current_wal_flush_lsn" in sql:
            return [("0/90",)]
        if "pg_filenode_relation" in sql:
            got = self.filenodes.get(args[1])
            return [got] if got else []
        if "relkind = 'r'" in sql:
            return [("public.items",)]
        if "dattablespace" in sql:
            return [(5, 1663)]
        if "to_jsonb(array[" in sql:
            return [(b, json.dumps([r["id"]])) for b, rs in
                    self.blocks.items() for r in rs]
        if "to_jsonb(t)" in sql:
            wanted = {int(a.strip("()").split(",")[0]) for a in args[::2]}
            return [(b, r) for b, rs in self.blocks.items() if b in wanted
                    for r in rs]
        raise AssertionError(sql)


WAY = SimpleNamespace(path="wal inspect")


def follow(e, records, monkeypatch, token="0/10"):
    monkeypatch.setattr(e, "_wal_read", lambda *a: records, raising=False)
    monkeypatch.setattr(decide, "HopCosts", lambda hop, prefix="":
                        SimpleNamespace(saw=lambda *a: None))
    return e._wal_changes("src", "app", token, 1000, WAY)


def test_changed_blocks_are_read_again_and_told_apart_by_key(monkeypatch):
    server = Server({0: [{"id": 1, "v": "a"}, {"id": 2, "v": "b"}]})
    e = engine(server)
    assert follow(e, [], monkeypatch, token=None) == ([], "0/90")
    # 2 is deleted from block 0, 1 updated, 3 inserted into block 0
    server.blocks = {0: [{"id": 1, "v": "a2"}, {"id": 3, "v": "c"}]}
    recs = [wal_record("0/20", 7, "Heap", "DELETE", "off: 2",
                       "blkref #0: rel 1663/5/16385 blk 0"),
            wal_record("0/30", 7, "Heap", "INSERT", "off: 3",
                       "blkref #0: rel 1663/5/16385 blk 0"),
            wal_record("0/40", 7, "Transaction", "COMMIT", "", "")]
    out, upto = follow(e, recs, monkeypatch)
    assert upto == "0/90"
    got = {(c["op"], c["key"]["id"]) for c in out}
    assert got == {("update", 1), ("insert", 3), ("delete", 2)}
    assert next(c for c in out if c["key"]["id"] == 1)["values"]["v"] == "a2"


def test_an_open_transaction_holds_the_position_at_its_first_record(
        monkeypatch):
    e = engine(Server({0: [{"id": 1}]}))
    follow(e, [], monkeypatch, token=None)
    recs = [wal_record("0/20", 8, "Heap", "INSERT", "off: 2",
                       "blkref #0: rel 1663/5/16385 blk 0")]
    assert follow(e, recs, monkeypatch) == ([], "0/20")


def test_a_relation_file_no_table_holds_is_refused(monkeypatch):
    e = engine(Server({0: [{"id": 1}]}, filenodes={}))
    follow(e, [], monkeypatch, token=None)
    recs = [wal_record("0/20", 7, "Heap", "DELETE", "off: 1",
                       "blkref #0: rel 1663/5/16385 blk 0"),
            wal_record("0/30", 7, "Transaction", "COMMIT", "", "")]
    with pytest.raises(SystemExit) as got:
        follow(e, recs, monkeypatch)
    assert "relation file 16385" in str(got.value)


def test_a_table_without_a_key_is_refused_where_no_digest_way_holds(
        monkeypatch):
    e = engine(Server({0: [{"id": 1}]}), keys={"public.items": []})
    monkeypatch.setattr(e, "_follow_facts", lambda *a, **k: facts(),
                        raising=False)
    monkeypatch.setattr(e, "_wal_digest_way", lambda db, t: "ddl stops",
                        raising=False)
    follow(e, [], monkeypatch, token=None)
    recs = [wal_record("0/20", 7, "Heap", "INSERT", "off: 2",
                       "blkref #0: rel 1663/5/16385 blk 0"),
            wal_record("0/30", 7, "Transaction", "COMMIT", "", "")]
    with pytest.raises(SystemExit) as got:
        follow(e, recs, monkeypatch)
    assert "public.items has no primary key" in str(got.value)


def test_a_row_removed_before_the_keys_were_held_is_refused(monkeypatch):
    """A follow resumed by a new process holds its keys as of now; a row
    a record before that took out cannot be named, so it stops."""
    e = engine(Server({0: [{"id": 1}]}))
    recs = [wal_record("0/20", 7, "Heap", "DELETE", "off: 2",
                       "blkref #0: rel 1663/5/16385 blk 0"),
            wal_record("0/30", 7, "Transaction", "COMMIT", "", "")]
    with pytest.raises(SystemExit) as got:
        follow(e, recs, monkeypatch, token="0/10")
    assert "before 0/90" in str(got.value)


def test_the_depth_says_what_each_way_below_a_slot_reads():
    assert changedepth.postgres("minimal", "d", True)[0] == "D0"
    assert changedepth.postgres("replica", "d", True)[0] == "D0"
    depth, fix = changedepth.postgres("replica", "d", True, "wal inspect")
    assert depth == "D2" and "logical" in fix
    assert changedepth.postgres("replica", "d", False, "wal dump") == (
        "D1", "give the table a primary key")
    assert changedepth.postgres("logical", "f", True, "wal dump")[0] == "D4"


def test_a_keyless_table_is_climbed_once_and_the_record_names_the_way(
        monkeypatch):
    e = engine(Server({0: [{"id": 1}]}), keys={"public.items": []})
    asked = {"facts": 0, "digest": 0}
    held = facts()

    def follow_facts(*a, **k):
        asked["facts"] += 1
        return held

    def digest_way(db, table):
        asked["digest"] += 1
        return True

    monkeypatch.setattr(e, "_follow_facts", follow_facts, raising=False)
    monkeypatch.setattr(e, "_wal_digest_way", digest_way, raising=False)
    follow(e, [], monkeypatch, token=None)
    recs = [wal_record("0/20", 7, "Heap", "INSERT", "off: 2",
                       "blkref #0: rel 1663/5/16385 blk 0"),
            wal_record("0/30", 7, "Transaction", "COMMIT", "", "")]
    for _ in range(2):
        out, _upto = follow(e, recs, monkeypatch)
        assert [c["op"] for c in out] == ["ddl"]
    assert asked == {"facts": 1, "digest": 1}
    said = [r for r in decide.recorded(e.hop) if "public.items" in r["work"]]
    assert len(said) == 1
    assert said[0]["way"] == "the table copied again and held to its digest"
    assert e._wal_keyless("src", "app", "public.items", WAY).path \
        == "digest diff"
    assert asked == {"facts": 1, "digest": 1}
