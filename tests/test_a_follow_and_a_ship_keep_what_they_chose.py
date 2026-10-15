"""What the WAL follow held and the row hash a pair chose are kept in the
hop's ledger, so a resumed follow and a later export go on from them."""

import json
from types import SimpleNamespace

import pytest

from migkit import canon, decide, leaves
from migkit.engines.hetero import HeteroEngine
from migkit.engines.mysql import MySQLEngine
from migkit.engines.postgres import PostgresEngine, wal_record


class Server:
    def __init__(self, blocks, end="0/10"):
        self.blocks, self.end = blocks, end

    def __call__(self, side, db, sql, args=None):
        if "pg_current_wal_flush_lsn" in sql:
            return [(self.end,)]
        if "pg_filenode_relation" in sql:
            return [("public.items", "r")]
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


def engine(server, kept, keys=("id",)):
    """A fresh process's engine: nothing held in memory, the hop's
    report directory `kept` shared."""
    e = PostgresEngine.__new__(PostgresEngine)
    e.hop = SimpleNamespace(name="h", options={},
                            report_dir=lambda db="": kept)
    e.in_scope = lambda db, t: True
    e.neutral_key = lambda side, db, t: list(keys)
    e._split = lambda t: tuple(t.split("."))
    e._wal_ask = server
    return e


WAY = SimpleNamespace(path="wal inspect")
DELETE_2 = [wal_record("0/20", 7, "Heap", "DELETE", "off: 2",
                       "blkref #0: rel 1663/5/16385 blk 0"),
            wal_record("0/30", 7, "Transaction", "COMMIT", "", "")]


def follow(e, records, monkeypatch, token):
    monkeypatch.setattr(e, "_wal_read", lambda *a: records, raising=False)
    monkeypatch.setattr(decide, "HopCosts", lambda hop, prefix="":
                        SimpleNamespace(saw=lambda *a: None))
    return e._wal_changes("src", "app", token, 1000, WAY)


def test_a_resumed_follow_names_a_row_removed_before_it_restarted(
        monkeypatch, tmp_path):
    server = Server({0: [{"id": 1}, {"id": 2}]}, end="0/10")
    assert follow(engine(server, tmp_path), [], monkeypatch, None) == (
        [], "0/10")
    # the process stops; 2 is removed; a new one resumes from 0/10
    server.blocks, server.end = {0: [{"id": 1}]}, "0/90"
    out, upto = follow(engine(server, tmp_path), DELETE_2, monkeypatch,
                       "0/10")
    assert upto == "0/90"
    assert ("delete", 2) in {(c["op"], c["key"]["id"]) for c in out}


def test_without_the_ledger_the_same_resume_is_refused(monkeypatch,
                                                         tmp_path):
    server = Server({0: [{"id": 1}]}, end="0/90")
    with pytest.raises(SystemExit) as got:
        follow(engine(server, tmp_path), DELETE_2, monkeypatch, "0/10")
    assert "before 0/90" in str(got.value)


def test_a_resume_at_the_older_position_reads_the_keys_as_of_it(
        monkeypatch, tmp_path):
    """A batch read but its position never saved: the resume at the old
    position holds the keys as they were there, not after the batch."""
    server = Server({0: [{"id": 1}, {"id": 2}]}, end="0/10")
    e = engine(server, tmp_path)
    follow(e, [], monkeypatch, None)
    server.blocks, server.end = {0: [{"id": 1}]}, "0/90"
    follow(e, DELETE_2, monkeypatch, "0/10")
    at_old = engine(server, tmp_path)._wal_kept("src", "app", "0/10")
    at_new = engine(server, tmp_path)._wal_kept("src", "app", "0/90")
    key = lambda n: json.dumps([n], sort_keys=True)  # noqa: E731
    assert at_old["keys"]["public.items"][0] == {key(1), key(2)}
    assert at_new["keys"]["public.items"][0] == {key(1)}
    assert engine(server, tmp_path)._wal_kept("src", "app", "0/50") is None


def test_a_cut_last_line_is_read_as_never_written(monkeypatch, tmp_path):
    server = Server({0: [{"id": 1}, {"id": 2}]}, end="0/10")
    e = engine(server, tmp_path)
    follow(e, [], monkeypatch, None)
    lines = e._wal_keys_path("src", "app").with_suffix(".jsonl")
    lines.write_text('{"from": "0/10", "position": "0/9')
    assert e._wal_kept("src", "app", "0/10") is not None
    assert e._wal_kept("src", "app", "0/90") is None


def test_a_table_without_a_key_is_copied_again_by_its_digest(
        monkeypatch, tmp_path):
    server = Server({0: [{"id": 1}]}, end="0/10")
    e = engine(server, tmp_path, keys=())
    monkeypatch.setattr(e, "_follow_facts", lambda *a, **k: decide.Facts(
        {"wal_level": "replica", "version": 160004,
         "wal functions": "installed", "wal read granted": True,
         "wal kept": True, "wal dirs": [], "wal program": None}),
        raising=False)
    monkeypatch.setattr(e, "_wal_digest_way", lambda db, t: True,
                        raising=False)
    follow(e, [], monkeypatch, None)
    server.end = "0/90"
    out, _ = follow(e, DELETE_2, monkeypatch, "0/10")
    assert out == [canon.ddl("public.items", "reload")]


def test_the_digest_way_holds_only_for_a_table_without_a_key():
    base = {"wal_level": "replica", "version": 160004,
            "wal functions": "installed", "wal read granted": True,
            "wal kept": True, "wal dirs": [], "wal program": None,
            "digest way": True}
    keyed = PostgresEngine.follow_choice(decide.Facts({**base,
                                                       "no key": False}))
    assert keyed.path == "wal inspect"
    keyless = PostgresEngine.follow_choice(decide.Facts({**base,
                                                         "no key": True}))
    assert keyless.path == "digest diff"
    stopped = PostgresEngine.follow_choice(decide.Facts(
        {**base, "no key": True, "digest way": "ddl stops"}))
    assert stopped.rung is None


def pair(tmp_path):
    p = HeteroEngine.__new__(HeteroEngine)
    p.hop = SimpleNamespace(name="h", report_dir=lambda db="": tmp_path)
    p._leaf = lambda t: str(t).split(".")[-1]
    return p


def test_the_chosen_hash_is_read_back_by_a_run_that_never_chose(
        monkeypatch, tmp_path):
    monkeypatch.setattr(leaves, "pair_hash", lambda *a: "sha256")
    assert pair(tmp_path).digest_way("app", "public.items", "items",
                                     [("id", "integer")],
                                     [("id", "integer")]) == "sha256"
    # an export with no columns, in a run that chose nothing
    assert pair(tmp_path).digest_way("app", "public.items") == "sha256"
    assert leaves.kept_way(pair(tmp_path).hop, "app", "items") == "sha256"
    assert pair(tmp_path).digest_way("app", "public.other") == "md5"


def test_a_measure_that_fails_falls_to_md5_and_is_not_kept(monkeypatch,
                                                            tmp_path):
    def broken(*a):
        raise ConnectionError("the probe's connection was refused")
    monkeypatch.setattr(leaves, "pair_hash", broken)
    assert pair(tmp_path).digest_way("app", "public.items", "items",
                                     [("id", "integer")],
                                     [("id", "integer")]) == "md5"
    assert leaves.kept_way(pair(tmp_path).hop, "app", "items") is None


def test_the_load_lines_digest_takes_the_chosen_hash():
    other = [w for w in canon.DIGEST_HASHES.get("mysql", {}) if w != "md5"]
    if not other:
        pytest.skip("the MySQL digest takes md5 alone here")
    my = MySQLEngine.__new__(MySQLEngine)
    my._d = lambda side, db: db
    cols = [("id", "integer")]
    sql = my._range_digest_sql("src", "app", "items", "", (cols, cols, []),
                               way=other[0])
    row = canon.row_expr("mysql", cols)
    assert canon.digest_expr("mysql", row, other[0]) in sql
    assert canon.digest_expr("mysql", row) not in sql
