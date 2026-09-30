"""A seed shipped on media (the owner's scope, 2026-10-01): the export
beside the source writes every table in parts to a carrier - digested as
the check digests, compressed, encrypted to the hop's recipients, hashed
as stored - with a signed manifest and the seal last; the load beside the
target checks each part's hash, holds its rows to its digest, writes it
through the target's own loader and proves it there, then each table as a
whole. Each half goes on from where it stopped, a part that fails is read
again from the source over the link, and nothing that is not the part
exported is ever loaded.

Every test here runs on files: SQLite on both sides, a directory as the
carrier, an age key and an SSH signing key made for the test.
"""
import json
import math
import sqlite3
import subprocess
from decimal import Decimal

import pytest

from migkit import canon, carriers, evidence, ship
from migkit.config import Endpoint, Hop

ORDERS, TAGS, LOG = 300, 120, 250


def _files(tmp_path):
    src, dst = tmp_path / "a.db", tmp_path / "b.db"
    with sqlite3.connect(src) as con:
        con.executescript(
            "create table orders (id integer primary key, item text,"
            " qty int, price real, note blob);"
            " create table tags (code text primary key, label text);"
            " create table log (at text, what text);")
        con.executemany("insert into orders values (?, ?, ?, ?, ?)",
                        [(i, f"item-{i}", i % 7, i / 3, bytes([i % 256]))
                         for i in range(1, ORDERS + 1)])
        con.executemany("insert into tags values (?, ?)",
                        [(f"t{i:04d}", f"label {i} é") for i in range(TAGS)])
        # a table with no key, one of its rows held twice
        con.executemany("insert into log values (?, ?)",
                        [(f"2026-10-01 00:{i // 60 % 60:02d}:{i % 60:02d}",
                          f"event {i}") for i in range(LOG - 1)]
                        + [("2026-10-01 00:00:00", "event 0")])
    sqlite3.connect(dst).close()
    return src, dst


def _keys(tmp_path):
    import pyrage
    who = pyrage.x25519.Identity.generate()
    (tmp_path / "me.age").write_text(str(who) + "\n")
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f",
                    str(tmp_path / "signer"), "-C", "ops@example.com"],
                   check=True)
    pub = (tmp_path / "signer.pub").read_text().strip()
    (tmp_path / "allowed").write_text(f"ops@example.com {pub}\n")
    return str(who.to_public())


def _hop(tmp_path, src, dst, **shipped):
    reports = tmp_path / "reports"

    def report_dir(db=""):
        d = reports / db if db else reports
        d.mkdir(parents=True, exist_ok=True)
        return d
    opts = {"at_rest": {"recipients": [_keys(tmp_path)]},
            "evidence": {"sign": str(tmp_path / "signer")},
            "ship": {"carrier": "disk", "path": str(tmp_path / "carrier"),
                     "shipping": "2d", "signers": str(tmp_path / "allowed"),
                     **shipped}}
    hop = Hop(name="lite", engine="sqlite",
              source=Endpoint(host=str(src), port=0, user="", password=""),
              target=Endpoint(host=str(dst), port=0, user="", password=""),
              databases=["main"], options=opts)
    hop.report_dir = report_dir
    return hop


@pytest.fixture
def seed(tmp_path, monkeypatch):
    monkeypatch.setenv("MIGKIT_IDENTITY", str(tmp_path / "me.age"))
    # parts of 100 rows: every table is more than one part
    monkeypatch.setattr(ship, "PART_ROWS", 100)
    monkeypatch.setattr(ship, "ROOM_SAMPLE", 0)
    # one part at a time, so what a stop leaves is the same every run
    monkeypatch.setattr(ship._Work, "workers", lambda self, units: 1)
    src, dst = _files(tmp_path)
    return tmp_path, src, dst, _hop(tmp_path, src, dst)


def _eng(hop):
    from migkit.engines.sqlite import SQLiteEngine
    return SQLiteEngine(hop)


def _carry(hop, said=None):
    said = said if said is not None else []
    got = ship.carry(hop, _eng(hop), "main", "full", True, said.append,
                     via="builtin")
    return got, said


def _rows(path, table):
    with sqlite3.connect(path) as con:
        order = "code" if table == "tags" else "id" if table == "orders" \
            else "at, what"
        return con.execute(f"select * from {table} order by {order}"
                           ).fetchall()


def _record(tmp_path):
    return [json.loads(x) for x in (tmp_path / "reports" / "changelog.jsonl"
                                    ).read_text().splitlines()]


def _manifest(tmp_path, hop):
    store = carriers.LocalStore(tmp_path / "carrier")
    seal = ship.seal_at(store, hop, "main")
    w = ship._Work(hop, _eng(hop), "main", "full", lambda m: None)
    return ship.read_manifest(w, store, seal), store, seal


def test_the_export_seals_the_carrier_and_the_load_proves_every_part(seed):
    tmp_path, src, dst, hop = seed
    got, said = _carry(hop)
    assert got["main"].half == "export", said
    carrier = tmp_path / "carrier" / "lite" / "main"
    assert (carrier / ship.SEALED).exists()
    # nothing on the carrier reads as a value: every part and the manifest
    # are sealed to the recipients
    parts = sorted((carrier / "parts").rglob("*.part"))
    assert len(parts) == 3 + 2 + math.ceil(LOG / 100), parts
    for f in parts + [carrier / ship.MANIFEST]:
        raw = f.read_bytes()
        assert raw.startswith(evidence.AGE), f
        assert b"item-1" not in raw and b"label" not in raw, f
    assert (carrier / ship.SIGNATURE).exists()
    # nothing is on the target yet: the rows travel on the carrier
    assert not sqlite3.connect(dst).execute(
        "select count(*) from sqlite_master where type = 'table'"
    ).fetchone()[0]

    got, said = _carry(hop)
    assert got["main"].half == "load", said
    for table in ("orders", "tags", "log"):
        assert _rows(dst, table) == _rows(src, table), table
    loaded = [e for e in _record(tmp_path)
              if e.get("op") == "seed-part-loaded"]
    exported = [e for e in _record(tmp_path)
                if e.get("op") == "seed-part-exported"]
    assert sorted((e["table"], e["part"]) for e in loaded) == \
        sorted((e["table"], e["part"]) for e in exported)
    # each part proved the way its table's cut allows
    proved = {e["table"]: {x["proved"] for x in loaded
                           if x["table"] == e["table"]} for e in loaded}
    assert proved == {"orders": {"the target's digest of the range"},
                      "tags": {"every row read back by its key"},
                      "log": {"the table's digest once every part is in"}}
    # and each table as a whole, in the check's own digests
    from migkit import digests
    kept = digests.read_in(tmp_path / "reports")
    for table in ("orders", "tags", "log"):
        line = kept[("main", table, "")]
        assert line["src"]["rows"] == line["dst"]["rows"] > 0, line
        assert line["src"]["digest"] == line["dst"]["digest"], line
    # the manifest's signature was checked against the allowed signers
    assert any("signed by ops@example.com" in m for m in said), said


def test_each_table_is_cut_the_way_its_key_allows(seed):
    tmp_path, src, dst, hop = seed
    _carry(hop)
    manifest, _, _ = _manifest(tmp_path, hop)
    tables = {e["table"]: e for e in manifest["tables"]}
    assert tables["orders"]["cut"] == "ranges"
    assert tables["tags"]["cut"] == "key order"
    assert tables["log"]["cut"] == "one pass"
    assert len(tables["orders"]["parts"]) == 3
    assert len(tables["tags"]["parts"]) == 2
    assert len(tables["log"]["parts"]) == math.ceil(LOG / 100)
    assert {e["format"] for e in tables.values()} == {"rows"}


def test_a_parts_digest_is_the_checks_own_over_its_rows(seed):
    """Taken again here from the source's rows of each range, by the fold
    every engine's digest is held to: the manifest's number is that one,
    and the parts add up to the table's."""
    tmp_path, src, dst, hop = seed
    _carry(hop)
    manifest, _, _ = _manifest(tmp_path, hop)
    orders = next(e for e in manifest["tables"] if e["table"] == "orders")
    names = [n for n, _ in orders["digest_columns"]]
    classes = [c for _, c in orders["digest_columns"]]
    total = 0
    with sqlite3.connect(src) as con:
        for p in orders["parts"].values():
            rows = con.execute(
                f"select {', '.join(names)} from orders where id > ? and"
                " id <= ?", (p["after"], p["upto"])).fetchall()
            n, d = canon.fold_rows(classes, rows)
            assert (n, str(d)) == (p["rows"], p["digest"]), p
            total += d
        whole = con.execute(f"select {', '.join(names)} from orders"
                            ).fetchall()
    assert canon.fold_rows(classes, whole) == (ORDERS, total)


def test_a_stopped_export_goes_on_from_the_next_part(seed, monkeypatch):
    """Stopped at the second range of a table: the tables finished before
    it and its first range are not written again."""
    tmp_path, src, dst, hop = seed
    put = carriers.LocalStore.put
    names = []
    second = f"lite/main/parts/{ship._tid('orders')}/000001.part"

    def failing(self, name, path):
        if name == second and second not in names:
            names.append(second + " (refused)")
            raise RuntimeError("the carrier went away")
        put(self, name, path)
        names.append(name)
    monkeypatch.setattr(carriers.LocalStore, "put", failing)
    with pytest.raises(RuntimeError, match="went away"):
        _carry(hop)
    assert not (tmp_path / "carrier" / "lite" / "main" / ship.SEALED
                ).exists()
    first = [n for n in names if n.endswith(".part")]
    assert f"lite/main/parts/{ship._tid('orders')}/000000.part" in first
    monkeypatch.setattr(carriers.LocalStore, "put",
                        lambda self, name, path: (put(self, name, path),
                                                  names.append(name)))
    got, said = _carry(hop)
    assert got["main"].said.startswith("sealed"), said
    # no part the first run put was put again, and the one refused was
    parts = [n for n in names if n.endswith(".part")]
    assert len(parts) == len(set(parts)), parts
    assert set(first) <= set(parts) and second in parts
    got, said = _carry(hop)
    for table in ("orders", "tags", "log"):
        assert _rows(dst, table) == _rows(src, table), table


def test_a_stopped_load_goes_on_from_the_next_part(seed, monkeypatch):
    tmp_path, src, dst, hop = seed
    _carry(hop)
    done = []
    part = ship._Loader.part

    def stopping(self, tgt, p, store=None, relinked=False):
        if len(done) == 2:
            raise RuntimeError("the load was stopped")
        got = part(self, tgt, p, store, relinked)
        done.append((tgt.entry["table"], p["n"]))
        return got
    monkeypatch.setattr(ship._Loader, "part", stopping)
    with pytest.raises(RuntimeError, match="stopped"):
        _carry(hop)
    first = list(done)
    monkeypatch.setattr(ship._Loader, "part",
                        lambda self, tgt, p, store=None, relinked=False: (
                            done.append((tgt.entry["table"], p["n"])),
                            part(self, tgt, p, store, relinked))[1])
    _carry(hop)
    ranged = [d for d in done if d[0] == "orders"]
    # a range loaded before the stop is not loaded again
    assert len(ranged) == len(set(ranged)), done
    assert set(first) <= set(done)
    for table in ("orders", "tags", "log"):
        assert _rows(dst, table) == _rows(src, table), table


@pytest.mark.parametrize("table", ["orders", "tags", "log"])
def test_a_part_broken_on_the_way_is_read_again_over_the_link(seed, table):
    tmp_path, src, dst, hop = seed
    _carry(hop)
    manifest, store, _ = _manifest(tmp_path, hop)
    entry = next(e for e in manifest["tables"] if e["table"] == table)
    part = entry["parts"]["1"]
    path = store.local(part["name"])
    raw = bytearray(path.read_bytes())
    raw[len(raw) // 2] ^= 0xFF
    path.write_bytes(bytes(raw))
    got, said = _carry(hop)
    assert any("read again from the source over the link" in m
               for m in said), said
    relinked = [e for e in _record(tmp_path)
                if e.get("op") == "seed-part-relinked"]
    assert [(e["table"], e["part"]) for e in relinked] == [(table, 1)]
    for t in ("orders", "tags", "log"):
        assert _rows(dst, t) == _rows(src, t), t


def test_a_part_whose_rows_are_not_its_digest_is_never_loaded_as_it_is(
        seed):
    """The manifest re-sealed and re-signed with one part's digest changed:
    the part's own bytes are whole, and its rows fold to another number -
    it is not taken, it is read again."""
    tmp_path, src, dst, hop = seed
    _carry(hop)
    manifest, store, seal = _manifest(tmp_path, hop)
    entry = next(e for e in manifest["tables"] if e["table"] == "orders")
    entry["parts"]["0"]["digest"] = str(int(entry["parts"]["0"]["digest"])
                                        + 1)
    sealed = evidence._seal(json.dumps(manifest).encode(),
                            hop.options["at_rest"]["recipients"])
    local = store.local(f"lite/main/{ship.MANIFEST}")
    local.write_bytes(sealed)
    from migkit import export
    (tmp_path / "carrier" / "lite" / "main" / ship.SIGNATURE).unlink()
    export._sign(local, str(tmp_path / "signer"), ship.NAMESPACE)
    import hashlib
    seal["manifest_sha256"] = hashlib.sha256(sealed).hexdigest()
    store.put_bytes(f"lite/main/{ship.SEALED}", json.dumps(seal).encode())
    got, said = _carry(hop)
    assert any("orders: part 0" in m and "digesting" in m for m in said), \
        said
    assert _rows(dst, "orders") == _rows(src, "orders")


def test_a_target_that_changes_what_it_is_given_stops_the_load(
        seed, monkeypatch):
    from migkit.engines.sqlite import SQLiteEngine
    tmp_path, src, dst, hop = seed
    _carry(hop)
    write = SQLiteEngine.neutral_write

    def changing(self, side, db, table, columns, rows):
        names = [n for n, _ in columns]
        if "qty" in names:
            i = names.index("qty")
            rows = [list(r[:i]) + [(r[i] or 0) + 1] + list(r[i + 1:])
                    for r in rows]
        return write(self, side, db, table, columns, rows)
    monkeypatch.setattr(SQLiteEngine, "neutral_write", changing)
    with pytest.raises(SystemExit, match="read back different"):
        _carry(hop)


def test_a_manifest_not_signed_by_a_signer_is_refused(seed):
    tmp_path, src, dst, hop = seed
    _carry(hop)
    subprocess.run(["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f",
                    str(tmp_path / "eve"), "-C", "eve@example.com"],
                   check=True)
    from migkit import export
    carrier = tmp_path / "carrier" / "lite" / "main"
    (carrier / ship.SIGNATURE).unlink()
    export._sign(carrier / ship.MANIFEST, str(tmp_path / "eve"),
                 ship.NAMESPACE)
    with pytest.raises(SystemExit, match="signature does not hold"):
        _carry(hop)
    # and none at all, where the hop names who must sign it
    (carrier / ship.SIGNATURE).unlink()
    with pytest.raises(SystemExit, match="carries no signature"):
        _carry(hop)
    assert not sqlite3.connect(tmp_path / "b.db").execute(
        "select count(*) from sqlite_master where type = 'table'"
    ).fetchone()[0]


def test_a_manifest_the_seal_does_not_name_is_refused(seed):
    tmp_path, src, dst, hop = seed
    _carry(hop)
    store = carriers.LocalStore(tmp_path / "carrier")
    seal = ship.seal_at(store, hop, "main")
    seal["manifest_sha256"] = "0" * 64
    store.put_bytes(f"lite/main/{ship.SEALED}", json.dumps(seal).encode())
    with pytest.raises(SystemExit, match="not the one the seal names"):
        _carry(hop)


def test_nothing_is_exported_without_recipients(seed):
    tmp_path, src, dst, hop = seed
    hop.options.pop("at_rest")
    with pytest.raises(SystemExit, match="at_rest recipients"):
        _carry(hop)
    assert not list((tmp_path / "carrier").rglob("*.part"))


def test_a_lost_position_is_taken_again_and_only_what_changed_goes(
        seed, monkeypatch):
    """The source kept its log for less than the carrier took: the far end
    takes a new position first, then copies the rows that changed since the
    seed, found leaf by leaf - the seed is not shipped again, and the
    stream starts from the new position."""
    tmp_path, src, dst, hop = seed
    taken, calls = iter(["P1", "P2"]), []
    monkeypatch.setattr(ship, "_take_position", lambda w, lost=False: (
        calls.append("position after the loss" if lost else "position"),
        next(taken))[1])
    got = ship.carry(hop, _eng(hop), "main", "full+cdc", True,
                     lambda m: None, via="builtin")
    assert got["main"].half == "export"
    # what the source does while the carrier travels
    with sqlite3.connect(src) as con:
        con.execute("update orders set item = 'changed' where id = 5")
        con.execute("insert into orders values (301, 'new', 1, 1.0, x'01')")
        con.execute("delete from tags where code = 't0003'")
    monkeypatch.setattr(ship, "lost", lambda s, db, token: (
        "the source no longer keeps its log back to the position"
        if token == "P1" else None))
    delta = ship._delta_by_the_leaves
    monkeypatch.setattr(ship, "_delta_by_the_leaves",
                        lambda w: (calls.append("delta"), delta(w))[1])
    said = []
    got = ship.carry(hop, _eng(hop), "main", "full+cdc", True, said.append,
                     via="builtin")
    assert got["main"].half == "load" and got["main"].position == "P2", said
    assert calls == ["position", "position after the loss", "delta"], calls
    for table in ("orders", "tags", "log"):
        assert _rows(dst, table) == _rows(src, table), table
    assert any(e.get("op") == "seed-delta" for e in _record(tmp_path))
    # the seed was loaded once, from the carrier, and not read again
    assert not any(e.get("op") == "seed-part-relinked"
                   for e in _record(tmp_path))


def test_a_value_is_carried_exactly_and_nothing_else_is_built():
    import datetime
    import os
    import pickle
    import zoneinfo
    tz = zoneinfo.ZoneInfo("Asia/Bangkok")
    row = [Decimal("1.10"), -0.0, float("nan"), "é\udc80",
           memoryview(b"\x00\x01"), datetime.datetime(2026, 10, 1, 7,
                                                        tzinfo=tz),
           canon.ABSENT, {1, 2}, 2 ** 70]
    back = ship._loads(ship._dumps([row]))[0]
    assert str(back[0]) == "1.10"
    assert math.copysign(1, back[1]) == -1 and math.isnan(back[2])
    assert back[3] == "é\udc80" and back[4] == b"\x00\x01"
    assert back[5] == row[5] and back[5].tzinfo.key == "Asia/Bangkok"
    assert back[6] is canon.ABSENT and back[7] == {1, 2}
    assert back[8] == 2 ** 70

    class Planted:
        def __reduce__(self):
            return os.system, ("echo planted",)
    with pytest.raises(pickle.UnpicklingError, match="system"):
        ship._loads(pickle.dumps([Planted()]))
    with pytest.raises(ship.Unfit, match="Planted"):
        ship._dumps([Planted()])


def test_the_whole_move_through_the_command_is_two_runs(tmp_path,
                                                        monkeypatch):
    """`migkit move --mode full --go` beside the source exports and says to
    ship the carrier; the same command beside the target loads."""
    from click.testing import CliRunner

    import migkit.config as cfg
    from migkit import cli
    monkeypatch.setenv("MIGKIT_IDENTITY", str(tmp_path / "me.age"))
    monkeypatch.setattr(ship, "ROOM_SAMPLE", 0)
    src, dst = _files(tmp_path)
    recipient = _keys(tmp_path)
    conf = tmp_path / "hops.yaml"
    conf.write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {src}, user: x, password: x}}\n"
        f"    target: {{host: {dst}, user: x, password: x}}\n"
        "    databases: [main]\n"
        "    options:\n"
        f"      at_rest: {{recipients: ['{recipient}']}}\n"
        f"      ship: {{carrier: disk, path: {tmp_path / 'carrier'},"
        " shipping: 2d}\n")
    monkeypatch.setattr(cfg, "CONF", str(conf))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")

    def run(*argv):
        got = CliRunner().invoke(cli.main, list(argv))
        return got, " ".join((got.output + str(got.exception or "")
                              ).split())
    got, said = run("move", "lite", "--mode", "full", "--go")
    assert got.exit_code == 0, said
    assert "ship the carrier" in said, said
    got, said = run("move", "lite", "--mode", "full", "--go")
    assert got.exit_code == 0, said
    assert "loaded and proved" in said, said
    for table in ("orders", "tags", "log"):
        assert _rows(dst, table) == _rows(src, table), table
    got, said = run("check", "lite")
    assert got.exit_code == 0, said
