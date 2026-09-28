"""A tail after a ranged copy, on PostgreSQL to PostgreSQL, leaves out the
changes the copy already read (backlog R19, lever 8) and the target ends
equal to the source.

200,000 rows are copied in eight ranges, two at a time, while a writer
updates, deletes and inserts random rows; the tail then starts from the
position taken before the copy, as `--mode full+cdc` starts it, and runs
until it has read what the writer wrote. The same run again with every
change applied (`MIGKIT_TAIL_APPLY_ALL=1`) is what leaving them out is
measured against.
"""
import ctypes
import json
import random
import threading
import time

import pytest

from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

pytestmark = needs_docker

ROWS = 200_000
DB = "postgres"
SLOT = "migkit_leave_out"


def _engine(pg_pair, where):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name="leave_out", engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=[DB], workers=4)
    where.mkdir(parents=True, exist_ok=True)
    hop.report_dir = lambda db=None: where
    return PostgresEngine(hop)._as_pair()


def _fresh(pg_pair):
    for port in (pg_pair["src"], pg_pair["dst"]):
        got = psql(port, "drop table if exists public.big;"
                         " create table public.big (id bigint primary key,"
                         " n int, payload text)")
        assert got.returncode == 0, got.stderr
    psql(pg_pair["src"], "select pg_drop_replication_slot(slot_name) from"
                         f" pg_replication_slots where slot_name = '{SLOT}'")
    got = psql(pg_pair["src"], f"""
        insert into public.big select g, 0, md5(g::text)
          from generate_series(1, {ROWS}) g;
        analyze public.big""")
    assert got.returncode == 0, got.stderr


def _writer(port, stop, done):
    """Random rows updated, deleted and inserted, one transaction each,
    until told to stop."""
    import psycopg2
    conn = psycopg2.connect(host="127.0.0.1", port=port, user="postgres",
                            password="test", dbname=DB)
    conn.autocommit = True
    rnd, fresh = random.Random(8), ROWS
    with conn.cursor() as cur:
        while not stop.is_set():
            r = rnd.random()
            if r < 0.8:
                cur.execute("update public.big set n = n + 1, payload ="
                            " md5(random()::text) where id = %s",
                            (rnd.randint(1, ROWS),))
            elif r < 0.9:
                cur.execute("delete from public.big where id = %s",
                            (rnd.randint(1, ROWS),))
            else:
                fresh += 1
                cur.execute("insert into public.big values (%s, 0, 'new')",
                            (fresh,))
            done[0] += 1
    conn.close()


def _tail_until(pair, token_path, end):
    """The tail in a thread until its saved position reaches `end`, then
    stopped the way a person stops it; (its lines, seconds to get
    there)."""
    from migkit.engines.postgres import PostgresEngine
    lines, finished = [], threading.Event()

    def run():
        try:
            pair.tail_apply(DB, True, token_path, lines.append)
        except BaseException as e:  # noqa: BLE001 - KeyboardInterrupt
            lines.append(f"exit: {type(e).__name__}: {e}")
        finally:
            finished.set()
    thread = threading.Thread(target=run, daemon=True)
    began = time.monotonic()
    thread.start()
    while not finished.is_set():
        try:
            at = json.loads(token_path.read_text()).get("token")
        except (OSError, ValueError):
            at = None
        if at and PostgresEngine.position_reached(at, end):
            break
        time.sleep(0.1)
    took = time.monotonic() - began
    ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_ulong(thread.ident), ctypes.py_object(KeyboardInterrupt))
    finished.wait(timeout=120)
    return lines, took


def _same(pg_pair):
    q = ("select count(*) || ' ' || md5(coalesce(string_agg(id || ':' || n"
         " || ':' || payload, ',' order by id), '')) from public.big")
    a, b = psql(pg_pair["src"], q), psql(pg_pair["dst"], q)
    assert a.returncode == 0 and b.returncode == 0, (a.stderr, b.stderr)
    return a.stdout.strip(), b.stdout.strip()


def _run(pg_pair, where, monkeypatch, apply_all):
    from migkit import ranges
    from migkit.cli import _Checkpoint
    if apply_all:
        monkeypatch.setenv("MIGKIT_TAIL_APPLY_ALL", "1")
    else:
        monkeypatch.delenv("MIGKIT_TAIL_APPLY_ALL", raising=False)
    # eight ranges, two read at once: each read begins later than the one
    # before, as a large table's ranges do beside the move's other tables
    monkeypatch.setattr(ranges, "LEAST", ROWS // 8)
    monkeypatch.setattr(ranges, "active", ranges.Slots(2))
    _fresh(pg_pair)
    pair = _engine(pg_pair, where)
    token_path = where / "tail-token.json"
    # the position, before the copy
    assert pair.tail_start(DB, token_path)
    stop, done = threading.Event(), [0]
    writer = threading.Thread(target=_writer,
                              args=(pg_pair["src"], stop, done), daemon=True)
    writer.start()
    ck, said = _Checkpoint(where / "move.json"), []
    began = time.monotonic()
    try:
        pair.move_table(DB, "public", "big", 500_000, ck, said.append)
    finally:
        copied = time.monotonic() - began
        stop.set()
        writer.join(timeout=60)
    st = ck[pair.move_key(DB, "", "big")]
    assert len(st["ranges"]) >= 8 and st["done"], st
    # every range read under a mark of its own
    assert len({str(m) for m in st["ranges_seen"].values()}) >= 4, st
    end = psql(pg_pair["src"], "select pg_current_wal_lsn()").stdout.strip()
    # what the target's applier spends, apart from reading the log
    spent = [0.0]
    real = pair.dst_engine.neutral_apply

    def timed(*a, **k):
        began = time.monotonic()
        try:
            return real(*a, **k)
        finally:
            spent[0] += time.monotonic() - began
    pair.dst_engine.neutral_apply = timed
    lines, took = _tail_until(pair, token_path, end)
    psql(pg_pair["src"], f"select pg_drop_replication_slot('{SLOT}')")
    src, dst = _same(pg_pair)
    assert src == dst, (src, dst, lines[-5:])
    # every change read accounted for, as the tail kept it
    counted = json.loads((where / "tail-accounts.json").read_text())
    got = counted["public.big"]
    left = got.get("already in what the copy read", 0)
    assert got["read"] == got["applied"] + left, got
    return {"writes": done[0], "copy": copied, "tail": took,
            "apply": spent[0], "applied": got["applied"], "left": left,
            "read": got["read"], "lines": lines}


def test_the_tail_leaves_out_what_the_copy_read_and_the_target_is_level(
        pg_pair, tmp_path, monkeypatch):
    kept = _run(pg_pair, tmp_path / "left-out", monkeypatch, False)
    every = _run(pg_pair, tmp_path / "all", monkeypatch, True)
    assert kept["left"] > 0, kept["lines"][-5:]
    assert every["left"] == 0, every["lines"][-5:]
    for name, r in (("left out", kept), ("all applied", every)):
        print(f"\n{name}: {r['writes']:,} writes during a {r['copy']:.1f}s"
              f" copy; the tail read {r['read']:,} changes, applied"
              f" {r['applied']:,}, left out {r['left']:,}"
              f" ({r['left'] / max(r['read'], 1):.0%}); caught up in"
              f" {r['tail']:.1f}s, {r['apply']:.2f}s of it applying")
