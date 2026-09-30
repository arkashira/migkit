"""The proof kept up as the data moves (backlog R20.8): after any workload
the tail carries, every leaf the tail kept equals the leaf read fresh from
that side - source and target - wherever the log gave whole images, and a
leaf it could not keep is marked to read again rather than kept wrong.

Held here with Hypothesis on an in-memory pair whose source marks are
transaction counts: writes before the leaves are seeded and after, a tail
lagging behind the source at the seed (so it applies changes the source's
seeded leaves already hold), images without a before, a batch kept and
its position never written, and a row written into the target behind the
tail's back - which only the cold read may find, and does. The servers'
side of it is `test_the_proof_is_kept_on_servers.py`.
"""

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from migkit import kept, leaves
from migkit.engines.hetero import HeteroEngine
from tests.test_one_scan_leaves import Fake

COLS = [("id", "integer"), ("name", "text"), ("v", "integer")]


class Source(Fake):
    """The source: rows, and a log of every committed change with its
    transaction number; a mark is how many had committed."""

    def __init__(self, rows):
        super().__init__(rows)
        self.log, self.txn = [], 0

    def neutral_tables(self, side, db):
        return ["t"]

    def neutral_key(self, side, db, table):
        return ["id"]

    def table_facts(self, side, db):
        return {"t": {"rows": len(self.rows)}}

    def snapshot_mark(self, side, db):
        return self.txn

    def mark_covers(self, side, db, mark, change):
        return change["txn"] <= mark

    def write(self, op, rid, name=None, v=None, whole=True):
        self.txn += 1
        at = {r[0]: i for i, r in enumerate(self.rows)}
        before = list(self.rows[at[rid]]) if rid in at else None
        if op == "delete":
            if before is None:
                self.txn -= 1
                return
            del self.rows[at[rid]]
            after = None
        elif before is None:
            after = [rid, name, v]
            self.rows.append(after)
            op = "insert"
        else:
            after = [rid, name, v]
            self.rows[at[rid]] = after
            op = "update"
        c = {"op": op, "table": "t", "key": {"id": rid},
             "values": dict(zip(("id", "name", "v"), after)) if after
             else {}, "txn": self.txn}
        if before is not None and whole:
            c["before"] = dict(zip(("id", "name", "v"), before))
        self.log.append(c)


class Target(Fake):
    def neutral_tables(self, side, db):
        return ["t"]

    def neutral_key(self, side, db, table):
        return ["id"]

    def apply(self, changes):
        for c in changes:
            rid = c["key"]["id"]
            self.rows = [r for r in self.rows if r[0] != rid]
            if c["op"] != "delete":
                v = c["values"]
                self.rows.append([v["id"], v["name"], v["v"]])


class Hop:
    options = {"keep_proof": True}

    def __init__(self, where):
        self.where = where

    def report_dir(self, db=None):
        return self.where


class Pair:
    """What the Keeper asks of a pair."""

    def __init__(self, src, dst, where):
        self.src_engine, self.dst_engine = src, dst
        self.hop = Hop(where)

    match_tables = staticmethod(HeteroEngine.match_tables)
    _leaf = staticmethod(HeteroEngine._leaf)

    def _rename(self, t):
        return t

    def _comparable_columns(self, db, s, st_, d, dt):
        return list(COLS), list(COLS), []

    def _row_scope(self, db, t):
        return None, None

    def digest_salt(self):
        return "5a1f"


def trees(pair, keeper):
    t = keeper.tables()["t"]
    a, b = t.sides(pair, "db")
    return leaves.scan(a, t.spec), leaves.scan(b, t.spec)


def check_equal(pair, keeper):
    """Every leaf not marked equals the fresh read of its side; returns
    how many leaves were marked."""
    ta, tb = trees(pair, keeper)
    got = keeper.store.leaves("t")
    z = (0, 0, 0)
    marked = 0
    for b in set(got) | set(ta.leaves) | set(tb.leaves):
        leaf = got.get(b)
        if leaf is not None and leaf[kept.STALE]:
            marked += 1
            continue
        s = tuple(leaf[0:3]) if leaf else z
        d = tuple(leaf[3:6]) if leaf else z
        assert s == ta.leaves.get(b, z), ("source", b)
        assert d == tb.leaves.get(b, z), ("target", b)
    return marked


ops = st.lists(st.tuples(st.sampled_from(["put", "put", "delete"]),
                         st.integers(1, 60), st.text(max_size=3),
                         st.integers(-5, 5), st.booleans()),
               min_size=1, max_size=120)


@pytest.mark.parametrize("layout", ["hash", "range"])
@settings(max_examples=150, deadline=None,
          suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(before=ops, lagging=st.integers(0, 30), after=ops,
       cuts=st.lists(st.integers(1, 25), min_size=1, max_size=8),
       holes=st.booleans())
def test_kept_leaves_equal_a_fresh_scan_after_any_workload(
        tmp_path_factory, monkeypatch, layout, before, lagging, after, cuts,
        holes):
    # leaves by ranges of the key (a server's index) or by its hash; the
    # rows here are read into this process either way
    monkeypatch.setattr(kept, "layout", lambda a, b: layout)
    monkeypatch.setattr(leaves, "key_range", lambda a, b: (1, 40))
    where = tmp_path_factory.mktemp("kept")
    rows = [[i, f"r{i}", i % 3] for i in range(1, 41)]
    src, dst = Source([list(r) for r in rows]), Target([list(r) for r in rows])
    pair = Pair(src, dst, where)
    lines = []
    for op, rid, name, v, whole in before:
        src.write("delete" if op == "delete" else "upsert", rid, name, v,
                  whole or not holes)
    # the tail has applied all but the last `lagging` changes when the
    # leaves are seeded: the source's are read with those in them
    lag = min(lagging, len(src.log))
    done = len(src.log) - lag
    dst.apply(src.log[:done])
    keeper = kept.Keeper(pair, "db", where / "tail-token.json", done,
                         lines.append)
    keeper.read([])
    keeper.saved([], done, None)
    assert keeper.tables(), lines
    for op, rid, name, v, whole in after:
        src.write("delete" if op == "delete" else "upsert", rid, name, v,
                  whole or not holes)
    # the tail catches up in batches of the given sizes
    pos, i = done, 0
    while pos < len(src.log):
        n = cuts[i % len(cuts)]
        i += 1
        batch = src.log[pos:pos + n]
        keeper.read(batch)
        dst.apply(batch)
        keeper.saved(batch, pos + len(batch), pos)
        pos += len(batch)
    marked = check_equal(pair, keeper)
    if not holes:
        assert marked == 0
    # every image whole: the kept numbers of the two sides are equal too,
    # leaf for leaf, with nothing read
    if not holes:
        for leaf in keeper.store.leaves("t").values():
            assert leaf[0:3] == leaf[3:6]


def test_a_batch_kept_whose_position_was_never_written_is_taken_out(
        tmp_path):
    rows = [[i, f"r{i}", 0] for i in range(1, 31)]
    src, dst = Source([list(r) for r in rows]), Target([list(r) for r in rows])
    pair = Pair(src, dst, tmp_path)
    keeper = kept.Keeper(pair, "db", tmp_path / "tail-token.json", 0,
                         lambda m: None)
    keeper.saved([], 0, None)
    for rid in (3, 7, 30, 31):
        src.write("upsert", rid, "new", 9)
    batch = list(src.log)
    keeper.read(batch)
    dst.apply(batch)
    keeper.saved(batch, 4, 0)
    # the tail stopped before writing position 4: it starts again at 0,
    # and the batch is read and applied again
    lines = []
    again = kept.Keeper(pair, "db", tmp_path / "tail-token.json", 0,
                        lines.append)
    assert any("taken back out" in l for l in lines), lines
    again.read(batch)
    dst.apply(batch)
    again.saved(batch, 4, 0)
    assert check_equal(pair, again) == 0
    # a position the kept leaves never stood at: every leaf marked
    lines = []
    lost = kept.Keeper(pair, "db", tmp_path / "tail-token.json", 99,
                       lines.append)
    assert any("another position" in l for l in lines), lines
    assert all(l[kept.STALE] for l in lost.store.leaves("t").values())


def test_a_row_written_behind_the_tails_back_is_found_by_the_cold_read(
        tmp_path):
    rows = [[i, f"r{i}", 0] for i in range(1, 2001)]
    src, dst = Source([list(r) for r in rows]), Target([list(r) for r in rows])
    pair = Pair(src, dst, tmp_path)
    keeper = kept.Keeper(pair, "db", tmp_path / "tail-token.json", 0,
                         lambda m: None)
    keeper.saved([], 0, None)
    # the target changed by something that is not the tail: the stream
    # never says so, and the kept numbers still agree
    dst.rows[1500][1] = "tampered"
    got = keeper.store.leaves("t")
    assert all(l[0:3] == l[3:6] for l in got.values())
    # the cold pass reads every leaf (a small table): the target's read
    # differs from what was kept, and the two kept numbers now differ
    seen = keeper.cold(most=10 ** 6)
    assert len(seen["t"]) == 1, seen
    b = seen["t"][0]
    leaf = keeper.store.leaves("t", [b])[b]
    assert leaf[0:3] != leaf[3:6]
    # each side's kept numbers are that side as it is now
    assert check_equal(pair, keeper) == 0


class Tailed(Pair):
    """The pair as the proof sees a running tail: its position read from
    the file the tail writes, the fence already passed."""

    def _tail_path(self, db):
        return self.hop.where / "tail-token.json"

    @staticmethod
    def _saved_token(path):
        import json
        return json.loads(path.read_text())["token"]

    def fence_wait(self, db, at, timeout=300):
        return True

    def _drill_clause(self, db, name, missing, changed, extra, capped,
                      how=""):
        return (f"; missing {sorted(missing)} changed {sorted(changed)}"
                f" extra {sorted(extra)}" + how)


def test_the_proof_reads_only_what_the_last_window_touched(tmp_path,
                                                           monkeypatch):
    import json

    from migkit import rowtext, tailctl
    monkeypatch.setattr(tailctl, "alive", lambda where: True)
    monkeypatch.setattr(tailctl, "pause", lambda where, timeout: True)
    monkeypatch.setattr(tailctl, "resume", lambda where: None)
    rows = [[i, f"r{i}", 0] for i in range(1, 5001)]
    src, dst = Source([list(r) for r in rows]), Target([list(r) for r in rows])
    src.log_position = lambda side, db: None
    pair = Tailed(src, dst, tmp_path)
    token = tmp_path / "tail-token.json"
    token.write_text(json.dumps({"token": 0}))
    keeper = kept.Keeper(pair, "db", token, 0, lambda m: None)
    keeper.saved([], 0, None)
    # no fence to take: no kept proof, the scans decide
    assert kept.prove(pair, "db") is None
    src.log_position = lambda side, db: 1
    for rid in (10, 2500, 4999):
        src.write("upsert", rid, "new", 1)
    batch = list(src.log)
    keeper.read(batch)
    dst.apply(batch)
    keeper.saved(batch, len(src.log), 0)
    token.write_text(json.dumps({"token": len(src.log)}))
    got = kept.prove(pair, "db")["t"]
    assert got[0] == "ok", got
    spec = keeper.tables()["t"].spec
    touched = {spec.bucket(leaves.text_hash(spec.salt, rowtext.encode(
        [str(rid)]))) for rid in (10, 2500, 4999)}
    assert f"read again: {len(touched)} touched since the last pass" in \
        got[1], got
    # nothing since: nothing read
    read = []
    real = kept.reread
    monkeypatch.setattr(kept, "reread",
                        lambda *a: read.append(a[4]) or real(*a))
    got = kept.prove(pair, "db")["t"]
    assert got[0] == "ok" and "none read again" in got[1] and not read, got
    # a row changed behind the tail's back: proved equal until the cold
    # read, then named
    next(r for r in dst.rows if r[0] == 101)[1] = "tampered"
    assert kept.prove(pair, "db")["t"][0] == "ok"
    keeper.cold(most=10 ** 6)
    got = kept.prove(pair, "db")["t"]
    assert got[0] == "diff" and "changed [('101',)]" in got[1], got
    assert "whose kept numbers differed" in got[1], got
