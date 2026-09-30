"""A one-way tail numbers its batches on the target by default (backlog
F7, R19 lever 11; `marks.settle_exact`): the batch's number goes into a
row of `migkit_origin` in the batch's own transaction, so a tail stopped
after the target committed a batch and before its position was saved
goes on after that batch, not from before it - the batch is neither read
nor applied again. A batch applied in lanes side by side is more than
one transaction and is not numbered: it is read again after a stop, as
before. `exact_batches: false` leaves every batch unnumbered.
"""
import json
import threading
import time

from migkit import marks, tailctl
from migkit.config import Endpoint, Hop
from tests.conftest import needs_docker, psql

DB = "postgres"


class _Dst:
    CANON_ENGINE = "postgres"

    def __init__(self, can_create=True, keeps=True):
        self.can_create, self.keeps = can_create, keeps
        self.seen = None

    def mark_facts(self, side, db):
        return {"family": "postgres", "version": 160000,
                "origin_grants": False, "foreign_origins": [],
                "can_create": self.can_create}

    def _apply_session(self, side, db):
        import contextlib

        @contextlib.contextmanager
        def one():
            yield
            if self.keeps:
                self.seen = json.loads(self.__dict__["_batch_seen"])
        return one()

    def origin_seen(self, side, db):
        return self.seen


class _Pair:
    def __init__(self, dst, options=None):
        ep = Endpoint(host="x", port=1, user="x", password="x")
        self.hop = Hop(name="ex", engine="postgres", source=ep, target=ep,
                       options=dict(options or {}))
        self.dst_engine = dst


def test_the_table_rung_is_proved_by_what_it_keeps(tmp_path):
    said = []
    path = tmp_path / "tail-token.json"
    path.write_text(json.dumps({"token": "0/1"}))
    dst = _Dst()
    assert marks.settle_exact(_Pair(dst), DB, path, said.append) == "table"
    assert dst._numbered and dst._mark_rung == "table"
    assert "_batch_seen" not in dst.__dict__
    # the probe's number is below every batch's
    assert dst.seen["batch"] < 0
    assert json.loads(path.read_text())["rung"] == "table"
    # kept: the next start stands on it without a probe
    again = _Dst(keeps=False)
    assert marks.settle_exact(_Pair(again), DB, path, said.append) == "table"


def test_a_target_that_keeps_no_number_leaves_batches_unnumbered(tmp_path):
    for dst, why in ((_Dst(can_create=False), "cannot make the table"),
                     (_Dst(keeps=False), "did not read back")):
        said = []
        path = tmp_path / f"{why[:6]}.json"
        path.write_text(json.dumps({"token": "0/1"}))
        assert marks.settle_exact(_Pair(dst), DB, path, said.append) is None
        assert not dst.__dict__.get("_numbered")
        assert any(why in s for s in said), said
    path = tmp_path / "off.json"
    path.write_text(json.dumps({"token": "0/1"}))
    assert marks.settle_exact(_Pair(_Dst(), {"exact_batches": False}), DB,
                              path, print) is None


def _pair(pg_pair, where, name):
    from migkit.engines.postgres import PostgresEngine
    hop = Hop(name=name, engine="postgres",
              source=Endpoint(host="127.0.0.1", port=pg_pair["src"],
                              user="postgres", password="test"),
              target=Endpoint(host="127.0.0.1", port=pg_pair["dst"],
                              user="postgres", password="test"),
              databases=[DB], workers=1)
    where.mkdir(parents=True, exist_ok=True)
    hop.report_dir = lambda db=None: where
    return PostgresEngine(hop)._as_pair()


def _run(pair, path, lines, seconds):
    done = threading.Event()

    def run():
        try:
            pair.tail_apply(DB, True, path, lines.append)
        except BaseException as e:  # noqa: BLE001 - said in the lines
            lines.append(f"exit: {type(e).__name__}: {e}")
        finally:
            done.set()
    threading.Thread(target=run, daemon=True).start()
    done.wait(seconds)
    if not done.is_set():
        tailctl.stop(path.parent)
        done.wait(60)


@needs_docker
def test_a_batch_the_target_committed_is_not_applied_again(pg_pair,
                                                           tmp_path,
                                                           monkeypatch):
    from migkit import failpoint
    for port in (pg_pair["src"], pg_pair["dst"]):
        psql(port, "drop table if exists public.nb, public.migkit_origin;"
                   " create table public.nb (id int primary key, n int)")
    where = tmp_path / "r"
    pair = _pair(pg_pair, where, "numbered")
    path = where / "tail-token.json"
    try:
        lines = []
        _run(pair, path, lines, 5)
        assert any("batches are numbered on the target" in s
                   for s in lines), lines
        psql(pg_pair["src"], "insert into public.nb select g, 0 from"
                             " generate_series(1, 50) g")
        # stopped after the target committed the batch, before its
        # position was saved
        monkeypatch.setattr(failpoint, "_counts", {})
        monkeypatch.setenv("MIGKIT_FAILPOINT", "tail.applied:1:raise")
        lines = []
        _run(pair, path, lines, 20)
        assert any("failpoint tail.applied" in s for s in lines), lines
        assert psql(pg_pair["dst"], "select count(*) from public.nb"
                    ).stdout.strip() == "50"
        monkeypatch.delenv("MIGKIT_FAILPOINT")
        before = json.loads((where / "tail-accounts.json").read_text()) \
            if (where / "tail-accounts.json").exists() else {}
        lines = []
        _run(pair, path, lines, 8)
        said = " | ".join(lines)
        assert "had committed batch" in said, said
        after = json.loads((where / "tail-accounts.json").read_text())
        # the batch was not read again: nothing more of it accounted
        read = (after.get("public.nb") or {}).get("read", 0) \
            - (before.get("public.nb") or {}).get("read", 0)
        assert read == 0, (before, after)
    finally:
        psql(pg_pair["src"], "select pg_drop_replication_slot("
                             f"'{pair.src_engine.slot_name()}')")
        time.sleep(0)
