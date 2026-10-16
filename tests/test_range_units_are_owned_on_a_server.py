"""Every range a PostgreSQL to PostgreSQL move copies is a unit of the
attempt ledger, owned on the server (backlog P0-ETA-ALL,
P0-CONNECTION-PROGRESS): opened before its statement, its target session
recorded, closed only once its checkpoint holds it, its remaining time said
from the ranges run - and a range whose session the server drops is told
stalled and replayed alone, the target holding every row once; a move
stopped mid-range and run again copies no range twice and loses none. The
range is held mid-load by a row lock on the target, never by a sleep."""
import glob
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from migkit.durable import read_state
from tests.conftest import _pg_container, needs_docker, psql


ROOT = Path(__file__).resolve().parent.parent
# a copy by ranges of its key, each range owned, replayed alone where its
# session is lost and gone on from where a stopped move left it
PROVES = [("move.table-copy", "postgres")]
Q = ("select count(*) || ':' || md5(string_agg(id || '|' || payload, ','"
     " order by id)) from public.t")
DDL = "create table public.t (id bigint primary key, payload text)"


def _hop(tmp, pg):
    (tmp / "hops.yaml").write_text(
        "hops:\n  owned:\n    engine: postgres\n"
        f"    source: {{host: 127.0.0.1, port: {pg['src']},"
        " user: postgres, password: test}\n"
        f"    target: {{host: 127.0.0.1, port: {pg['dst']},"
        " user: postgres, password: test}\n"
        "    databases: [postgres]\n"
        "    options:\n      target_rows: replace\n")


def _move(tmp):
    env = dict(os.environ, MIGKIT_CONF=str(tmp / "hops.yaml"),
               MIGKIT_REPORTS=str(tmp / "reports"), MIGKIT_MOVER="builtin",
               COLUMNS="200")
    env.pop("MIGKIT_FAILPOINT", None)
    return subprocess.Popen(
        [sys.executable, "-c", "from migkit.cli import main; main()",
         "move", "owned", "--go", "--chunk", "10000"], env=env, cwd=ROOT,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)


def _setup(pg, tmp):
    psql(pg["src"], "drop table if exists public.t")
    got = psql(pg["src"], f"{DDL}; insert into public.t select g, 'row-' || g"
                          " from generate_series(1, 30000) g; analyze")
    assert got.returncode == 0, got.stderr
    # the target's table is empty: a key of the second range written and
    # not committed there holds that range's load - and only that one -
    # on the key's unique index, mid-statement
    psql(pg["dst"], "drop table if exists public.t")
    assert psql(pg["dst"], DDL).returncode == 0
    _hop(tmp, pg)


def _hold(pg):
    """A session on the target holding key 15000 until it is ended."""
    return subprocess.Popen(
        ["docker", "exec", "-i", "-e", "PGPASSWORD=test", "-e",
         "PGAPPNAME=range-holder", _pg_container(pg["dst"]), "psql", "-U",
         "postgres", "-At"],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL, text=True)


def _release(pg, holder):
    """The hold ended on the server: the client killed here leaves its
    session asleep in the container, holding the key."""
    holder.kill()
    holder.wait()
    psql(pg["dst"], "select pg_terminate_backend(pid) from pg_stat_activity"
                    " where application_name = 'range-holder'")


def _lock(holder):
    holder.stdin.write("begin; insert into public.t values (15000,"
                       " 'held'); select pg_sleep(600);\n")
    holder.stdin.flush()


def _ranges(tmp):
    """{after: record} of public.t's range units in the move's ledger."""
    out = {}
    for path in glob.glob(str(tmp / "reports" / "**" / "attempts*.json"),
                          recursive=True):
        if path.endswith(".bak"):
            continue
        for name, rec in ((read_state(path, "state") or {}).get("units")
                          or {}).items():
            if rec.get("parent") == "public.t" and " r:" in name:
                out[int(name.rsplit(":", 1)[1])] = rec
    return out


def _second_held(tmp, move, timeout=120):
    """The second range's record once it has a target session recorded."""
    end = time.time() + timeout
    got = {}
    while time.time() < end and move.poll() is None:
        got = _ranges(tmp)
        if len(got) >= 2:
            rec = got[sorted(got)[1]]
            if rec.get("sessions") and rec.get("state") == "open":
                return sorted(got)[1], rec
        time.sleep(0.2)
    move.kill()
    out, _ = move.communicate(timeout=60)
    files = sorted(glob.glob(str(tmp / "reports" / "**" / "*"),
                             recursive=True))
    raise AssertionError(f"the second range never held a session: {got}"
                         f" {files} {out[-3000:]}")


def _same(pg):
    return psql(pg["src"], Q).stdout.strip(), psql(pg["dst"], Q).stdout.strip()


@needs_docker
def test_a_dropped_range_session_is_told_stalled_and_replayed_alone(
        pg_pair, tmp_path):
    _setup(pg_pair, tmp_path)
    holder = _hold(pg_pair)
    _lock(holder)
    time.sleep(1)
    move = _move(tmp_path)
    try:
        after, rec = _second_held(tmp_path, move)
        side, pid = rec["sessions"][-1]
        assert side == "dst"
        # the server drops exactly the recorded session of range 2
        got = psql(pg_pair["dst"], f"select pg_terminate_backend({pid})")
        assert got.stdout.strip() == "t", got.stderr
    finally:
        _release(pg_pair, holder)
    out, _ = move.communicate(timeout=600)
    assert move.returncode == 0, out[-3000:]
    units = _ranges(tmp_path)
    assert len(units) == 3, units
    assert all(r["state"] == "done" for r in units.values()), units
    assert all(r.get("statement", "").startswith("load ")
               for r in units.values())
    # each closed after its checkpoint held it, with a session of its own
    assert all(r["sessions"] for r in units.values())
    two = units[after]
    assert two.get("stalls") == 1 and "no longer lists" in two["stalled"]
    assert [pid] == [s for _, s in two["sessions"]][:1]
    assert len(two["sessions"]) >= 2, two
    others = [r for a, r in units.items() if a != after]
    assert all(not r.get("stalls") and len(r["sessions"]) == 1
               for r in others), others
    assert "stalled on the target's session" in out
    assert _same(pg_pair)[0] == _same(pg_pair)[1]
    assert _same(pg_pair)[0].startswith("30000:")
    # the time left said after the first range, from the ranges run
    line = next(s for s in out.splitlines() if "1 of 3 ranges done" in s)
    left = float(line.split("about ")[1].split("s left")[0])
    lo, hi = (float(x.rstrip("s")) for x in
              line.split("left (")[1].split(")")[0].split(" to "))
    first = min((r for r in units.values()), key=lambda r: r["ended"])
    t1 = first["ended"] - first["started"]
    assert lo <= left <= hi
    assert t1 / 2 * 0.9 - 0.05 <= left <= 2 * t1 * 1.1 + 0.05, (left, t1)


@needs_docker
def test_a_move_stopped_mid_range_copies_no_range_twice(pg_pair, tmp_path):
    _setup(pg_pair, tmp_path)
    holder = _hold(pg_pair)
    _lock(holder)
    time.sleep(1)
    move = _move(tmp_path)
    try:
        after, _ = _second_held(tmp_path, move)
        move.send_signal(signal.SIGTERM)
        move.communicate(timeout=120)
        assert move.returncode != 0
    finally:
        _release(pg_pair, holder)
    cut = _ranges(tmp_path)
    assert cut[after]["state"] in ("open", "failed"), cut[after]
    done_before = {a for a, r in cut.items() if r["state"] == "done"}
    again = _move(tmp_path)
    out, _ = again.communicate(timeout=600)
    assert again.returncode == 0, out[-3000:]
    units = _ranges(tmp_path)
    assert len(units) == 3 and all(r["state"] == "done"
                                   for r in units.values()), units
    # the cut-off range was reconciled and opened once more; one done
    # before the stop was not copied again
    assert units[after]["opens"] == 2, units[after]
    assert all(units[a]["opens"] == 1 for a in done_before)
    assert all(units[a]["started"] == cut[a]["started"] for a in done_before)
    assert _same(pg_pair)[0] == _same(pg_pair)[1]
    assert _same(pg_pair)[0].startswith("30000:")


def test_the_deep_check_keeps_each_range_it_proves(tmp_path, monkeypatch):
    """The row text way of PostgreSQL's deep check hands the check its
    kept ranges, the table and the engine: each range a unit, a stopped
    check going on from the ranges it owes."""
    from migkit import scanplan
    from migkit.config import Endpoint, Hop
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setenv("MIGKIT_REPORTS", str(tmp_path))
    ep = Endpoint(host="10.0.0.1", port=5432, user="u", password="CHANGE_ME")
    eng = PostgresEngine(Hop(name="h", engine="postgres", source=ep,
                             target=ep, databases=["d"]))
    seen = {}

    def caught(read, preds, **kw):
        seen.update(kw)
        raise LookupError("seen")
    monkeypatch.setattr(scanplan, "plan_most", lambda *a, **k: 1)
    monkeypatch.setattr(scanplan, "row_text", caught)
    try:
        eng._row_text("d", "public.t", None)
    except LookupError:
        pass
    assert isinstance(seen["kept"], scanplan.RangeTexts)
    assert seen["kept"] is eng.row_texts("d")
    assert seen["kept"].path == eng.hop.report_dir("d") / "row-texts.json"
    assert seen["table"] == "d.public.t" and seen["eng"] is eng
    assert seen["scope"] == eng.row_text_scope(None, [""])


def test_the_last_ranges_proof_lands_before_an_unproved_range_is_owed():
    """A range proved beside the next one's load - the last one's proof,
    or one cut short by a stop, still running once every range returned -
    is closed done once `settle` lets it land, never told owed while its
    checkpoint holds it; a range `settle` does not prove is owed, and a
    failure of `settle` is left to its caller."""
    import contextlib
    from types import SimpleNamespace

    from migkit import attempts, ranges
    mon = attempts.Monitor(None, attempts.Ledger(), thread=False,
                           say=lambda line: None)
    eng = SimpleNamespace(attempts=lambda: mon,
                          attempt_unit=lambda *a: contextlib.nullcontext())
    led = mon.ledger
    st = {"ranges": [[0, 10], [10, 20], [20, 30]], "ranges_done": []}
    save, pending = (lambda: None), []

    def one(rng):
        # each range proved later, beside the next load
        pending.append(rng[0])
        return 10

    def settle():
        for after in pending[:2]:
            ranges.finished(st, after, save)
        raise RuntimeError("the third range's proof failed")
    ranges.each_owned(ranges.Slots(1), eng, "public.t", st,
                      [(0, 10), (10, 20), (20, 30)], one, settle=settle)
    got = {a: led.unit_record(ranges.unit_of("public.t", a))
           for a in (0, 10, 20)}
    assert [got[a]["state"] for a in (0, 10)] == ["done", "done"]
    assert got[20]["state"] == "failed"
    assert got[20]["reason"] == "not proved"
    assert st["ranges_done"] == [0, 10]
