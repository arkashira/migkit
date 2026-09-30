"""The cutover's settings, its words, and the order its undo runs in,
without a server: what the hop can say, what the dry run and the
approvers are shown, and which step is put back before which.

The order is the part that loses rows if it is wrong. A stream whose
new slot starts "now" has to be back before the source takes a write
again, or the writes in between are carried by nothing; a stream that
starts from the target's own position has to wait for the source to be
writable, because setting it up writes there. Both are held here against
a path that fails at every step.
"""
import json

import pytest

from migkit import cutover
from migkit.config import Endpoint, Hop


def _hop(tmp_path, **options):
    options.setdefault("state", {"mirror": str(tmp_path / "mirror")})
    hop = Hop(name="cx", engine="postgres",
              source=Endpoint(host="10.0.0.1", port=5432, user="migkit",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5432, user="migkit",
                              password="CHANGE_ME"),
              databases=["app"], options=options)
    root = tmp_path / "reports"

    def report_dir(db=""):
        d = root / db if db else root
        d.mkdir(parents=True, exist_ok=True)
        return d
    hop.report_dir = report_dir
    return hop


def test_a_hop_without_it_is_off_and_one_with_it_gets_the_defaults(
        tmp_path):
    assert cutover.settings(_hop(tmp_path)) is None
    assert cutover.settings(_hop(tmp_path, cutover=False)) is None
    got = cutover.settings(_hop(tmp_path, cutover=True))
    assert got["budget"] == 30 and got["freeze"] == "auto" \
        and got["jobs"] == "move" and cutover.margin(got) == 0
    got = cutover.settings(_hop(tmp_path, cutover={"budget": 5,
                                                   "keys": "apart:1000"}))
    assert got["budget"] == 5.0 and cutover.margin(got) == 1000


@pytest.mark.parametrize("bad, said", [
    ({"budgets": 5}, "budgets - not a setting"),
    ({"budget": 0}, "more than 0"),
    ({"budget": "soon"}, "are seconds"),
    ({"freeze": "maybe"}, "freeze maybe - one of"),
    ({"keys": "apart:x"}, "keys apart:x"),
    ({"keys": "apart:0"}, "keys apart:0"),
    ({"jobs": "both"}, "jobs is move or leave"),
    ({"pooler": {"kind": "proxysql"}}, "pooler is"),
    ("yes please", "is `true` or"),
])
def test_what_the_hop_cannot_mean_is_refused_before_anything_runs(
        tmp_path, bad, said):
    with pytest.raises(SystemExit, match=said):
        cutover.settings(_hop(tmp_path, cutover=bad))


def test_the_steps_are_said_by_hand_without_it_and_by_side_with_it(
        tmp_path):
    hand = "\n".join(cutover.plan(_hop(tmp_path, reverse="at_cutover"),
                                  ["app"], None))
    for step in ("switch off on the source the jobs",
                 "stop the application's writes to the source",
                 "--mode cdc --drop --go", "--kind sequences",
                 "point the application at the target",
                 "and starts the stream back"):
        assert step in hand, step
    conf = cutover.settings(_hop(tmp_path, cutover={
        "budget": 12, "keys": "apart:500", "flip": "./switch.sh",
        "pooler": {"kind": "pgbouncer", "databases": ["app"]}}))
    said = "\n".join(cutover.plan(_hop(tmp_path, reverse="at_cutover"),
                                  ["app"], conf))
    for step in ("within 12s of write outage", "pooler: PAUSE app",
                 "source: frozen", "taken after the freeze",
                 "500 apart", "the stream back to the old source started",
                 "then `./switch.sh`", "the pooler repointed and resumed",
                 "the old source stays frozen"):
        assert step in said, step


def test_a_signature_is_for_the_plan_it_was_shown(tmp_path, monkeypatch):
    """The approvers sign the cutover's plan; a plan changed since - a
    bigger budget, another freeze - asks again."""
    from migkit import approvals
    hop = _hop(tmp_path, approvals={"steps": ["cutover"],
                                    "signers": str(tmp_path / "signers")})
    (tmp_path / "signers").write_text("")
    monkeypatch.setattr(approvals, "_requester", lambda: "me")
    with pytest.raises(SystemExit, match="needs 1 approval"):
        approvals.require(hop, "cutover", "", detail={"budget": 10})
    path = hop.report_dir("") / "approvals" / "cutover-all.request.json"
    first = json.loads(path.read_text())
    assert first["detail"] == {"budget": 10}
    with pytest.raises(SystemExit):
        approvals.require(hop, "cutover", "", detail={"budget": 10})
    assert json.loads(path.read_text())["nonce"] == first["nonce"]
    with pytest.raises(SystemExit):
        approvals.require(hop, "cutover", "", detail={"budget": 60})
    again = json.loads(path.read_text())
    assert again["nonce"] != first["nonce"] and again["detail"] == {
        "budget": 60}


def test_doctor_names_a_cutover_left_half_way_and_a_source_kept_frozen(
        tmp_path):
    hop = _hop(tmp_path, cutover=True)
    assert cutover.said_by_doctor(hop) == []
    (hop.report_dir() / "cutover.json").write_text(json.dumps(
        {"state": "running", "phases": {"freeze": {"began": 1}}}))
    said = cutover.said_by_doctor(hop)
    assert any("stopped at freeze" in s and "--drop --go" in s
               for s in said), said
    (hop.report_dir("app") / "freeze-src.json").write_text(json.dumps(
        {"db": "app", "roles": {}}))
    assert any("kept frozen" in s for s in cutover.said_by_doctor(hop))


def test_a_refusal_for_being_read_only_is_told_from_any_other():
    import pymysql

    from migkit import cli
    assert cli._read_only(pymysql.err.OperationalError(
        1290, "The MySQL server is running with the --super-read-only"
              " option so it cannot execute this statement"))
    assert cli._read_only(RuntimeError(
        "ERROR:  cannot execute DROP PUBLICATION in a read-only"
        " transaction"))
    assert not cli._read_only(pymysql.err.OperationalError(
        1045, "Access denied for user"))


class _Calls:
    def __init__(self):
        self.log = []

    def __call__(self, what):
        return lambda *a, **k: self.log.append(what)


def _fake(monkeypatch, tmp_path, after_thaw, fail_at):
    """A path whose every step is recorded and whose step `fail_at`
    raises, with nothing of a server behind it."""
    import migkit.config as cfg
    from migkit import freeze, jobs
    from migkit.engines.base import RepairAction, Result
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    calls = _Calls()
    hop = _hop(tmp_path, cutover={"budget": 60}, reverse="at_cutover",
               protect_target=True)

    class Eng:
        def maps_columns(self):
            return False

        def src_lsn(self, d):
            return "0/10"

        def fence_wait(self, d, pos, timeout=0):
            return True

        def check_data(self, d):
            return [Result("data", d, "ok", "")]

        def repair_plan(self, d, kind):
            return [RepairAction(d, "sequences", ["select 1"],
                                 ["select 0"], "1 set")]

        def apply(self, d, action):
            calls.log.append(f"counters {action.statements[0]}")

        def snapshot_state(self, d, where, kind):
            (where / "dst-sequences.txt").write_text("")

    eng = Eng()
    monkeypatch.setattr(freeze, "scope", lambda *a: ("server", ""))
    monkeypatch.setattr(freeze, "freeze_source", calls("freeze source"))
    monkeypatch.setattr(freeze, "proof", lambda *a: [(True, "held")])
    monkeypatch.setattr(freeze, "thaw_source", calls("thaw source"))
    monkeypatch.setattr(freeze, "freeze", calls("hold target"))
    monkeypatch.setattr(freeze, "thaw", calls("release target"))
    monkeypatch.setattr(jobs, "inventory", lambda *a: [])
    monkeypatch.setattr(jobs, "switch", lambda *a: 0)
    monkeypatch.setattr(jobs, "undo", calls("jobs back"))
    monkeypatch.setattr(cutover, "_has_delta", lambda *a: False)
    leg = cutover.Leg("native", fence=eng, down=calls("stream down"),
                      up=calls("stream up"), back_up=calls("back up"),
                      back_down=calls("back down"), back_fence=eng,
                      after_thaw=after_thaw)
    real = cutover.failpoint.hit

    def hit(name):
        if name == f"cutover.{fail_at}":
            raise RuntimeError(f"broken at {fail_at}")
        return real(name)
    monkeypatch.setattr(cutover.failpoint, "hit", hit)
    return hop, eng, leg, calls


@pytest.mark.parametrize("after_thaw", [False, True])
@pytest.mark.parametrize("fail_at", ["freeze", "drain", "verify",
                                     "counters", "stream", "reverse",
                                     "jobs_on", "flip"])
def test_every_step_is_put_back_the_last_first(monkeypatch, tmp_path,
                                               after_thaw, fail_at):
    hop, eng, leg, calls = _fake(monkeypatch, tmp_path, after_thaw, fail_at)
    conf = cutover.settings(hop)
    with pytest.raises(SystemExit, match="the cutover was undone"):
        cutover._phased(hop, eng, ["app"], leg, conf, lambda m: None, [])
    log = calls.log
    reached = list(cutover.PHASES).index(fail_at)
    froze = reached >= list(cutover.PHASES).index("freeze")
    # the source is frozen only once asked, and always given back
    assert ("freeze source" in log) == (fail_at != "freeze" and froze)
    assert "thaw source" in log
    if "stream down" in log:
        # the stream is put back, and in the order its kind needs
        assert "stream up" in log
        up, thaw = log.index("stream up"), log.index("thaw source")
        assert (up > thaw) == after_thaw, log
    else:
        assert "stream up" not in log, log
    if "back up" in log:
        assert log.index("back down") < log.index("thaw source"), log
    if "counters select 1" in log:
        assert "counters select 0" in log, log
    run = json.loads((hop.report_dir() / "cutover-runs.jsonl").read_text()
                     .splitlines()[-1])
    assert run["state"] == "undone" and f"broken at {fail_at}" in run["why"]
    assert not (hop.report_dir() / "cutover.json").exists()


def test_a_blown_budget_goes_back_even_when_no_step_fails(monkeypatch,
                                                          tmp_path):
    hop, eng, leg, calls = _fake(monkeypatch, tmp_path, False, "none")
    hop.options["cutover"] = {"budget": 0.3}
    slow = eng.check_data

    def check(d):
        import time
        time.sleep(1)
        return slow(d)
    eng.check_data = check
    with pytest.raises(SystemExit, match="the cutover was undone"):
        cutover._phased(hop, eng, ["app"], leg, cutover.settings(hop),
                        lambda m: None, [])
    assert "thaw source" in calls.log and "stream down" not in calls.log
    run = json.loads((hop.report_dir() / "cutover-runs.jsonl").read_text()
                     .splitlines()[-1])
    assert "budget ran out" in run["why"] or "still reading" in run["why"]


def test_without_it_a_stream_is_left_up_while_writes_go_on(monkeypatch,
                                                          tmp_path):
    """A hop without `cutover:`: the stream torn down only on the proofs;
    MIGKIT_CUTOVER=unproven tears it down anyway and records what the
    proofs said."""
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    hop = _hop(tmp_path)
    torn = []

    class Fence:
        def src_lsn(self, d):
            return "0/10"

        def fence_wait(self, d, pos, timeout=0):
            return True
    leg = cutover.Leg("native", fence=Fence(), manual=torn.append)
    monkeypatch.setattr(cutover, "_quiet",
                        lambda *a: "12 rows written in 1.5s")
    with pytest.raises(SystemExit, match="the stream was left up.*12 rows"):
        cutover._manual(hop, None, ["app"], leg, lambda m: None, [], [],
                        False)
    assert torn == []
    said = []
    cutover._manual(hop, None, ["app"], leg, said.append, [], [], True)
    assert torn == [True]
    assert any("torn down unproven" in s and "12 rows" in s for s in said)
    record = [json.loads(x) for x in (hop.report_dir() / "changelog.jsonl")
              .read_text().splitlines()]
    assert record[-1]["by"] == "hand" and "12 rows" in \
        record[-1]["unproven"][0], record[-1]
