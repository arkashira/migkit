"""The online MongoDB sync is committed on the lag it reports now, and
never on a lag it does not report.

The commit read `lagTimeSeconds`, deprecated since 1.21 in favour of
`lag.overallLagSeconds` and gone from 1.22. There, `(absent or 0) <= 1`
read as caught up the moment `canCommit` turned true - up to 30 seconds
behind the source by its own definition of `canCommit` - and the commit
stopped the copy with those changes never applied. The sync is driven
here against a stand-in that answers `/progress` in each build's shape.
"""
import pytest

from migkit import movers
from migkit.config import Endpoint, Hop


class _Proc:
    def poll(self):
        return None


def _drive(monkeypatch, answers):
    """Run the commit loop against `answers`, one `/progress` each; the
    last repeats. Returns the calls made."""
    calls, left = [], list(answers)
    clock = [1000.0]

    def api(port, method, path, body=None, timeout=15):
        calls.append((method, path))
        if path == "start":
            return {"success": True}
        if path == "commit":
            return {"success": True}
        if calls.count(("POST", "commit")):
            return {"progress": {"state": "COMMITTED"}}
        p = left.pop(0) if len(left) > 1 else left[0]
        return {"progress": p}

    def tick(seconds=0):
        clock[0] += seconds or 1
    monkeypatch.setattr(movers, "_sync_api", api)
    monkeypatch.setattr("time.sleep", tick)
    monkeypatch.setattr("time.time", lambda: clock[0])
    movers._mongosync_run(1, _Proc(), {}, "app", None)
    return calls


V121_BEHIND = {"state": "RUNNING", "canCommit": True, "lagTimeSeconds": 0,
               "lag": {"overallLagSeconds": 12, "crudLagSeconds": 12,
                       "ddlLagSeconds": 0}}
V121_CAUGHT_UP = {"state": "RUNNING", "canCommit": True, "lagTimeSeconds": 0,
                  "lag": {"overallLagSeconds": 0}}
V122_BEHIND = {"state": "RUNNING", "canCommit": True,
               "lag": {"overallLagSeconds": 25, "crudLagSeconds": 25,
                       "ddlLagSeconds": 0}}
V122_CAUGHT_UP = {"state": "RUNNING", "canCommit": True,
                  "lag": {"overallLagSeconds": 1}}


@pytest.mark.parametrize("behind,caught_up", [
    (V121_BEHIND, V121_CAUGHT_UP), (V122_BEHIND, V122_CAUGHT_UP)],
    ids=["1.21", "1.22"])
def test_it_commits_only_once_the_overall_lag_is_caught_up(
        monkeypatch, behind, caught_up):
    calls = _drive(monkeypatch, [behind, behind, behind, caught_up])
    polls_before = calls[:calls.index(("POST", "commit"))]
    # three answers behind, then the one it committed on
    assert polls_before.count(("GET", "progress")) == 4, calls


def test_a_build_that_reports_no_lag_is_not_committed(monkeypatch):
    """1.22 without the lag object - or any build that stops reporting
    it - is refused, never read as zero."""
    bare = {"state": "RUNNING", "canCommit": True}
    with pytest.raises(RuntimeError, match="does not say how far behind"):
        _drive(monkeypatch, [bare])


def test_a_lag_that_comes_back_is_waited_for(monkeypatch):
    bare = {"state": "RUNNING", "canCommit": True}
    calls = _drive(monkeypatch, [bare, bare, V122_CAUGHT_UP])
    assert ("POST", "commit") in calls


def test_the_reading_itself():
    assert movers.commit_ready(V122_BEHIND) == (False, 25)
    assert movers.commit_ready(V122_CAUGHT_UP) == (True, 1)
    assert movers.commit_ready({"canCommit": False,
                                "lag": {"overallLagSeconds": 0}}) == (False, 0)
    # the deprecated field is no longer what decides
    assert movers.commit_ready({"canCommit": True, "lagTimeSeconds": 0}) \
        == (False, None)
    assert movers.commit_ready({"canCommit": True,
                                "lag": {"overallLagSeconds": None}}) \
        == (False, None)


def _hop(**options):
    ep = Endpoint(host="10.0.0.1", port=27017, user="", password="")
    return Hop(name="m", engine="mongodb", source=ep, target=ep,
               databases=["app"], options=options)


def test_without_a_declared_licence_the_open_path_runs_and_says_why(
        monkeypatch):
    monkeypatch.setattr(movers, "which", lambda p: "/x/" + p)
    via, why = movers.fitted(_hop(), "mongodb", "mongosync")
    assert via == "mongodump", via
    assert "Atlas" in why and "Enterprise Advanced" in why, why
    # and the reason names no program
    assert "mongosync" not in why.lower()


def test_a_declared_licence_lets_it_be_asked_of_the_servers(monkeypatch):
    monkeypatch.setattr(movers, "which", lambda p: "/x/" + p)
    asked = []

    class Eng:
        def __init__(self, hop):
            pass

        def _client(self, side):
            asked.append(side)
            raise OSError("no server here")
    import migkit.engines.mongodb as m
    monkeypatch.setattr(m, "MongoEngine", Eng)
    for said in ("atlas", "enterprise", "Enterprise-Advanced"):
        via, why = movers.fitted(_hop(mongodb_entitlement=said), "mongodb",
                                 "mongosync")
        # past the licence, to the servers - which answered nothing here
        assert "could not be asked" in why, (said, why)
    assert asked == ["src"] * 3
