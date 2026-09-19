"""Two readings that failed are not two servers that agree.

The same mistake keeps turning up in a different place, so this file collects
the cases as they are found rather than leaving each one next to its engine.
What they have in common: a side that could not be read produced a value, the
two sides' values were compared, and because both had failed identically the
comparison said they matched.

`_param_result` is the widest one - it is shared by four engines. Measured on
a MongoDB started with authentication and connected to without credentials,
which is what a managed cluster looks like from outside:

    admin.command({"getParameter": "*"})   OperationFailure: requires authentication
    admin.command("buildInfo")             allowed
    migkit check --check params            ok | 1 settings, all equal both sides

The second case here is narrower but the same shape: MongoDB counted only the
collections both sides had, so a collection of 9 documents that existed on the
source alone was dropped from the check before anything was counted, and the
count that remained was reported as agreement.
"""
import pathlib
import subprocess
import tempfile
import time

import pytest

from migkit.config import Endpoint, Hop

MG = "migkit-test-auth-mg"
MG_PORT = 27078
USER, PASSWORD = "root", "secret"


def _docker():
    try:
        subprocess.run(["docker", "info"], capture_output=True, timeout=10,
                       check=True)
        return True
    except Exception:
        return False


needs_docker = pytest.mark.skipif(not _docker(), reason="docker not available")


def _engine(tmp_path, user="", password="", port=MG_PORT, db_map=None):
    from migkit.engines.mongodb import MongoEngine
    auth = "&authSource=admin" if user else ""
    ep = Endpoint(host="127.0.0.1", port=port, user=user, password=password,
                  options={"uri_options": f"directConnection=true{auth}"})
    hop = Hop(name="m", engine="mongodb", source=ep, target=ep,
              db_map=db_map or {"x": "x"})
    hop.report_dir = lambda db=None: tmp_path
    return MongoEngine(hop)


# ---- the shared contract, which needs no server ------------------------

def _offline_engine():
    """An engine that is never asked to connect: `_param_result` compares the
    two mappings it is handed and writes the dump, nothing more."""
    from migkit.engines.mongodb import MongoEngine
    ep = Endpoint(host="127.0.0.1", port=1, user="", password="")
    hop = Hop(name="m", engine="mongodb", source=ep, target=ep,
              db_map={"x": "x"})
    hop.report_dir = lambda db=None: pathlib.Path(tempfile.mkdtemp())
    return MongoEngine(hop)


def test_two_sides_that_both_failed_are_not_equal_settings():
    eng = _offline_engine()
    failed = {eng.UNREADABLE: "Command getParameter requires authentication"}
    got = eng._param_result("x", dict(failed), dict(failed), ("time_zone",),
                            "hint")
    assert [r.status for r in got] == ["error"], [(r.status, r.detail)
                                                  for r in got]
    assert "could not be read" in got[0].detail
    assert "source:" in got[0].detail and "target:" in got[0].detail
    assert "not two servers that agree" in got[0].detail


def test_one_side_that_failed_is_not_a_clean_comparison():
    eng = _offline_engine()
    real = {"time_zone": "UTC", "sql_mode": "STRICT"}
    failed = {eng.UNREADABLE: "denied"}
    for src, dst, who in ((real, failed, "target"), (failed, real, "source")):
        got = eng._param_result("x", src, dst, ("time_zone",), "hint")
        assert got[0].status == "error", got[0].detail
        assert f"{who}: denied" in got[0].detail, got[0].detail


def test_nothing_at_all_from_both_sides_is_not_agreement():
    """The emptiest possible pass: `0 settings, all equal both sides`."""
    eng = _offline_engine()
    got = eng._param_result("x", {}, {}, ("time_zone",), "hint")
    assert got[0].status == "error", got[0].detail
    assert "nothing came back" in got[0].detail
    assert "0 settings" not in got[0].detail


def test_settings_that_really_are_equal_still_pass():
    """A brake that reports an error on healthy input is not an improvement."""
    eng = _offline_engine()
    same = {"time_zone": "UTC", "max_connections": "100"}
    got = eng._param_result("x", dict(same), dict(same), ("time_zone",),
                            "hint")
    assert got[0].status == "ok", got[0].detail
    assert "2 settings, all equal" in got[0].detail

    differs = dict(same, time_zone="+07:00")
    got = eng._param_result("x", same, differs, ("time_zone",), "hint")
    assert got[0].status == "diff", got[0].detail
    assert "time_zone" in got[0].detail


# ---- the same thing against a server that really does refuse -----------

@pytest.fixture(scope="module")
def auth_mongo():
    subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)
    subprocess.run(
        ["docker", "run", "-d", "--name", MG, "-p", f"{MG_PORT}:27017",
         "-e", f"MONGO_INITDB_ROOT_USERNAME={USER}",
         "-e", f"MONGO_INITDB_ROOT_PASSWORD={PASSWORD}", "mongo:7"],
        check=True, capture_output=True)
    # wait for the server to be refusing, not merely answering: the entrypoint
    # runs an unauthenticated server while it creates the user, and a test
    # that connected during that window would be testing the wrong server
    for _ in range(90):
        r = subprocess.run(
            ["docker", "exec", MG, "mongosh", "--quiet", "--eval",
             "JSON.stringify(db.getSiblingDB('admin')"
             ".runCommand({getParameter: '*'}))"],
            capture_output=True, text=True)
        if "requires authentication" in (r.stdout + r.stderr):
            break
        time.sleep(1)
    else:
        pytest.skip("mongo never started enforcing authentication")
    yield
    subprocess.run(["docker", "rm", "-f", "-v", MG], capture_output=True)


@pytest.mark.docker
@needs_docker
def test_a_server_that_refuses_to_say_is_not_a_server_that_matches(
        auth_mongo, tmp_path):
    """The measurement this file opens with, run through the check itself."""
    got = _engine(tmp_path).check_params("x")
    assert [r.status for r in got] == ["error"], [(r.status, r.detail)
                                                  for r in got]
    assert "requires authentication" in got[0].detail, got[0].detail
    assert "not two servers that agree" in got[0].detail
    assert "account that can read" in got[0].fix_hint

    # the brand probe is unaffected: buildInfo is allowed without credentials,
    # which is worth knowing - the two are not the same permission
    assert _engine(tmp_path)._brands()[0].name == "mongodb"


@pytest.mark.docker
@needs_docker
def test_the_same_check_passes_once_it_can_read_them(auth_mongo, tmp_path):
    got = _engine(tmp_path, USER, PASSWORD).check_params("x")
    assert [r.status for r in got] == ["ok"], [(r.status, r.detail)
                                               for r in got]
    assert "settings, all equal both sides" in got[0].detail
    assert " 0 settings" not in got[0].detail, got[0].detail


@pytest.mark.docker
@needs_docker
def test_a_collection_on_one_side_only_is_counted_as_a_difference(
        auth_mongo, tmp_path):
    """Two databases on the one server stand in for the two sides."""
    from pymongo import MongoClient
    uri = (f"mongodb://{USER}:{PASSWORD}@127.0.0.1:{MG_PORT}/"
           "?directConnection=true&authSource=admin")
    client = MongoClient(uri)
    client.drop_database("cs")
    client.drop_database("cd")
    client["cs"]["shared"].insert_many([{"_id": i, "v": "x"} for i in range(5)])
    client["cd"]["shared"].insert_many([{"_id": i, "v": "x"} for i in range(5)])
    client["cs"]["source_only"].insert_many([{"_id": i} for i in range(9)])

    eng = _engine(tmp_path, USER, PASSWORD, db_map={"cs": "cd"})
    got = eng.check_counts("cs")
    assert all(r.status == "diff" for r in got), [(r.status, r.detail)
                                                  for r in got]
    assert any("missing collections on target" in r.detail for r in got), [
        r.detail for r in got]
    assert any("source_only" in r.detail for r in got)
    # the nine documents are not passed over in silence
    assert not any("5==5" in r.detail for r in got), [r.detail for r in got]

    client["cd"]["source_only"].insert_many([{"_id": i} for i in range(9)])
    ok = eng.check_counts("cs")
    assert [r.status for r in ok] == ["ok"], [(r.status, r.detail)
                                              for r in ok]
    assert "docs 14==14" in ok[0].detail, ok[0].detail

    client["cd"]["target_only"].insert_one({"_id": 1})
    extra = eng.check_counts("cs")
    assert any("extra collections on target" in r.detail for r in extra), [
        r.detail for r in extra]
    client.close()
