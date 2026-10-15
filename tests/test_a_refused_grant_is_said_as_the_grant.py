"""A server's refusal for want of a grant stops as a blocker naming the
grant - never an answer made up in its place."""
import pytest

from migkit import decide
from migkit.engines import enterprise_connectors as ec


class _Refused(Exception):
    def __init__(self, msg, code=None):
        super().__init__(msg)
        self.code = code


def test_a_refused_list_of_databases_names_its_grant():
    e = _Refused("not authorized on admin to execute command"
                 " { listDatabases: 1, nameOnly: true }", code=13)
    with pytest.raises(ec.MissingGrant) as got:
        ec.missing_grant("mongodb", "listDatabases", e)
    assert isinstance(got.value, decide.Blocked)
    assert "listDatabases" in str(got.value)
    assert "readAnyDatabase or clusterMonitor" in str(got.value)
    assert got.value.action == "listDatabases"


def test_every_action_raises_never_answers():
    for action in ("listDatabases", "serverStatus", "collStats",
                   "changeStream", "replSetGetStatus", "no_such_action"):
        with pytest.raises(ec.MissingGrant):
            ec.missing_grant("mongodb", action, _Refused("unauthorized",
                                                         code=13))


def test_the_action_is_read_from_the_refusal():
    e = _Refused("not authorized on shop to execute command"
                 " { collStats: \"orders\" }", code=13)
    assert "collStats" in ec.lacking("mongodb", e)
    assert "read role" in ec.lacking("mongodb", e)


def test_a_postgres_refusal_by_sqlstate():
    e = Exception("permission denied to set parameter")
    e.pgcode = "42501"
    assert "superuser" in ec.lacking("postgres", e, "disable_triggers")
    assert "superuser" in ec.lacking("cockroachdb", e, "disable_triggers")


def test_no_grant_is_named_for_what_is_no_refusal():
    assert ec.lacking("mongodb", TimeoutError("timed out")) is None
    assert ec.lacking("postgres", None) is None
    e = _Refused("connection refused", code=6)
    assert ec.lacking("mongodb", e) is None


def test_an_unknown_action_still_stops():
    with pytest.raises(ec.MissingGrant) as got:
        ec.missing_grant("mysql", "load_data", None)
    assert "load_data" in str(got.value)
