"""The ways an engine builds as it runs - the sample of a collection, an
object's records, the rows of an entity set by their key - each reached
on every branch it has on fakes (backlog AUD-Z.rungs-outside-owed): the
engine's own function builds the ladder and the facts it stands on from
what the fake server says, and the decision is handed them; the run that
follows is the fake's, so what is proved is which way the facts admit and
why the other was passed over."""
import types

from migkit import decide
from migkit.engines import qdrant, saas_salesforce, sapodp
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("engines.qdrant", "QdrantEngine", "random", "holds"),
    ("engines.qdrant", "QdrantEngine", "leading", "holds"),
    ("engines.saas_salesforce", "Salesforce", "bulk", "holds"),
    ("engines.saas_salesforce", "Salesforce", "rest", "holds"),
    ("engines.sapodp", "SapOdpEngine", "by key", "holds"),
    ("engines.sapodp", "SapOdpEngine", "by key", "refuses"),
    ("engines.sapodp", "SapOdpEngine", "whole", "holds"),
]


def _deciding(handed, payload):
    """An engine's `_attempt` that hands the ladder and the facts to the
    decision, keeps what it answered, and returns `payload` as the run's."""
    def attempt(work, rungs, run, facts=None, *args, **kw):
        kept, out = decide.prune(rungs, facts if facts is not None
                                 else decide.Facts())
        handed.append((work, kept, out))
        return types.SimpleNamespace(rung=kept[0] if kept else None), payload
    return attempt


def test_a_sample_is_drawn_by_the_server_or_read_from_the_leading_ids():
    q = object.__new__(qdrant.QdrantEngine)
    handed = []
    q._attempt = _deciding(handed, [{"id": 1}])
    with watched() as seen:
        got, way = q._sample_points("src", "docs", 1)
    assert got == [{"id": 1}] and way == "random"
    (work, kept, out), = handed
    assert "docs" in work and not out
    assert [r.name for r in kept] == ["random", "leading"]
    assert branch(seen, "random") == [HOLDS]
    assert branch(seen, "leading") == [HOLDS]


def test_an_objects_records_are_read_by_a_job_or_by_the_pages():
    s = object.__new__(saas_salesforce.Salesforce)
    handed = []
    s._soql = lambda stream: f"SELECT Id FROM {stream.name}"
    s.engine = types.SimpleNamespace(_attempt=_deciding(
        handed, ({"Id": "1"}, iter([{"Id": "2"}]))))
    with watched() as seen:
        got = list(s._records(types.SimpleNamespace(name="Account")))
    assert got == [{"Id": "1"}, {"Id": "2"}]
    (work, kept, out), = handed
    assert work == "reading Account" and not out
    assert [r.name for r in kept] == ["bulk", "rest"]
    assert branch(seen, "bulk") == [HOLDS]
    assert branch(seen, "rest") == [HOLDS]


def _odp(filterable):
    e = object.__new__(sapodp.SapOdpEngine)
    e._props = lambda side, db, table: [
        types.SimpleNamespace(name="id", filterable=filterable),
        types.SimpleNamespace(name="name", filterable=True)]
    return e


def test_rows_by_key_are_asked_by_the_key_only_where_the_service_filters_it():
    handed = []
    columns = [("id", "integer"), ("name", "text")]
    with watched() as seen:
        for filterable in (True, False):
            e = _odp(filterable)
            e._attempt = _deciding(handed, [])
            assert e.neutral_rows_by_key("src", "svc", "Orders", columns,
                                         ["id"], [(1,)]) == {}
    (_, kept, out), (_, kept2, out2) = handed
    assert [r.name for r in kept] == ["by key", "whole"] and not out
    assert [r.name for r in kept2] == ["whole"]
    assert [r.name for r, _ in out2] == ["by key"]
    assert out2[0][1] == "the service does not filter every key field"
    assert branch(seen, "by key") == [HOLDS, REFUSES]
    assert why(seen, "by key") == out2[0][1]
    assert branch(seen, "whole") == [HOLDS]
