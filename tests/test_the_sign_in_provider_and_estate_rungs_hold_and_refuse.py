"""The sign-in, provider, free-space, hidden-value and estate rungs each
held and refused on the facts that decide them (backlog AUD-Z.rungs-owed):
a Kafka side signs in by what its broker advertised and this machine has,
the provider is asked only of a managed side the egress lets out, a
provider operation stands only where it is no slower than migkit's own
move, the free space comes from the first source that answers, a column is
hidden in the way its readers need, and an estate's server is sized and
priced only from runs measured. Fakes only."""
from types import SimpleNamespace

from migkit import decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

OWN = decide.MANAGED_OWN_MOVE

PROVES_RUNGS = [
    ("kafka_sign_in", "AWS_MSK_IAM", "holds"),
    ("kafka_sign_in", "AWS_MSK_IAM", "refuses"),
    ("kafka_sign_in", "SCRAM-SHA-512", "holds"),
    ("kafka_sign_in", "SCRAM-SHA-512", "refuses"),
    ("kafka_sign_in", "SCRAM-SHA-256", "holds"),
    ("kafka_sign_in", "SCRAM-SHA-256", "refuses"),
    ("kafka_sign_in", "PLAIN", "holds"), ("kafka_sign_in", "PLAIN", "refuses"),
    ("provider_asked", "ask", "holds"), ("provider_asked", "ask", "refuses"),
    ("managed_against_own", "<choice.path or 'provider-operation'>", "holds"),
    ("managed_against_own", "<choice.path or 'provider-operation'>",
     "refuses"),
    ("managed_against_own", "<MANAGED_OWN_MOVE>", "holds"),
    ("free_space_source", "server", "holds"),
    ("free_space_source", "server", "refuses"),
    ("free_space_source", "directory", "holds"),
    ("free_space_source", "directory", "refuses"),
    ("free_space_source", "provider", "holds"),
    ("free_space_source", "provider", "refuses"),
    ("free_space_source", "none", "holds"),
    ("hide_column", "fpe", "holds"), ("hide_column", "fpe", "refuses"),
    ("hide_column", "token", "holds"), ("hide_column", "token", "refuses"),
    ("hide_column", "hash", "holds"),
    ("estate_eta", "pair", "holds"), ("estate_eta", "pair", "refuses"),
    ("estate_eta", "engine", "holds"), ("estate_eta", "engine", "refuses"),
    ("estate_eta", "none", "holds"),
    ("estate_price", "catalog", "holds"),
    ("estate_price", "catalog", "refuses"),
    ("estate_price", "none", "holds"),
]


def test_a_kafka_side_signs_in_by_what_its_broker_advertised():
    with watched() as seen:
        got = decide.kafka_sign_in({"advertised": ["AWS_MSK_IAM",
                                                   "SCRAM-SHA-512"],
                                    "credentials": True, "account": True})
    assert got.path == "AWS_MSK_IAM"
    assert branch(seen, "AWS_MSK_IAM") == [HOLDS]
    assert branch(seen, "SCRAM-SHA-512") == [HOLDS]
    assert branch(seen, "SCRAM-SHA-256") == [REFUSES]
    assert branch(seen, "PLAIN") == [REFUSES]
    with watched() as seen:
        got = decide.kafka_sign_in({"advertised": ["AWS_MSK_IAM",
                                                   "SCRAM-SHA-512"],
                                    "credentials": False, "account": True})
    assert got.path == "SCRAM-SHA-512"
    assert why(seen, "AWS_MSK_IAM") == ("this machine has no provider"
                                        " credentials")
    with watched() as seen:
        got = decide.kafka_sign_in({"advertised": ["SCRAM-SHA-256", "PLAIN"],
                                    "account": True})
    assert got.path == "SCRAM-SHA-256"
    assert branch(seen, "SCRAM-SHA-256") == [HOLDS]
    assert branch(seen, "PLAIN") == [HOLDS]
    assert "does not advertise SCRAM-SHA-512" in why(seen, "SCRAM-SHA-512")
    with watched() as seen:
        got = decide.kafka_sign_in({"advertised": ["SCRAM-SHA-256", "PLAIN"],
                                    "asked": "plain", "account": True})
    assert got.path == "PLAIN"
    assert branch(seen, "SCRAM-SHA-256") == [REFUSES]
    with watched() as seen:
        got = decide.kafka_sign_in({"advertised": ["PLAIN"],
                                    "account": None})
    assert got.path is None
    assert why(seen, "PLAIN") == "the side has no user and password"


def test_the_provider_is_asked_only_of_a_managed_side_the_egress_lets_out():
    with watched() as seen:
        got = decide.provider_asked({"managed": True, "egress": True})
    assert got.path == "ask"
    assert branch(seen, "ask") == [HOLDS]
    with watched() as seen:
        got = decide.provider_asked({"managed": True, "egress": False,
                                     "refused": "the egress is local only"})
    assert got.path is None
    assert branch(seen, "ask") == [REFUSES]
    assert why(seen, "ask") == "the egress is local only"
    with watched() as seen:
        got = decide.provider_asked({"managed": False, "egress": True})
    assert why(seen, "ask").startswith("the side is run by its owner")


class _Took:
    def __init__(self, mean):
        self.mean = mean

    def said(self):
        return f"{self.mean:.0f}s"


OPERATION = SimpleNamespace(path="snapshot-restore",
                            rung=SimpleNamespace(said="a snapshot restored"))


def test_a_provider_operation_stands_only_where_it_is_no_slower():
    costs = {"snapshot-restore": _Took(10), OWN: _Took(20)}
    with watched() as seen:
        got = decide.managed_against_own(OPERATION, costs)
    assert got.path == "snapshot-restore"
    assert branch(seen, "snapshot-restore") == [HOLDS]
    assert branch(seen, OWN) == [HOLDS]
    with watched() as seen:
        got = decide.managed_against_own(
            OPERATION, {"snapshot-restore": _Took(30), OWN: _Took(20)})
    assert got.path == OWN
    assert why(seen, "snapshot-restore") == ("the operation's 30s is longer"
                                             " than migkit's own 20s")
    with watched() as seen:
        got = decide.managed_against_own(OPERATION,
                                         {"snapshot-restore": _Took(10)})
    assert got.path == OWN
    assert "has no forecast" in why(seen, "snapshot-restore")
    with watched() as seen:
        got = decide.managed_against_own(SimpleNamespace(path=None, rung=None),
                                         {OWN: _Took(20)})
    assert got.path == OWN
    assert why(seen, "provider-operation") == ("no provider operation is"
                                               " eligible on the facts read")


def test_the_free_space_comes_from_the_first_source_that_answers():
    with watched() as seen:
        got = decide.free_space_source({"server": 100, "directory": 50,
                                        "provider": 70})
    assert got.path == "server"
    assert branch(seen, "server") == [HOLDS]
    assert why(seen, "directory") == "the server answered first"
    assert why(seen, "provider") == "the server answered first"
    assert branch(seen, "none") == [HOLDS]
    with watched() as seen:
        got = decide.free_space_source({"server": None,
                                        "server_why": "no right to read it",
                                        "directory": 50})
    assert got.path == "directory"
    assert branch(seen, "directory") == [HOLDS]
    assert why(seen, "server") == "no right to read it"
    with watched() as seen:
        got = decide.free_space_source({"provider": 70})
    assert got.path == "provider"
    assert branch(seen, "provider") == [HOLDS]
    assert why(seen, "directory") == "the directory did not answer"
    with watched() as seen:
        got = decide.free_space_source({"provider_why": "no metric"})
    assert got.path == "none"
    assert why(seen, "provider") == "no metric"


def test_a_column_is_hidden_in_the_way_its_readers_need():
    with watched() as seen:
        got = decide.hide_column({"kind": "digits", "length": 9,
                                  "shape": True})
    assert got.path == "fpe"
    assert branch(seen, "fpe") == [HOLDS]
    assert branch(seen, "hash") == [HOLDS]
    assert "its shape is read" in why(seen, "token")
    with watched() as seen:
        got = decide.hide_column({"kind": "digits", "length": 3,
                                  "unique": True})
    assert got.path == "token"
    assert branch(seen, "token") == [HOLDS]
    assert "possible values" in why(seen, "fpe")
    with watched() as seen:
        got = decide.hide_column({"kind": "text", "length": 20,
                                  "reversible": True})
    assert got.path == "token"
    assert why(seen, "fpe") == ("the values are not digits or a known set"
                                " of characters")
    with watched() as seen:
        got = decide.hide_column({"kind": "text", "length": 20})
    assert got.path == "hash"
    assert why(seen, "fpe") == ("nothing reads the shape or needs the value"
                                " back: a hash is enough")
    assert branch(seen, "token") == [REFUSES]


def test_an_estates_server_is_sized_only_from_runs_measured():
    pair = [{"bytes": 1000, "seconds": 10}]
    engine = [{"bytes": 1000, "seconds": 20}]
    with watched() as seen:
        got, bounds = decide.estate_eta({"size": 10 ** 6, "pair": pair,
                                         "engine": engine})
    assert got.path == "pair" and bounds == (10 ** 4, 10 ** 4)
    assert branch(seen, "pair") == [HOLDS]
    assert branch(seen, "engine") == [HOLDS]
    assert branch(seen, "none") == [HOLDS]
    with watched() as seen:
        got, bounds = decide.estate_eta({"size": 10 ** 6, "engine": engine})
    assert got.path == "engine" and bounds == (2 * 10 ** 4, 2 * 10 ** 4)
    assert why(seen, "pair") == "no run of this engine pair was kept"
    with watched() as seen:
        got, bounds = decide.estate_eta({"size": None, "pair": pair,
                                         "engine": engine})
    assert got.path == "none" and bounds is None
    assert why(seen, "pair") == ("nobody signed in to read the size, so no"
                                 " time is said")
    assert branch(seen, "engine") == [REFUSES]
    with watched() as seen:
        got, bounds = decide.estate_eta({"size": 10 ** 6})
    assert got.path == "none"
    assert why(seen, "engine") == "no run from this engine was kept"


def test_an_estates_server_is_priced_only_over_a_measured_time():
    with watched() as seen:
        got, ends = decide.estate_price({"bounds": (10.0, 20.0),
                                         "types": {("cloud", "r"): ["m"]},
                                         "stages": ()})
    assert got.path == "catalog" and ends == ()
    assert branch(seen, "catalog") == [HOLDS]
    assert branch(seen, "none") == [HOLDS]
    with watched() as seen:
        got, ends = decide.estate_price({"bounds": None,
                                         "types": {("cloud", "r"): ["m"]}})
    assert got.path == "none" and ends is None
    assert why(seen, "catalog") == "no measured time, so no price"
    with watched() as seen:
        got, ends = decide.estate_price({"bounds": (10.0, 20.0),
                                         "types": {("cloud", "r"): []}})
    assert got.path == "none"
    assert why(seen, "catalog") == "no type in the catalog has a price there"
