"""Synapse's dedicated SQL pool as a side, on a fake answering its catalogue (backlog
P0-CLOUDS.more): distribution, COPY INTO from a landing and no follow, each on its facts. No server is reached."""
import types

import pytest

from migkit import decide, managed, users
from migkit.engines import synapse


@pytest.fixture
def eng(monkeypatch):
    got = synapse.SynapseEngine.__new__(synapse.SynapseEngine)
    got.hop = types.SimpleNamespace(
        engine="synapse", source=types.SimpleNamespace(options={}),
        target=types.SimpleNamespace(options={}))
    got.said = []
    monkeypatch.setattr(decide, "record",
                        lambda hop, choice, log=None, **kw: got.said.append(
                            choice))
    monkeypatch.setattr(type(got), "_d", lambda self, side, db: db,
                        raising=False)
    return got


def test_the_catalogue_row_is_kept_for_the_engine():
    assert "azure-synapse" in managed.services()


def test_its_accounts_are_read_as_its_wires():
    hop = types.SimpleNamespace(engine="synapse")
    assert users._speaking(hop).engine == "mssql"


def test_a_table_is_hashed_by_its_key_or_round_robin(eng):
    assert eng.spread_of("app", "t", ["id"]) == \
        "with (distribution = hash([id]))"
    assert eng.spread_of("app", "t", []) == \
        "with (distribution = round_robin)"


def test_copy_into_needs_a_writer_for_its_landing(eng):
    eng.hop.target.options = {"stage": "s3://b/p"}
    assert eng.load_way("app", "t").path == "copier"
    assert "nothing here writes s3://b/p" in eng.said[-1].reason
    sql = eng.landing_sql("app", "t", ["id"], "https://a.blob.core.windows"
                                              ".net/c/t.csv")
    assert sql[0].startswith("copy into") and "file_type = 'CSV'" in sql[0]


def test_as_a_source_it_is_refused_with_why(eng):
    assert "no change data capture" in eng.follow_way("app").reason
