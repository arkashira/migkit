"""What `assess` says of the engines that follow a source's changes through
a feed, or through none (P0-PARITY.assess, `assess.engine-checks`), without
a server: each engine's own readiness rows, from the hop's options and
what the side answers.

* a source that keeps no log a client can read says its changes are not
  followed, and says what to name to follow them - it never passes a
  stream it was not given;
* one given the stream says what carries the changes and in what order;
* a side that cannot be reached, or a source of no kind, fails: the
  report never reads a side it could not ask as ready.
"""
import tempfile
from pathlib import Path

import pytest

from migkit.config import Endpoint, Hop
from migkit.engines import _class_for

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [("assess.engine-checks", ("tidb", "dsql", "couchbase",
                                    "cockroachdb", "cosmosdb", "teradata",
                                    "saas"))]


def _engine(name, **options):
    ep = Endpoint(host="10.0.0.9", port=1, user="u", password="p",
                  options=dict(options))
    hop = Hop(name="t", engine=name, source=ep, target=ep, databases=["d"])
    where = Path(tempfile.mkdtemp())
    hop.report_dir = lambda db=None: where
    return _class_for(name)(hop)


def _rows(eng):
    rows = eng._assess_extra()
    assert rows
    for r in rows:
        assert r["level"] in ("pass", "warn", "fail", "info"), r
        assert r["scope"] == "reach" and r["item"] and r["detail"], r
    return {r["item"]: r for r in rows}


@pytest.mark.parametrize("name,named", [
    ("tidb", "Kafka topic TiCDC publishes to"),
    ("dsql", "Kinesis stream DSQL's CDC stream publishes to"),
    ("couchbase", "topic Couchbase's Kafka connector publishes to"),
])
def test_a_source_with_no_stream_given_says_its_changes_are_not_followed(
        name, named):
    rows = _rows(_engine(name))
    assert rows["copied and verified"]["level"] == "pass"
    assert rows["changes are not followed"]["level"] == "warn"
    assert named in rows["changes are not followed"]["detail"]
    assert "changes followed" not in " ".join(rows)


def test_tidb_given_its_topic_says_how_the_changes_are_carried():
    rows = _rows(_engine("tidb", changes={"topic": "cdc",
                                          "format": "canal-json"}))
    got = rows["changes followed through TiCDC"]
    assert got["level"] == "pass"
    assert "canal-json from cdc" in got["detail"]
    assert "in commit order" in got["detail"]
    assert "changes are not followed" not in rows


@pytest.mark.parametrize("name,item,said", [
    ("cockroachdb", "changes followed through a changefeed",
     "in commit order by each resolved timestamp"),
    ("cosmosdb", "changes followed through the change feed",
     "all-versions-and-deletes"),
])
def test_an_engine_with_its_own_feed_says_what_carries_the_changes(
        name, item, said):
    rows = _rows(_engine(name))
    assert rows["copied and verified"]["level"] == "pass"
    assert rows[item]["level"] == "pass"
    assert said in rows[item]["detail"]


def test_teradata_never_reads_a_side_it_could_not_ask_as_ready(monkeypatch):
    from migkit.engines.teradata import TeradataEngine

    def refused(self, side, db):
        raise SystemExit("the driver is not installed")
    monkeypatch.setattr(TeradataEngine, "_sessions_readable", refused)
    rows = TeradataEngine._assess_extra(_engine("teradata"))
    assert [r["level"] for r in rows] == ["fail", "fail"]
    assert all("not installed" in r["detail"] for r in rows)


def test_teradata_says_what_its_sessions_and_its_changes_come_to(
        monkeypatch):
    from migkit.engines.teradata import TeradataEngine
    monkeypatch.setattr(TeradataEngine, "_sessions_readable",
                        lambda self, side, db: False)
    src = {r["item"]: r for r in _engine("teradata").reach_items("src")}
    assert src["parallel sessions for large reads and loads"]["level"] \
        == "warn"
    assert "single statements" in \
        src["parallel sessions for large reads and loads"]["detail"]
    assert "no change log a client can read" in src["changes"]["detail"]
    # the target has no changes to say
    dst = {r["item"] for r in _engine("teradata").reach_items("dst")}
    assert "changes" not in dst


def test_a_saas_source_of_no_kind_fails():
    rows = _rows(_engine("saas"))
    got = rows["the program's catalogue"]
    assert got["level"] == "fail"
    assert "not none" in got["detail"]
