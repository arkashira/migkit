"""The rungs the two-way marks and the physical copy's pages are chosen
by, each reached on both its branches on fakes (backlog
AUD-Z.rungs-outside): a mark the side allows is held, one it does not is
refused with the side's own reason - so a tail never stands on a mark
that cannot tell migkit's writes apart, nor counts batches by one that
cannot say which it last committed - and the pages are compressed only
by a source that can."""
import pytest

from migkit import decide, marks, physical
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why

#: (family, rung, facts it is allowed on, facts it is not, the reason)
SIDES = [
    ("postgres", "origin", {"origin_grants": True}, {},
     "replication origin functions are not granted"),
    ("postgres", "origin", {"origin_grants": True},
     {"origin_grants": True, "foreign_origins": ["other"]},
     "another replication origin applies into this server"),
    ("postgres", "message", {"version": 160000}, {"version": 90500},
     "logical messages are written from PostgreSQL 9.6"),
    ("postgres", "table", {"can_create": True}, {},
     "cannot make the table"),
    ("mysql", "table", {"can_create": True}, {"can_create": False},
     "cannot make the table"),
    ("mysql", "gtid_tag",
     {"version": (8, 4), "gtid_mode": "ON",
      "grants": {"TRANSACTION_GTID_TAG", "SESSION_VARIABLES_ADMIN"}},
     {"version": (8, 4), "gtid_mode": "ON",
      "grants": {"TRANSACTION_GTID_TAG"}},
     "may not set a session's GTID"),
    ("mysql", "comment", {"rows_query": True}, {"mariadb": True},
     "annotate events"),
    ("mysql", "gtid_domain", {"mariadb": True, "gtid_domain": 7},
     {"mariadb": True}, "GTID domain is not configured"),
    ("mysql", "skip_flag", {"mariadb": True, "can_create": True},
     {"can_create": True}, "skip_replication is MariaDB's"),
    ("mongodb", "txn_doc", {"transactions": True, "can_create": True},
     {"can_create": True}, "needs a replica set or a sharded cluster"),
    ("mongodb", "field", {"pre_images": True}, {},
     "unless pre-images are on"),
    ("cassandra", "batch", {"can_create": True}, {},
     "cannot make the table"),
    ("redis", "multi", {"version": (7, 0)},
     {"version": (7, 0), "cluster": True},
     "a cluster's transaction keeps to one slot"),
    ("dynamodb", "item", {"transactions": True, "can_create": True},
     {"can_create": True}, "takes no TransactWriteItems"),
    ("kafka", "header", {"headers": True}, {}, "takes no record headers"),
]

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("marks", "RUNGS", "origin", "holds"),
    ("marks", "RUNGS", "origin", "refuses"),
    ("marks", "RUNGS", "message", "holds"),
    ("marks", "RUNGS", "message", "refuses"),
    ("marks", "RUNGS", "table", "holds"),
    ("marks", "RUNGS", "table", "refuses"),
    ("marks", "RUNGS", "gtid_tag", "holds"),
    ("marks", "RUNGS", "gtid_tag", "refuses"),
    ("marks", "RUNGS", "comment", "holds"),
    ("marks", "RUNGS", "comment", "refuses"),
    ("marks", "RUNGS", "gtid_domain", "holds"),
    ("marks", "RUNGS", "gtid_domain", "refuses"),
    ("marks", "RUNGS", "skip_flag", "holds"),
    ("marks", "RUNGS", "skip_flag", "refuses"),
    ("marks", "RUNGS", "txn_doc", "holds"),
    ("marks", "RUNGS", "txn_doc", "refuses"),
    ("marks", "RUNGS", "field", "holds"),
    ("marks", "RUNGS", "field", "refuses"),
    ("marks", "RUNGS", "batch", "holds"),
    ("marks", "RUNGS", "batch", "refuses"),
    ("marks", "RUNGS", "multi", "holds"),
    ("marks", "RUNGS", "multi", "refuses"),
    ("marks", "RUNGS", "item", "holds"),
    ("marks", "RUNGS", "item", "refuses"),
    ("marks", "RUNGS", "header", "holds"),
    ("marks", "RUNGS", "header", "refuses"),
    ("marks", "_decided", "<r.name>", "holds"),
    ("marks", "_decided", "<r.name>", "refuses"),
    ("physical", "ZSTD", "zstd", "holds"),
    ("physical", "ZSTD", "zstd", "refuses"),
    ("physical", "PLAIN", "as they are", "holds"),
    ("physical", "LOGICAL", "logical", "holds"),
]

def test_the_declaration_names_every_mark_proved_here():
    named = {r for m, _, r, _ in PROVES_RUNGS if m == "marks"}
    assert {name for _, name, *_ in SIDES} <= named


@pytest.mark.parametrize("family,name,ok,bad,said", SIDES)
def test_a_mark_is_held_where_the_side_allows_it_and_refused_where_not(
        family, name, ok, bad, said):
    r = marks.rung(family, name)
    assert isinstance(r, decide.Rung) and r.engine == family
    with watched() as seen:
        assert r.refused(ok) is None
        got = r.refused(bad)
    assert branch(seen, name) == [HOLDS, REFUSES], seen
    assert got and said in got and why(seen, name) == got


def test_every_mark_is_a_rung_of_the_engines_own():
    assert marks.Rung is decide.ServerRung
    assert len({(r.engine, r.name) for r in marks.RUNGS}) == len(marks.RUNGS)
    for r in marks.RUNGS:
        assert isinstance(r, decide.ServerRung), r
        assert r.footprint == (0 if r.leaves == "nothing" else 1), r
        assert r.needs and r.needs[0].what == "the side to allow it", r


class _Side:
    CANON_ENGINE = "postgres"

    def __init__(self):
        self.proved = []

    def mark_prove(self, side, db, name):
        self.proved.append((side, db, name))
        return None


def test_a_hop_that_counts_refuses_a_mark_that_cannot_say_its_batch():
    side = _Side()
    facts = {"family": "postgres", "can_create": True, "version": 160000,
             "exact": True}
    ways = marks._decided([marks.rung("postgres", "message"),
                           marks.rung("postgres", "table")],
                          side, "dst", "app", facts)
    with watched() as seen:
        kept, out = decide.prune(ways, facts)
    assert [r.name for r in kept] == ["table"]
    assert [r.name for r, _ in out] == ["message"]
    assert "cannot say which batch" in out[0][1]
    assert HOLDS in branch(seen, "table")
    assert REFUSES in branch(seen, "message")
    # what each leaves goes with it, and its proof is the side's own
    table = kept[0]
    assert table.footprint == 1 and "migkit_origin" in table.leaves
    assert table.prove(facts) is None and side.proved == [
        ("dst", "app", "table")]
    # without counting, the message is allowed again
    facts = dict(facts, exact=False)
    with watched() as seen:
        kept, _ = decide.prune(marks._decided(
            [marks.rung("postgres", "message")], side, "dst", "app", facts),
            facts)
    assert [r.name for r in kept] == ["message"]
    assert branch(seen, "message") == [HOLDS]


def test_the_pages_are_compressed_only_by_a_source_that_can():
    with watched() as seen:
        old = decide.climb("the pages", (physical.ZSTD, physical.PLAIN),
                           {"version": 140000}, prove=False)
        new = decide.climb("the pages", (physical.ZSTD, physical.PLAIN),
                           {"version": 160000}, prove=False)
    assert old.rung is physical.PLAIN and new.rung is physical.ZSTD
    assert branch(seen, "zstd") == [REFUSES, HOLDS]
    assert "only from PostgreSQL 15" in why(seen, "zstd")
    assert branch(seen, "as they are") == [HOLDS]


def test_the_logical_copy_holds_wherever_it_is_offered():
    with watched() as seen:
        kept, out = decide.prune([physical.LOGICAL], {})
    assert kept == [physical.LOGICAL] and not out
    assert branch(seen, "logical") == [HOLDS]
    assert physical.Rung is decide.ServerRung
    assert all(isinstance(r, decide.ServerRung) for r in physical.rungs())
