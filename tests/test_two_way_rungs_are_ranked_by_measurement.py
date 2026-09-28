"""How a two-way tail marks its own writes is chosen per side, from the
rungs the side allows, by what they were measured to cost (backlog R3,
the owner's rule: footprint only breaks a tie), and each rung's mark is
read back as the servers wrote it.

The bytes below are what the servers wrote, captured by
`bench/marks_probe_pg.py` and `bench/marks_probe_my.py`.
"""
import pytest

from migkit import marks


class Side:
    """A side whose facts and proofs are given."""

    def __init__(self, family, facts, fails=()):
        self.CANON_ENGINE = family
        self.facts = dict(facts, family=family)
        self.fails = dict.fromkeys(fails, "refused on purpose")
        self.proved = []

    def mark_facts(self, side, db):
        return dict(self.facts)

    def mark_prove(self, side, db, rung):
        self.proved.append(rung)
        return self.fails.get(rung)


PG = {"version": 160000, "origin_grants": True, "foreign_origins": [],
      "can_create": True}
MY84 = {"mariadb": False, "version": (8, 4, 8), "gtid_mode": "ON",
        "rows_query": True, "grants": {"ALL PRIVILEGES",
                                       "TRANSACTION_GTID_TAG",
                                       "SYSTEM_VARIABLES_ADMIN"},
        "can_create": True}


def _names(ranked):
    return [r.name for r in ranked]


def test_a_hop_that_counts_takes_only_rungs_that_say_which_batch():
    ranked, dropped = marks.choose_rung(Side("postgres", PG),
                                        dict(PG, family="postgres",
                                             exact=True))
    assert set(_names(ranked)) == {"origin", "table"}
    assert [r.name for r, _ in dropped] == ["message"]
    ranked, _ = marks.choose_rung(Side("mysql", MY84),
                                  dict(MY84, family="mysql", exact=True))
    assert set(_names(ranked)) == {"gtid_tag", "table"}


def test_the_rungs_are_ranked_by_their_measured_cost(monkeypatch):
    monkeypatch.setattr(marks, "COST", {
        ("postgres", "origin"): (300.0, 1.0),
        ("postgres", "message"): (50.0, 1.0),
        ("postgres", "table"): (200.0, 1.0)})
    monkeypatch.setattr(marks, "TIE_US", {"postgres": 10.0})
    ranked, _ = marks.choose_rung(Side("postgres", PG),
                                  dict(PG, family="postgres"))
    assert _names(ranked) == ["message", "table", "origin"]


def test_what_a_rung_leaves_behind_only_breaks_a_tie(monkeypatch):
    monkeypatch.setattr(marks, "TIE_US", {"mysql": 30.0})
    # the table faster by more than the spread: it goes first, footprint
    # and all
    monkeypatch.setattr(marks, "COST", {
        ("mysql", "gtid_tag"): (200.0, 2.0),
        ("mysql", "comment"): (500.0, 2.0),
        ("mysql", "table"): (100.0, 2.0)})
    ranked, _ = marks.choose_rung(Side("mysql", MY84),
                                  dict(MY84, family="mysql"))
    assert _names(ranked) == ["table", "gtid_tag", "comment"]
    # within the spread: the one that leaves nothing on the server
    monkeypatch.setattr(marks, "COST", {
        ("mysql", "gtid_tag"): (120.0, 2.0),
        ("mysql", "comment"): (500.0, 2.0),
        ("mysql", "table"): (100.0, 2.0)})
    ranked, _ = marks.choose_rung(Side("mysql", MY84),
                                  dict(MY84, family="mysql"))
    assert _names(ranked) == ["gtid_tag", "table", "comment"]


def test_the_measured_costs_put_each_side_on_its_rung():
    """What `bench/marks_cost.py` measured decides, as the owner ruled:
    PostgreSQL 16's origin costs a connection a millisecond to take, so a
    hop that counts stands on the table there; MySQL 8.4 on its tagged
    GTID; MariaDB 11 on its flag."""
    def top(family, facts, exact):
        return _names(marks.choose_rung(Side(family, facts), dict(
            facts, family=family, exact=exact))[0])
    assert top("postgres", PG, True) == ["table", "origin"]
    assert top("postgres", PG, False)[0] == "message"
    assert top("mysql", MY84, True) == ["gtid_tag", "table"]
    assert top("mysql", MY84, False)[0] == "gtid_tag"
    maria = dict(MY84, mariadb=True, gtid_mode=None, rows_query=False)
    assert top("mysql", maria, False) == ["skip_flag", "table"]
    assert top("mysql", maria, True) == ["table"]


@pytest.mark.parametrize("facts,gone,why", [
    (dict(MY84, gtid_mode="OFF"), "gtid_tag", "gtid_mode is OFF"),
    (dict(MY84, version=(8, 0, 36)), "gtid_tag", "8.3 and later"),
    (dict(MY84, grants={"ALL PRIVILEGES", "SYSTEM_VARIABLES_ADMIN"}),
     "gtid_tag", "TRANSACTION_GTID_TAG"),
    (dict(MY84, rows_query=False), "comment", "changes no setting"),
    (dict(MY84, mariadb=True, gtid_mode=None), "gtid_tag", "MariaDB"),
])
def test_a_rung_the_side_does_not_allow_is_left_out_and_said(facts, gone,
                                                             why):
    ranked, dropped = marks.choose_rung(Side("mysql", facts),
                                        dict(facts, family="mysql"))
    assert gone not in _names(ranked)
    said = dict((r.name, w) for r, w in dropped)
    assert why in said[gone], said
    # the table stays: the bottom rung, where nothing higher is allowed
    assert "table" in _names(ranked)


def test_skip_replication_is_mariadbs_and_needs_no_global_grant():
    # measured on MariaDB 11.8: a user granted only its database sets it
    maria = dict(MY84, mariadb=True, gtid_mode=None, rows_query=False,
                 grants={"USAGE"})
    ranked, dropped = marks.choose_rung(Side("mysql", maria),
                                        dict(maria, family="mysql"))
    assert set(_names(ranked)) == {"skip_flag", "table"}
    assert {r.name for r, _ in dropped} == {"gtid_tag", "comment"}
    ranked, dropped = marks.choose_rung(Side("mysql", MY84),
                                        dict(MY84, family="mysql"))
    assert "skip_flag" not in _names(ranked)
    assert "MariaDB's" in dict((r.name, w) for r, w in dropped)["skip_flag"]


def test_another_origin_on_the_server_keeps_the_origin_rung_off():
    facts = dict(PG, foreign_origins=["pg_16391"])
    ranked, dropped = marks.choose_rung(Side("postgres", facts),
                                        dict(facts, family="postgres"))
    assert "origin" not in _names(ranked)
    assert "pg_16391" in dict((r.name, w) for r, w in dropped)["origin"]


def test_a_rung_that_does_not_prove_itself_falls_to_the_next():
    side = Side("postgres", PG)
    ranked, _ = marks.choose_rung(side, dict(PG, family="postgres"))
    side.fails = {ranked[0].name: "refused on purpose"}
    said = []
    got = marks.climb(side, "app", False, said.append)
    assert got == ranked[1].name
    assert side.proved == [ranked[0].name, ranked[1].name]
    assert any("did not prove itself" in m and "refused on purpose" in m
               for m in said), said


def test_nothing_proves_and_the_tail_stops_before_applying():
    side = Side("postgres", PG, fails=("origin", "message", "table"))
    with pytest.raises(SystemExit, match="no way of marking"):
        marks.climb(side, "app", False, print)


def test_a_token_from_before_the_rungs_stays_on_the_table(tmp_path):
    token = tmp_path / "tail-token.json"
    token.write_text('{"token": "0/16", "batch": 41}')
    assert marks.kept_rung(token) == "table"
    token.write_text('{"token": "0/16"}')
    assert marks.kept_rung(token) is None
    token.write_text('{"token": "0/16", "batch": 41, "rung": "origin"}')
    assert marks.kept_rung(token) == "origin"


def test_the_two_way_teardown_takes_away_what_the_rung_left(tmp_path,
                                                            monkeypatch):
    """`move --mode cdc --drop` on a two-way hop: the rung its token keeps
    names what its marks left on the target, dry-run first."""
    from migkit import cli
    from migkit.config import Endpoint, Hop
    from migkit.engines.hetero import HeteroEngine
    hop = Hop(name="m2p", engine="hetero", source=Endpoint(host="h", port=1),
              target=Endpoint(host="h", port=2), databases=["app"],
              options={"source_engine": "mysql", "target_engine": "postgres",
                       "two_way": {"on_conflict": "error"}})
    hop.report_dir = lambda db=None: tmp_path
    eng = HeteroEngine(hop)
    ran = []
    monkeypatch.setattr(type(eng.dst_engine), "_psql",
                        lambda self, side, db, sql: ran.append(sql) or "")
    said = []
    monkeypatch.setattr(cli.console, "print", lambda *a, **k: said.append(
        " ".join(str(x) for x in a)))
    (tmp_path / "tail-token.json").write_text(
        '{"token": "0/16", "batch": 7, "rung": "origin"}')
    cli._two_way_teardown(hop, eng, "app", False)
    assert any("pg_replication_origin_drop" in s for s in said), said
    assert ran == []
    cli._two_way_teardown(hop, eng, "app", True)
    assert ran and "migkit\\_twoway\\_m2p" in ran[0], ran
    # a token from before the rungs: the table was the rung
    ran.clear()
    (tmp_path / "tail-token.json").write_text('{"token": "0/16", "batch": 7}')
    cli._two_way_teardown(hop, eng, "app", True)
    assert ran == ["drop table if exists public.migkit_origin"]


def test_a_batch_numbers_as_an_origins_position_and_back():
    for n in (0, 1, 41, 2 ** 32 + 5, 2 ** 48 - 1):
        assert marks.number(marks.lsn(n)) == n


class Target:
    def __init__(self, committed):
        self.committed = committed

    def mark_committed(self, side, db, rung, batch):
        return batch in self.committed


def test_a_committed_batch_on_a_rung_without_its_end_stops_the_tail():
    from migkit import twoway
    got = twoway.committed_ahead(Target({42}), "app", 41, "origin",
                                 {"token": "0/99", "batch": 42})
    assert got == ("0/99", 42)
    assert twoway.committed_ahead(Target(set()), "app", 41, "origin",
                                  {"token": "0/99", "batch": 42}) is None
    # committed, and where it ended is not known here: never applied again
    with pytest.raises(SystemExit, match="no record of where"):
        twoway.committed_ahead(Target({42}), "app", 41, "gtid_tag", None)
    with pytest.raises(SystemExit, match="no record of where"):
        twoway.committed_ahead(Target({42}), "app", 41, "gtid_tag",
                               {"token": "x", "batch": 40})


def test_the_message_line_is_migkits_only_under_its_own_prefix():
    from migkit import pgslot
    line = "message: transactional: 1 prefix: migkit, sz: 5 content:hello"
    assert pgslot.mark_message(line) == "hello"
    assert pgslot.mark_message(line.replace("prefix: migkit,",
                                            "prefix: migkit_probe,")) is None
    assert pgslot.mark_message(line.replace("transactional: 1",
                                            "transactional: 0")) is None
    assert pgslot.mark_message("table public.t: INSERT: id[integer]:1") \
        is None


def test_pgoutputs_origin_and_message_as_postgresql_16_sent_them():
    from migkit import pgslot
    origin = b"O" + b"\x00\x00\x00\x00\x00\x00\x00*migkit_h1\x00"
    assert pgslot.pgoutput_origin(origin) == ("migkit_h1", "0/2A")
    message = (b"M" + b"\x01\x00\x00\x00\x00\x01Q\xa2Pmigkit\x00"
               b"\x00\x00\x00\x05hello")
    assert pgslot.pgoutput_message(message) == (True, "migkit", b"hello")
    assert pgslot.pgoutput_origin(message) is None


#: tagged GTID events as MySQL 8.4 wrote them, the GTIDs set by hand under
#: the probe's UUID: 5, 2**40 + 7 and 77 (an empty transaction)
TAGGED = [
    ("0278000000026d03f6b83503da4902a8463d02460201026d025d020e8103041406"
     "0c6d69676b697408080a0c0c7f192127f48c5c0610a50512dbd009", 5, 0),
    ("0282000000026d03f6b83503da4902a8463d02460201026d025d020e8103049f03"
     "00000080060c6d69676b697408080a140c7f2c4427f48c5c0610c10512dbd009",
     2 ** 40 + 7, 0),
    ("027a000002026d03f6b83503da4902a8463d02460201026d025d020e8103046902"
     "060c6d69676b697408080a180c7fa65d27f48c5c06108d0312dbd009", 77, 1),
]


@pytest.mark.parametrize("body,gno,flags", TAGGED)
def test_a_tagged_gtid_is_read_as_mysql_84_wrote_it(body, gno, flags):
    import uuid

    from migkit.binlog_marks import tagged_gtid
    got = tagged_gtid(bytes.fromhex(body))
    assert got == {"flags": flags, "gno": gno, "tag": "migkit",
                   "uuid": str(uuid.uuid5(uuid.NAMESPACE_URL,
                                          "migkit marks probe"))}


def test_a_tagged_gtid_that_does_not_add_up_is_refused_not_guessed():
    from migkit.binlog_marks import tagged_gtid
    body = bytes.fromhex(TAGGED[0][0])
    with pytest.raises(ValueError, match="not the layout"):
        tagged_gtid(body[:-3])


def test_the_highest_number_a_gtid_set_holds_for_a_tag():
    from migkit.binlog_marks import gtid_top
    import uuid
    uid = str(uuid.uuid5(uuid.NAMESPACE_URL, "a hop"))
    got = (f"3e11fa47-71ca-11e1-9e33-c80aa9429562:1-40,\n{uid}:5:77:migkit"
           f":5:77:300:1099511627783, {uid}:other:9000")
    assert gtid_top(got, uid, "migkit") == 1099511627783
    assert gtid_top(got, uid, "other") == 9000
    assert gtid_top(got, uid, "none") == 0
    assert gtid_top("", uid, "migkit") == 0
