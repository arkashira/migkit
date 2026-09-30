"""A faster way that needs a change on the source is offered, never
assumed and never hidden (the owner, 2026-09-30).

The servers are not asked here: each fact a probe reads is given. What
is held: where a change stands decides whether it is offered at all
(possible), handed to the DBA without a question (only a DBA can), or
said impossible without a question; the offer says what changes, that it
is taken off again, and what it saves; yes makes it behind the approvals
and takes it off after the work or on its failure, recorded; no, and no
answer, leave the source as it is and say what that means; the answer
given ahead in the hop, the old `physical.source_plugin: install` and a
terminal's recorded answer are one answer; the DBA's script and the
offer are written from one list. `test_a_clone_plugin_offer_on_mysql.py`
reads a real server.
"""
import pytest

from migkit import decide, dba, hopkeys, physical, sourcechange
from migkit.config import Endpoint, Hop

MY_OPTS = {"source": "10.0.0.1:3306", "restart": "ssh 10.0.0.2 restart"}


@pytest.fixture(autouse=True)
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path


def _hop(opts=None, **extra):
    return Hop(name="seed", engine="mysql",
               source=Endpoint(host="10.0.0.1", port=3306, user="app",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=3306, user="app",
                               password="CHANGE_ME"),
               options={"physical": dict(opts or MY_OPTS), **extra})


def _facts(hop, **over):
    f = physical.MyFacts(hop, None, "full")
    about = {"version": "8.4.2", "os": "Linux", "machine": "aarch64",
             "page": 16384, "files": "ibdata1:12M:autoextend",
             "packet": 64 * 1024 * 1024}
    f.update(options=hop.options["physical"], covers=True, managed_src="",
             src=dict(about), dst=dict(about), not_innodb=[], dst_tables=[],
             src_backup_admin=True, dst_clone_admin=True, src_plugin="",
             dst_plugin="ACTIVE", log_bin=True, gtid_mode="ON",
             src_users={"app"}, plugin_rights=True, dynamic_loading=True,
             src_active={"InnoDB"}, dst_active={"InnoDB", "clone"})
    f.update(over)
    return f


def _clone():
    return next(r for r in physical.rungs("mysql") if r.name ==
                "physical-clone")


# --- where a change stands ---------------------------------------------------

@pytest.mark.parametrize("over,state,words", [
    ({"src_plugin": "ACTIVE"}, decide.PRESENT, ""),
    ({}, decide.POSSIBLE, "may put it on and take it off"),
    ({"plugin_rights": False}, decide.BY_DBA,
     "INSERT and DELETE on mysql.plugin"),
    ({"src_plugin": "DISABLED"}, decide.BY_DBA, "disabled"),
    ({"dynamic_loading": False}, decide.IMPOSSIBLE, "loads no plugin"),
    ({"src": {"version": "10.11.6-MariaDB"}}, decide.IMPOSSIBLE,
     "which has no clone plugin"),
    ({"plugin_rights": physical.Unread("grants could not be read: denied")},
     decide.BY_DBA, "grants could not be read: denied"),
])
def test_where_the_clone_plugin_stands_is_read_not_guessed(over, state,
                                                           words):
    hop = _hop()
    facts = _facts(hop)
    for k, v in over.items():
        facts[k] = {**facts[k], **v} if isinstance(v, dict) else v
    got = physical.CLONE_PLUGIN.stand(facts)
    assert got.state == state, got
    assert words in got.why, got


def test_an_address_that_names_a_service_is_a_hint_not_the_proof():
    """The address says RDS; the server's own answers say nothing of it:
    the probes decide, and the hint is said beside them."""
    from migkit.clouds import Where
    hop = _hop()
    facts = _facts(hop, where=Where("aws", "rds",
                                    ["the address (an RDS instance name)"]))
    got = physical.CLONE_PLUGIN.stand(facts)
    assert got.state == decide.POSSIBLE, got
    assert "its address names Amazon RDS" in got.why, got
    # the server saying it is RDS refuses the clone as before
    facts = _facts(hop, where=Where("aws", "rds", ["the server (it runs"
                                                   " from /rdsdbbin/)"]))
    facts.pop("managed_src")
    assert facts["managed_src"] == "Amazon RDS"
    assert "cloud: ..." in _clone().refused(facts)


def test_a_refusal_the_server_gave_is_the_answer_from_then_on(reports):
    hop = _hop()
    rec = {"refused": {"clone_plugin": {
        "why": "(1227, 'Access denied')", "at": "2026-09-30 10:00:00",
        "source": "10.0.0.1:3306", "version": "8.4.2"}}}
    sourcechange._save(hop, rec)
    got = physical.CLONE_PLUGIN.stand(_facts(hop))
    assert got.state == decide.IMPOSSIBLE and "Access denied" in got.why
    # another version of the server is asked again
    got = physical.CLONE_PLUGIN.stand(_facts(hop, src={
        "version": "8.4.3", "os": "Linux", "machine": "aarch64",
        "page": 16384, "files": "ibdata1:12M:autoextend",
        "packet": 64 * 1024 * 1024}))
    assert got.state == decide.POSSIBLE, got


# --- the climb and the offer -------------------------------------------------

def test_a_possible_change_is_never_taken_unanswered():
    hop = _hop()
    why = _clone().refused(_facts(hop))
    assert why and "offered, never assumed" in why, why
    assert "source_changes: {clone_plugin: yes | no | dba}" in why
    assert "INSTALL PLUGIN clone SONAME 'mysql_clone.so'" in why
    facts = _facts(hop)
    facts[decide.answer_key("clone_plugin")] = "yes"
    assert _clone().refused(facts) is None
    facts[decide.answer_key("clone_plugin")] = "no"
    assert "the answer is no" in _clone().refused(facts)


def test_the_offer_says_the_way_with_and_the_way_without():
    hop = _hop()
    facts = _facts(hop)
    got = decide.offered("the seed", [_clone()], facts)
    assert got is not None and got.rung.name == "physical-clone"
    assert [c.name for c in got.changes] == ["clone_plugin"]
    assert got.instead.rung is None
    # the facts are as they were: offering answers nothing
    assert decide.answer_key("clone_plugin") not in facts
    # nothing to offer where it is there, answered, or not possible
    for over in ({"src_plugin": "ACTIVE"}, {"plugin_rights": False},
                 {"dynamic_loading": False}):
        assert decide.offered("the seed", [_clone()],
                              _facts(hop, **over)) is None, over
    answered = _facts(hop)
    answered[decide.answer_key("clone_plugin")] = "no"
    assert decide.offered("the seed", [_clone()], answered) is None


def test_a_rung_out_for_another_reason_asks_no_probe():
    """A target that holds tables takes the clone out before its change
    is asked of the server."""
    hop = _hop()
    facts = _facts(hop, dst_tables=["app.t"])
    facts.pop("plugin_rights")
    assert decide.offered("the seed", [_clone()], facts) is None
    assert "plugin_rights" not in facts and "change clone_plugin" not in \
        facts


def test_the_offer_in_words_saves_minutes_and_the_source(monkeypatch):
    hop = _hop()
    facts = _facts(hop)
    off = decide.offered("the seed", [_clone()], facts)
    saving = sourcechange.Saving(faster=60, slower=900, how="estimated",
                                 instead="a logical copy",
                                 spared="every row read one by one")
    text = "\n".join(sourcechange.offer_lines(off, facts, saving))
    for words in ("offered, never assumed",
                  "INSTALL PLUGIN clone SONAME 'mysql_clone.so'",
                  "taken off again after the copy (UNINSTALL PLUGIN clone)",
                  "it saves about 14 minutes", "about 15 minutes for a"
                  " logical copy (estimated)",
                  "the source is spared every row read one by one",
                  "yes - migkit makes it", "no - the source is left",
                  "dba - the statements go to the DBA's script",
                  "source_changes: {clone_plugin: yes | no | dba}"):
        assert words in text, (words, text)


def test_the_saving_is_measured_or_estimated_and_said_which(monkeypatch):
    from migkit import planner

    class Eng:
        def databases(self):
            return ["app"]
    monkeypatch.setattr(physical, "_rows", lambda eng, dbs: 7_150_000)
    hop = _hop()
    got = physical.saving(hop, Eng(), _clone())
    assert round(got.faster, 1) == 5.3 and round(got.slower, 1) == 77.7
    assert "estimated from migkit's own measurement" in got.how, got.how
    # the logical copy's rate as the resource model's probes measured it
    monkeypatch.setattr(physical, "_modelled_rate",
                        lambda hop, dbs: 7_150_000 / 50)
    got = physical.saving(hop, Eng(), _clone())
    assert round(got.slower) == 50, got
    assert "a logical copy from the resource model's probes" in got.how
    planner.record_rate(hop, "physical-clone", 7_150_000, 10)
    planner.record_rate(hop, "mydumper", 7_150_000, 100)
    got = physical.saving(hop, Eng(), _clone())
    assert round(got.faster) == 10 and round(got.slower) == 100
    assert got.how.startswith("measured on this hop"), got.how


# --- the answer --------------------------------------------------------------

@pytest.mark.parametrize("opts,extra,want", [
    ({**MY_OPTS, "source_plugin": "install"}, {}, "yes"),
    (MY_OPTS, {"source_changes": {"clone_plugin": True}}, "yes"),
    (MY_OPTS, {"source_changes": {"clone_plugin": False}}, "no"),
    (MY_OPTS, {"source_changes": "dba"}, "dba"),
    (MY_OPTS, {"source_changes": False}, "no"),
    (MY_OPTS, {}, None),
])
def test_one_answer_however_the_hop_gives_it(opts, extra, want):
    hop = _hop(opts, **extra)
    assert sourcechange.answer_of(hop, "clone_plugin")[0] == want


def test_the_hops_own_answer_overrides_a_terminals(reports):
    hop = _hop()
    off = decide.offered("the seed", [_clone()], _facts(hop))
    sourcechange.record_answer(hop, off.changes, "yes")
    got, where = sourcechange.answer_of(hop, "clone_plugin")
    assert got == "yes" and "answered at a terminal" in where
    hop = _hop(source_changes={"clone_plugin": "no"})
    assert sourcechange.answer_of(hop, "clone_plugin") == (
        "no", "the hop's source_changes")


def test_the_hop_option_is_declared_and_checked():
    assert "source_changes" in hopkeys.HOP_OPTIONS
    base = {"source": "mysql://app@m1/shop", "target": "mysql://app@m2/shop"}

    def problems(opts, goal=None):
        raw = {**base, "options": opts, **({"goal": goal} if goal else {})}
        return hopkeys.check({"hops": {"h": raw}})
    assert not [p for p in problems({"source_changes": {"clone_plugin":
                                                        "yes"}})
                if p.level == "error"]
    bad = problems({"source_changes": "maybe"})
    assert any(p.level == "error" and "yes, no or dba" in p.text
               for p in bad), bad
    near = problems({"source_changes": {"clone_plugn": "yes"}})
    assert any("did you mean clone_plugin" in p.text for p in near), near
    verify = problems({"source_changes": "yes"}, goal="verify")
    assert any(p.level == "error" and "writes to neither side" in p.text
               for p in verify), verify
    assert not [p for p in problems({"source_changes": "no"}, "verify")
                if p.level == "error"]


# --- yes: made, recorded, taken off ------------------------------------------

class _Server:
    """The source's statements, as run; `refuse` the error one raises."""

    def __init__(self, refuse=None, undo_fails=None):
        self.ran, self.refuse, self.undo_fails = [], refuse, undo_fails

    def run(self, stmt):
        self.ran.append(stmt)
        if self.refuse and stmt.startswith("INSTALL"):
            raise self.refuse
        if self.undo_fails and stmt.startswith("UNINSTALL"):
            raise self.undo_fails


def _states(hop):
    return [e["state"] for e in sourcechange._record(hop).get("made", [])]


def test_yes_is_made_behind_the_approvals_and_taken_off_after(
        reports, monkeypatch):
    from migkit import approvals
    hop = _hop()
    asked = []
    monkeypatch.setattr(approvals, "require",
                        lambda h, step, db, log=None, detail=None:
                        asked.append((step, detail)) or [])
    server, said = _Server(), []
    with sourcechange.making(hop, physical.CLONE_PLUGIN, _facts(hop),
                             server.run, "app", said.append):
        assert server.ran == ["INSTALL PLUGIN clone SONAME"
                              " 'mysql_clone.so'"]
        assert _states(hop) == ["made"]
    assert server.ran[-1] == "UNINSTALL PLUGIN clone"
    assert _states(hop) == ["undone"]
    assert asked == [("source-setting", {
        "change": "clone_plugin",
        "make": ["INSTALL PLUGIN clone SONAME 'mysql_clone.so'"],
        "undo": ["UNINSTALL PLUGIN clone"]})]
    assert any("taken off again" in m for m in said), said


def test_the_change_is_taken_off_when_the_work_fails(reports):
    hop = _hop()
    server = _Server()
    with pytest.raises(SystemExit, match="clone failed"):
        with sourcechange.making(hop, physical.CLONE_PLUGIN, _facts(hop),
                                 server.run, "app", lambda m: None):
            raise SystemExit("clone failed")
    assert server.ran == ["INSTALL PLUGIN clone SONAME 'mysql_clone.so'",
                          "UNINSTALL PLUGIN clone"]
    assert _states(hop) == ["undone"]


def test_a_refusal_is_recorded_and_nothing_is_taken_off(reports):
    import pymysql
    hop = _hop()
    server = _Server(refuse=pymysql.err.OperationalError(
        1126, "Can't open shared library 'mysql_clone.so'"))
    with pytest.raises(sourcechange.Refused, match="shared library"):
        with sourcechange.making(hop, physical.CLONE_PLUGIN, _facts(hop),
                                 server.run, "app", lambda m: None,
                                 version="8.4.2"):
            pytest.fail("the block ran over a refused change")
    # nothing was made, so nothing is taken off
    assert server.ran == ["INSTALL PLUGIN clone SONAME 'mysql_clone.so'"]
    assert _states(hop) == ["not made"]
    assert sourcechange.refused_here(hop, "clone_plugin", "8.4.2")
    assert isinstance(sourcechange.Refused("x"), decide.Blocked)


def test_a_connection_lost_is_not_a_refusal(reports):
    import pymysql
    hop = _hop()
    server = _Server(refuse=pymysql.err.OperationalError(2013, "Lost"))
    with pytest.raises(pymysql.err.OperationalError):
        with sourcechange.making(hop, physical.CLONE_PLUGIN, _facts(hop),
                                 server.run, "app", lambda m: None):
            pass
    assert sourcechange.refused_here(hop, "clone_plugin", "8.4.2") is None


def test_a_change_a_killed_run_left_is_taken_off_first(reports):
    hop = _hop()
    entry = {"id": "clone_plugin-1", "change": "clone_plugin",
             "said": "the clone plugin", "state": "made",
             "make": ["INSTALL PLUGIN clone SONAME 'mysql_clone.so'"],
             "undo": ["UNINSTALL PLUGIN clone"], "source": "10.0.0.1:3306",
             "db": "app", "at": "2026-09-30 10:00:00"}
    sourcechange._save(hop, {"made": [entry]})
    assert sourcechange.said_by_doctor(hop) and "not taken off yet" in \
        sourcechange.said_by_doctor(hop)[0]
    server = _Server()
    assert sourcechange.undo_left(hop, server.run, lambda m: None)
    assert server.ran == ["UNINSTALL PLUGIN clone"]
    assert _states(hop) == ["undone"]
    assert sourcechange.said_by_doctor(hop) == []


def test_an_undo_the_server_refuses_is_left_said_and_retried(reports):
    import pymysql
    hop = _hop()
    server = _Server(undo_fails=pymysql.err.OperationalError(
        1227, "Access denied"))
    said = []
    with sourcechange.making(hop, physical.CLONE_PLUGIN, _facts(hop),
                             server.run, "app", said.append):
        pass
    assert _states(hop) == ["left"]
    assert any("could not be taken off" in m for m in said), said
    assert sourcechange.left_on_source(hop)
    # the next change is not made over it
    with pytest.raises(SystemExit, match="could not be taken off"):
        with sourcechange.making(hop, physical.CLONE_PLUGIN, _facts(hop),
                                 server.run, "app", lambda m: None):
            pass


def test_an_undo_that_finds_nothing_counts_as_taken_off(reports):
    import pymysql
    hop = _hop()
    server = _Server(undo_fails=pymysql.err.OperationalError(
        1305, "PLUGIN clone does not exist"))
    with sourcechange.making(hop, physical.CLONE_PLUGIN, _facts(hop),
                             server.run, "app", lambda m: None):
        pass
    assert _states(hop) == ["undone"]


# --- the seed, end to end with the servers' answers given ------------------

class _Eng:
    def __init__(self):
        self.ran = []

    def databases(self):
        return ["app"]

    def _q(self, side, sql):
        self.ran.append((side, sql))
        return []


def _seed(monkeypatch, hop, facts, cloned=True):
    monkeypatch.setattr(physical, "facts_for", lambda h, e, m: facts)
    monkeypatch.setattr(physical, "_rows", lambda eng, dbs: 7_150_000)
    got = []

    def clone(*a):
        if not cloned:
            raise decide.Blocked("the target could not clone the source:"
                                 " refused")
        raise SystemExit("cloned")
    monkeypatch.setattr(physical, "_my_clone", clone)
    return got


def test_the_dry_run_offers_and_changes_nothing(monkeypatch):
    hop = _hop()
    facts = _facts(hop)
    said = _seed(monkeypatch, hop, facts)
    eng = _Eng()
    asked = []
    assert physical.carry(hop, eng, "app", "full", False, said.append,
                          ask=asked.append) is False
    text = "\n".join(said)
    assert "offered, never assumed" in text and "it saves about" in text
    assert "dry-run: --go at a terminal asks it" in text
    assert "without it: a logical copy" in text, text
    assert asked == [] and eng.ran == []


def test_yes_at_a_terminal_puts_it_on_for_the_clone_only(monkeypatch):
    hop = _hop()
    facts = _facts(hop)
    said = _seed(monkeypatch, hop, facts)
    eng = _Eng()
    with pytest.raises(SystemExit, match="cloned"):
        physical.carry(hop, eng, "app", "full", True, said.append,
                       ask=lambda q: "yes")
    src = [s for side, s in eng.ran if side == "src"]
    assert src == ["INSTALL PLUGIN clone SONAME 'mysql_clone.so'",
                   "UNINSTALL PLUGIN clone"], eng.ran
    assert sourcechange.answer_of(hop, "clone_plugin")[0] == "yes"


def test_no_leaves_the_source_and_says_what_that_means(monkeypatch):
    hop = _hop()
    facts = _facts(hop)
    said = _seed(monkeypatch, hop, facts)
    eng = _Eng()
    assert physical.carry(hop, eng, "app", "full", True, said.append,
                          ask=lambda q: "no") is False
    assert [s for side, s in eng.ran if side == "src"] == []
    text = "\n".join(said)
    assert "the answer is no" in text and "without it: a logical copy" in \
        text, text
    assert sourcechange.answer_of(hop, "clone_plugin")[0] == "no"
    # recorded: the next run does not ask again
    asked = []
    said.clear()
    physical.carry(hop, eng, "app", "full", True, said.append,
                   ask=asked.append)
    assert asked == []


def test_no_terminal_and_no_answer_changes_nothing(monkeypatch):
    hop = _hop()
    said = _seed(monkeypatch, hop, _facts(hop))
    eng = _Eng()
    assert physical.carry(hop, eng, "app", "full", True, said.append) is \
        False
    assert [s for side, s in eng.ran if side == "src"] == []
    assert any("not answered" in m for m in said), said
    assert sourcechange.answer_of(hop, "clone_plugin")[0] is None


@pytest.mark.parametrize("over", [{"dynamic_loading": False},
                                  {"src": {"version": "8.0.16"}}])
def test_impossible_is_said_without_a_question(monkeypatch, over):
    hop = _hop()
    facts = _facts(hop)
    for k, v in over.items():
        facts[k] = {**facts[k], **v} if isinstance(v, dict) else v
    said = _seed(monkeypatch, hop, facts)

    def never(q):
        raise AssertionError("asked where nothing could be done")
    assert physical.carry(hop, _Eng(), "app", "full", True, said.append,
                          ask=never) is False
    assert any("the physical copy is not taken" in m for m in said), said


def test_only_a_dba_can_is_handed_over_without_a_question(monkeypatch):
    hop = _hop()
    said = _seed(monkeypatch, hop, _facts(hop, plugin_rights=False))

    def never(q):
        raise AssertionError("asked where only a DBA can")
    assert physical.carry(hop, _Eng(), "app", "full", True, said.append,
                          ask=never) is False
    text = "\n".join(said)
    assert "only a DBA can make here" in text and "without it:" in text
    needs = sourcechange.for_the_dba(hop)
    assert len(needs) == 1 and needs[0].ok is False
    assert needs[0].fix == ["INSTALL PLUGIN clone SONAME 'mysql_clone.so';"]
    assert any("UNINSTALL PLUGIN clone" in line for line in needs[0].outside)
    assert "logical copy" in needs[0].without
    # once the plugin is there, it is taken out of the DBA's script
    _seed(monkeypatch, hop, _facts(hop, src_plugin="ACTIVE"))
    with pytest.raises(SystemExit, match="cloned"):
        physical.carry(hop, _Eng(), "app", "full", True, lambda m: None)
    assert sourcechange.for_the_dba(hop) == []


def test_the_old_option_is_the_same_yes(monkeypatch):
    hop = _hop({**MY_OPTS, "source_plugin": "install"})
    said = _seed(monkeypatch, hop, _facts(hop))
    eng = _Eng()
    with pytest.raises(SystemExit, match="cloned"):
        physical.carry(hop, eng, "app", "full", True, said.append)
    assert [s for side, s in eng.ran if side == "src"] == [
        "INSTALL PLUGIN clone SONAME 'mysql_clone.so'",
        "UNINSTALL PLUGIN clone"]


def test_a_clone_the_target_refuses_switches_to_the_logical_copy(
        monkeypatch):
    hop = _hop({**MY_OPTS, "source_plugin": "install"})
    said = _seed(monkeypatch, hop, _facts(hop), cloned=False)
    eng = _Eng()
    assert physical.carry(hop, eng, "app", "full", True, said.append) is \
        False
    # the plugin was taken off again, and the switch said
    assert [s for side, s in eng.ran if side == "src"][-1] == \
        "UNINSTALL PLUGIN clone"
    assert any("switching to a logical copy" in m for m in said), said


# --- the DBA's script and the offer: one list --------------------------------

def test_the_dbas_statements_are_the_offers():
    from migkit.clouds import Where
    ctx = sourcechange.Ctx(who="'app'@'%'", where=Where())
    assert sourcechange.statements("binlog_format", ctx) == [
        "set persist binlog_format = 'ROW';"]
    rds = sourcechange.Ctx(who="app", where=Where("aws", "rds"))
    assert sourcechange.statements("replication_role", rds) == [
        "grant rds_replication to app;"]
    assert "DB parameter group: rds.logical_replication = 1" in \
        sourcechange.outside("wal_level", rds)[0]
    got = sourcechange.requirement("change_tracking", "off",
                                   sourcechange.Ctx(db="shop"))
    assert "alter database [shop] set change_tracking = on" in got
    assert "cannot be followed" in got
    # every definition is written for an engine the DBA's script is for
    for d in sourcechange.DEFINITIONS.values():
        assert d.engine in dba.SCRIPT, d.name


def test_the_dba_script_says_what_not_having_it_means():
    need = dba.Need("source", "a binary log", False, "log_bin is OFF",
                    ["-- nothing here"], [], True,
                    without=sourcechange.consequence("log_bin"))

    class E:
        CANON_ENGINE = "mysql"
    hop = _hop()
    text = dba.script(hop, E(), "source", [need])
    assert "cannot be followed" in text and "takes a restart" in text
