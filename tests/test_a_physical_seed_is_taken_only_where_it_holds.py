"""A physical seed (R19 lever 1) is a rung: taken only where every need
holds, proved before it is trusted, left where this hop measured it
slower, refused with the reason where the source or the hop blocks it -
and never reached at all by a hop that does not ask for it.

The servers are not asked here: each fact a need reads is given, one at a
time made to fail, and every refusal is held to the words that tell the
operator what to do. The rungs running against real servers are
`test_a_physical_seed_goes_on_exactly_pg.py` and
`test_a_clone_goes_on_exactly_mysql.py`.
"""
import subprocess

import pytest

from migkit import physical
from migkit.config import Endpoint, Hop

PG_OPTS = {"data": "/var/lib/postgresql/data",
           "run": "ssh postgres@10.0.0.2", "stop": "ssh 10.0.0.2 stop",
           "start": "ssh 10.0.0.2 start", "source": "10.0.0.1:5432"}


def _hop(engine="postgres", physical_opts=None, **kw):
    opts = {} if physical_opts is None else {"physical": physical_opts}
    return Hop(name="seed", engine=engine,
               source=Endpoint(host="10.0.0.1", port=5432, user="app",
                               password="CHANGE_ME"),
               target=Endpoint(host="10.0.0.2", port=5432, user="app",
                               password="CHANGE_ME"),
               options=opts, **kw)


@pytest.fixture(autouse=True)
def reports(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    return tmp_path


def _pg_facts(hop, mode="full+cdc", **over):
    f = physical.PgFacts(hop, None, mode)
    f.update(options=hop.options["physical"], covers=True, managed_src="",
             src_version=160004, dst_version=160002, dst_tables=[],
             src_subscriptions=0, replication_role=True, sender_room=8,
             slot_room=8, slots_wanted=2, wal_level="logical",
             src_users={"app"}, src_system="7400000000000000001",
             checksums="on", unlogged=[], tablespaces=[])
    f.update(over)
    return f


def _rung(name):
    return next(r for r in physical.rungs() if r.name == name)


PAGES = "physical-pages"
CLONE = "physical-clone"


def test_a_hop_that_does_not_ask_never_reaches_a_server():
    """No `physical:` - the move is what it was. The engine answers
    nothing: any question put to it would fail this test."""
    class Untouchable:
        def __getattr__(self, name):
            raise AssertionError(f"asked the engine for {name}")
    hop = _hop()
    assert physical.choose(hop, Untouchable(), "full+cdc") == (None, "")
    said = []
    assert physical.carry(hop, Untouchable(), "", "full+cdc",
                          True, said.append) is False
    assert said == []


def test_the_way_back_never_seeds_the_old_source():
    """A stream back runs on the hop reversed; a physical seed there would
    empty the old source's data directory."""
    hop = _hop(physical_opts=PG_OPTS)
    assert physical.settings(hop)
    assert physical.settings(hop.reversed()) is None


def test_every_need_holding_takes_the_pages():
    hop = _hop(physical_opts=PG_OPTS)
    assert _rung(PAGES).refused(_pg_facts(hop)) is None


@pytest.mark.parametrize("fact,value,words", [
    ("src_version", 150007, "PostgreSQL 15 and the target 16"),
    ("dst_tables", ["app.public.orders", "app.public.lines"],
     "the target holds 2 tables (app.public.orders, app.public.lines)"),
    ("managed_src", "Amazon RDS", "cloud: ..."),
    ("src_subscriptions", 2, "subscribes to 2 publications"),
    ("replication_role", False, "alter role app replication"),
    ("slot_room", 1, "1 more slots"),
    ("sender_room", 1, "room for 1 more connections"),
    ("wal_level", "replica", "wal_level is replica"),
    ("src_users", {"postgres"}, "app is not one of the source's"),
    ("unlogged", ["app.public.cache"],
     "app.public.cache are unlogged: their rows are not in the log"),
    ("tablespaces", ["fast"], "tablespaces fast outside its data"),
    ("covers", "the hop excludes app.audit, and a copy of the pages carries"
               " every table", "the hop excludes app.audit"),
])
def test_each_need_that_fails_is_said(fact, value, words):
    hop = _hop(physical_opts=PG_OPTS)
    facts = _pg_facts(hop, **{fact: value})
    if fact == "src_users":
        facts.pop("target_signs_in", None)
    why = _rung(PAGES).refused(facts)
    assert why and words in why, why


def test_a_fact_that_could_not_be_read_refuses_rather_than_guesses():
    hop = _hop(physical_opts=PG_OPTS)
    facts = _pg_facts(hop, replication_role=physical.Unread(
        "replication role could not be read: permission denied"))
    why = _rung(PAGES).refused(facts)
    assert "permission denied" in why and "a user that may open a" \
        " replication connection is needed" in why, why


def test_a_full_copy_does_not_need_logical_decoding():
    """Only going on from the copy reads a logical slot."""
    hop = _hop(physical_opts=PG_OPTS)
    assert _rung(PAGES).refused(_pg_facts(hop, "full",
                                          wal_level="replica")) is None
    assert "wal_level" in _rung(PAGES).refused(
        _pg_facts(hop, "full+cdc", wal_level="replica"))


def test_a_hop_without_the_commands_is_told_which():
    opts = dict(PG_OPTS)
    del opts["stop"], opts["run"]
    hop = _hop(physical_opts=opts)
    why = _rung(PAGES).refused(_pg_facts(hop))
    assert "run, stop" in why, why


def test_what_the_hop_covers(reports):
    """A copy of the pages cannot leave a table out, rename a database or
    filter a row, and it carries every database on the server."""
    cases = [
        (dict(exclude=["app.audit"]), "the hop excludes app.audit"),
        (dict(mapping={"where": {"orders": "id > 5"}}), "filters rows"),
        (dict(mapping={"columns": {"orders": {"a": "b"}}}), "maps tables"),
        (dict(db_map={"app": "app2"}), "renames app to app2"),
        (dict(databases=["app"]), "also holds tables in other"),
        # a hop naming no database follows every one but the maintenance
        # database, whose tables would arrive and never change again
        (dict(), "also holds tables in postgres"),
    ]
    for kw, words in cases:
        hop = _hop(physical_opts=PG_OPTS, **kw)
        f = physical.PgFacts(hop, None, "full")
        f["src_databases"] = ["app", "other", "postgres"] if not kw else [
            "app", "other"]
        f["followed"] = hop.databases or ["app", "other"]
        got = f["covers"]
        assert got is not True and words in got, (kw, got)
    f = physical.PgFacts(_hop(physical_opts=PG_OPTS,
                              databases=["app", "other"]), None, "full")
    f.update(src_databases=["app", "other"], followed=["app", "other"])
    assert f["covers"] is True


def _proved(monkeypatch, out, rc=0):
    seen = {}

    def fake(opts, script, secret="", timeout=None):
        seen["script"], seen["secret"] = script, secret
        return subprocess.CompletedProcess([], rc, out, "")
    monkeypatch.setattr(physical, "_on_target", fake)
    return seen


def test_the_proof_asks_the_targets_host_for_the_source(monkeypatch):
    hop = _hop(physical_opts=PG_OPTS)
    seen = _proved(monkeypatch, "pg_basebackup (PostgreSQL) 16.4\n"
                                "7400000000000000001|1|0/3000148|\n")
    assert physical._pg_prove(_pg_facts(hop)) is None
    # the password on standard input, never on the command line
    assert seen["secret"] == "CHANGE_ME"
    assert "CHANGE_ME" not in seen["script"]
    assert "host=10.0.0.1 port=5432" in seen["script"], seen["script"]


@pytest.mark.parametrize("out,words", [
    ("pg_basebackup (PostgreSQL) 15.8\n7400000000000000001|1|0/1|\n",
     "version 15, the source 16"),
    ("pg_basebackup (PostgreSQL) 16.4\n7999999999999999999|1|0/1|\n",
     "not the source's"),
])
def test_a_proof_that_fails_says_why(monkeypatch, out, words):
    hop = _hop(physical_opts=PG_OPTS)
    _proved(monkeypatch, out)
    why = physical._pg_prove(_pg_facts(hop))
    assert why and words in why, why


def test_a_proof_that_cannot_connect_is_said(monkeypatch):
    hop = _hop(physical_opts=PG_OPTS)
    monkeypatch.setattr(physical, "_on_target",
                        lambda *a, **k: subprocess.CompletedProcess(
                            [], 2, "", "pg_basebackup: error: FATAL: no"
                                       " pg_hba.conf entry for replication"
                                       " connection"))
    why = physical._pg_prove(_pg_facts(hop))
    assert "no pg_hba.conf entry for replication" in why, why
    # the program's name does not reach the operator
    assert "pg_basebackup" not in why, why


def _choose(monkeypatch, hop, facts, proved=True):
    monkeypatch.setattr(physical, "facts_for", lambda h, e, m: facts)
    # the rungs are built on each call, so they take the proof given here
    monkeypatch.setattr(physical, "_pg_prove",
                        lambda f: None if proved else "no route")
    return physical.choose(hop, None, "full+cdc")


def test_chosen_where_it_holds_and_proves(monkeypatch):
    hop = _hop(physical_opts=PG_OPTS)
    rung, why = _choose(monkeypatch, hop, _pg_facts(hop))
    assert rung is not None and rung.name == PAGES, why
    rung, why = _choose(monkeypatch, hop, _pg_facts(hop), proved=False)
    assert rung is None and "did not prove itself: no route" in why, why


def test_measured_slower_on_this_hop_it_is_left(monkeypatch):
    from migkit import planner
    hop = _hop(physical_opts=PG_OPTS)
    # never measured: tried, so it gets its number
    planner.record_rate(hop, "pgcopydb", 1_000_000, 10)
    rung, _ = _choose(monkeypatch, hop, _pg_facts(hop))
    assert rung is not None
    # measured slower than the logical copy: left, and said
    planner.record_rate(hop, PAGES, 1_000_000, 20)
    rung, why = _choose(monkeypatch, hop, _pg_facts(hop))
    assert rung is None and "measured slower" in why, why
    assert "50,000 rows a second against 100,000" in why, why
    # measured faster: taken
    planner.record_rate(hop, PAGES, 1_000_000, 4)
    rung, _ = _choose(monkeypatch, hop, _pg_facts(hop))
    assert rung is not None and rung.name == PAGES


def test_a_cloud_the_hop_names_is_not_the_local_copy(monkeypatch):
    hop = _hop(physical_opts={**PG_OPTS, "cloud": "nowhere"})
    rung, why = physical.choose(hop, None, "full")
    assert rung is None and "through nowhere is not built" in why, why


def test_the_copy_empties_only_a_data_directory():
    script = physical._pg_script(_hop(physical_opts=PG_OPTS), PG_OPTS, True)
    assert "not a data directory, left alone" in script
    assert '[ ! -f "$D/PG_VERSION" ]' in script
    assert "-Xstream" in script and "--checkpoint=fast" in script
    assert "--compress=server-zstd" in script
    assert "--manifest-checksums=SHA256" in script
    assert 'pg_verifybackup -q "$D"' in script
    # the password is read from standard input, never written here
    assert "CHANGE_ME" not in script and "read -r PGPASSWORD" in script
    assert "--compress" not in physical._pg_script(
        _hop(physical_opts=PG_OPTS), PG_OPTS, False)


def test_how_the_pages_travel_is_chosen_then_measured():
    """A near source sends them as they are first, a far one compresses
    them first; once both are timed on this hop, the faster goes; a source
    before 15 cannot compress at all."""
    from migkit import decide
    hop = _hop(physical_opts=PG_OPTS)
    near, far = 0.0003, 0.040
    assert physical.compression(hop, 160004, near)[0] is physical.PLAIN
    assert physical.compression(hop, 160004, far)[0] is physical.ZSTD
    way, why = physical.compression(hop, 140010, far)
    assert way is physical.PLAIN and "only from PostgreSQL 15" in why, why
    kept = decide.HopCosts(hop, "physical-pages ")
    kept.saw("as they are", 1_000_000, 10)
    # the other untried: it gets its number next
    assert physical.compression(hop, 160004, near)[0] is physical.ZSTD
    kept.saw("zstd", 1_000_000, 18)
    assert physical.compression(hop, 160004, far)[0] is physical.PLAIN
    kept.saw("zstd", 1_000_000, 4)
    assert physical.compression(hop, 160004, near)[0] is physical.ZSTD
    # measured under the pages rung, never read as a logical copy's rate
    assert physical._logical_rate(hop) == (None, None)


def test_the_slot_is_made_the_one_way_before_the_copy(monkeypatch):
    """The publication, then the slot through the engine's one slot maker
    (its snapshot, and the wait for what is out of sight) with the plugin
    the subscription reads - and all of it before the copy is asked for."""
    hop = _hop(physical_opts=PG_OPTS)
    ran = []

    class Eng:
        def databases(self):
            return ["app", "crm"]

        def replicate_sql(self, db, copy_data=True):
            return {"src": [f"create publication for {db}"]}

        def apply_replication_stmt(self, side, db, stmt):
            ran.append(("stmt", side, stmt))

        def _psql(self, side, db, sql):
            return ""

        def _slot_with_its_snapshot(self, side, db, name, plugin=None):
            ran.append(("slot", side, db, name, plugin))

        _repl_name = staticmethod(lambda: "migkit_seed")

    monkeypatch.setattr(physical, "facts_for",
                        lambda h, e, m: _pg_facts(hop))

    def copy(*a):
        ran.append(("copy",))
        raise SystemExit("stopped here: the order is what is asserted")
    monkeypatch.setattr(physical, "_pg_copy", copy)
    monkeypatch.setattr(physical, "_pg_drop_slots", lambda e, s: None)
    eng = Eng()
    eng.hop = hop
    with pytest.raises(SystemExit):
        physical._pg_carry(hop, eng, _rung(PAGES), "", "full+cdc", True,
                           lambda m: None)
    assert ran == [
        ("stmt", "src", "create publication for app"),
        ("slot", "src", "app", "migkit_seed_seed_app", "pgoutput"),
        ("stmt", "src", "create publication for crm"),
        ("slot", "src", "crm", "migkit_seed_seed_crm", "pgoutput"),
        ("copy",)], ran


def test_a_failed_copy_drops_the_slots_it_made(monkeypatch):
    hop = _hop(physical_opts=PG_OPTS)
    dropped = []
    monkeypatch.setattr(physical, "_pg_slots_before",
                        lambda h, e, dbs, log: {"app": "migkit_seed_x"})
    monkeypatch.setattr(physical, "_pg_drop_slots",
                        lambda e, slots: dropped.append(dict(slots)))

    def failed(*a):
        raise SystemExit("the copy of the source's pages failed")
    monkeypatch.setattr(physical, "_pg_copy", failed)
    monkeypatch.setattr(physical, "facts_for",
                        lambda h, e, m: _pg_facts(hop))

    class Eng:
        def databases(self):
            return ["app"]
    with pytest.raises(SystemExit):
        physical._pg_carry(hop, Eng(), _rung(PAGES), "", "full+cdc", True,
                           lambda m: None)
    assert dropped == [{"app": "migkit_seed_x"}]
    assert physical.seeded(hop) is None


# --- the position the target's recovery ended at -----------------------------

def test_the_replay_end_is_held_to_everything_around_it():
    ok = physical.check_replay_end("0/3001000", "0/3001000", "74",
                                   copied="0/3000100", system="74")
    assert ok["lsn"] == "0/3001000"
    cases = [
        (("", "0/3001000", "74", "0/3000100", "74"), "never guessed"),
        (("0/3001000", "0/3001000", "75", "0/3000100", "74"),
         "not a copy of this source"),
        (("0/3000000", "0/3001000", "74", "0/3000100", "74"),
         "before the copy's own end"),
        (("0/3001000", "0/3000F00", "74", "0/3000100", "74"),
         "do not describe one recovery"),
    ]
    for args, words in cases:
        with pytest.raises(SystemExit) as e:
            physical.check_replay_end(*args)
        assert words in str(e.value), (args, e.value)


def test_lsns_compare_as_numbers():
    assert physical.lsn_value("1/0") > physical.lsn_value("0/FFFFFFFF")
    assert physical.lsn_value("0/A") == 10


# --- the subscription that goes on from the copy -----------------------------

def test_the_subscription_goes_on_from_the_slot_and_the_lsn():
    from migkit.engines.postgres import PostgresEngine
    hop = _hop(physical_opts=PG_OPTS)
    eng = PostgresEngine(hop)
    eng._subscription_streaming = lambda db: "parallel"
    # the hop's tables, as the source lists them; no subscription yet
    eng._plan_rows = lambda side, db, sql: (
        [("public.t",)] if side == "src" else [])
    plain = eng.replicate_sql("app", copy_data=False)
    got = eng.replicate_sql("app", copy_data=False, slot="migkit_seed_x",
                            at="0/3001000", via=("pg-src", 5432))
    create, advance, enable = got["dst"]
    assert "create_slot = false, slot_name = 'migkit_seed_x'" in create
    assert "enabled = false" in create and "host=pg-src port=5432" in create
    assert "pg_replication_origin_advance('pg_' || oid, '0/3001000')" in \
        advance and "subname = 'migkit_seed'" in advance, advance
    assert enable == "alter subscription migkit_seed enable;"
    # asked for none of it, the statements are what they were
    assert len(plain["dst"]) == 1 and "slot_name" not in plain["dst"][0]
    assert "enabled" not in plain["dst"][0]
    assert "host=10.0.0.1 port=5432" in plain["dst"][0]


def test_the_slot_is_named_for_the_hop_and_the_database():
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop(physical_opts=PG_OPTS))
    assert physical.seed_slot(eng, "App-DB") == "migkit_seed_seed_app_db"
    assert len(physical.seed_slot(eng, "x" * 100)) == 63


# --- MySQL -------------------------------------------------------------------

MY_OPTS = {"source": "10.0.0.1:3306", "restart": "ssh 10.0.0.2 restart"}


def _my_facts(hop, mode="full+cdc", **over):
    f = physical.MyFacts(hop, None, mode)
    about = {"version": "8.4.2", "os": "Linux", "machine": "aarch64",
             "page": 16384, "files": "ibdata1:12M:autoextend",
             "packet": 64 * 1024 * 1024}
    f.update(options=hop.options["physical"], covers=True, managed_src="",
             src=dict(about), dst=dict(about), not_innodb=[], dst_tables=[],
             src_backup_admin=True, dst_clone_admin=True, src_plugin="ACTIVE",
             dst_plugin="ACTIVE", log_bin=True, gtid_mode="ON",
             src_users={"app"})
    f.update(over)
    return f


def test_every_need_holding_takes_the_clone():
    hop = _hop("mysql", MY_OPTS)
    assert _rung(CLONE).refused(_my_facts(hop)) is None


@pytest.mark.parametrize("over,words", [
    ({"src": {"version": "8.0.16"}}, "only from MySQL 8.0.17"),
    ({"src": {"version": "10.11.6-MariaDB"}}, "only from MySQL 8.0.17"),
    ({"dst": {"version": "8.0.37"}, "src": {"version": "8.0.36"}},
     "a clone needs the same"),
    ({"dst": {"machine": "x86_64"}}, "a clone needs the same"),
    ({"dst": {"page": 8192}}, "innodb_page_size"),
    ({"src": {"packet": 1024 * 1024}}, "max_allowed_packet"),
    ({"not_innodb": ["app.log (MyISAM)"]}, "would arrive empty"),
    ({"src_backup_admin": False}, "grant backup_admin on *.* to app"),
    ({"dst_clone_admin": False}, "grant clone_admin on *.* to app"),
    ({"src_plugin": ""}, "INSTALL PLUGIN clone SONAME 'mysql_clone.so'"),
    ({"managed_src": "Amazon Aurora"}, "cloud: ..."),
    ({"log_bin": False}, "the binlog is off"),
])
def test_each_clone_need_that_fails_is_said(over, words):
    hop = _hop("mysql", MY_OPTS)
    facts = _my_facts(hop)
    for k, v in over.items():
        facts[k] = {**facts[k], **v} if isinstance(v, dict) else v
    why = _rung(CLONE).refused(facts)
    assert why and words in why, why


def test_one_series_clones_from_8_0_37():
    assert physical._clone_versions_ok("8.4.0", "8.4.2")
    assert physical._clone_versions_ok("8.0.37", "8.0.39")
    assert not physical._clone_versions_ok("8.0.36", "8.0.37")
    assert not physical._clone_versions_ok("8.0.40", "8.4.2")


def test_the_source_plugin_goes_on_only_where_the_hop_allows_it():
    """Putting the plugin on the source changes its settings: refused, and
    the DBA's statement said, unless the hop allows it."""
    hop = _hop("mysql", {**MY_OPTS, "source_plugin": "install"})
    assert _rung(CLONE).refused(_my_facts(hop, src_plugin="")) is None


def test_the_plugin_put_on_the_source_is_taken_off_again(monkeypatch):
    """The one setting the hop may let migkit change on the source is put
    back whether the clone succeeds or not."""
    hop = _hop("mysql", {**MY_OPTS, "source_plugin": "install"})
    ran = []

    class Eng:
        def databases(self):
            return ["app"]

        def _q(self, side, sql):
            ran.append((side, sql))
            return []
    monkeypatch.setattr(physical, "facts_for",
                        lambda h, e, m: _my_facts(hop, src_plugin=""))

    def failed(*a):
        raise SystemExit("the target could not clone the source")
    monkeypatch.setattr(physical, "_my_clone", failed)
    with pytest.raises(SystemExit):
        physical._my_carry(hop, Eng(), _rung(CLONE), "app", "full+cdc",
                           True, lambda m: None)
    assert ran == [("src", "INSTALL PLUGIN clone SONAME 'mysql_clone.so'"),
                   ("src", "UNINSTALL PLUGIN clone")], ran
    # asked for nothing, it touches nothing on the source
    ran.clear()
    monkeypatch.setattr(physical, "facts_for",
                        lambda h, e, m: _my_facts(hop))
    with pytest.raises(SystemExit):
        physical._my_carry(hop, Eng(), _rung(CLONE), "app", "full+cdc",
                           True, lambda m: None)
    assert [x for x in ran if x[0] == "src"] == []


def test_privileges_are_read_by_name():
    assert physical._granted("GRANT SELECT ON *.* TO X GRANT BACKUP_ADMIN,"
                             "CLONE_ADMIN ON *.* TO X", "CLONE_ADMIN")
    assert not physical._granted("GRANT ALL PRIVILEGES ON *.* TO X",
                                 "BACKUP_ADMIN")


def test_a_clone_is_read_only_from_the_server_that_opened_it():
    """Before its restart the target still answers as the server it was,
    with the clone's status already written: nothing is read from it until
    it has started after the clone ended."""
    class Eng:
        def __init__(self, opened):
            self.opened = opened

        def _q(self, side, sql):
            assert side == "dst" and "clone_status" in sql
            return [("Completed", 0, "", "binlog.000003", 4711,
                     f"{U}:1-90,\n{V}:1-3", self.opened)]
    with pytest.raises(RuntimeError):
        physical._opened_clone(Eng(0))
    got = physical._opened_clone(Eng(1))
    assert (got["log_file"], got["log_pos"]) == ("binlog.000003", 4711)
    assert got["gtids"] == f"{U}:1-90,{V}:1-3"


def test_the_clone_statement_quotes_what_it_is_given():
    hop = _hop("mysql", MY_OPTS)
    hop.source.password = "it's"
    assert physical.clone_sql(hop, MY_OPTS) == (
        "CLONE INSTANCE FROM 'app'@'10.0.0.1':3306 IDENTIFIED BY 'it\\'s'")


# --- GTID sets and the position proof ----------------------------------------

U = "3e11fa47-71ca-11e1-9e33-c80aa9429562"
V = "4c9e3dfc-9d25-11e9-8d2e-0242ac1cfd7e"


def test_gtid_sets_as_the_servers_print_them():
    s = physical.gtid_set(f"{U.upper()}:1-5:7,\n{V}:3")
    assert s == {U: [(1, 5), (7, 7)], V: [(3, 3)]}
    assert physical.gtid_add(s, f"{U}:6") == {U: [(1, 7)], V: [(3, 3)]}
    assert physical.gtid_minus(s, physical.gtid_set(f"{U}:2-3")) == {
        U: [(1, 1), (4, 5), (7, 7)], V: [(3, 3)]}
    assert physical.gtid_and(s, physical.gtid_set(f"{U}:5-9")) == {
        U: [(5, 5), (7, 7)]}
    assert physical.gtid_text(s) == f"{U}:1-5:7,{V}:3"
    tagged = physical.gtid_set(f"{U}:1-2:blue:1-3")
    assert tagged == {U: [(1, 2)], f"{U}:blue": [(1, 3)]}


class _My:
    def __init__(self, executed):
        self.executed = executed

    def _q(self, side, sql):
        assert "gtid_executed" in sql
        return [(self.executed,)]


TOKEN = {"log_file": "binlog.000003", "log_pos": 4711}


def test_an_exact_position_is_proved():
    got = physical.position_proof(
        _My(f"{U}:1-100"), TOKEN, f"{U}:1-90",
        read_after=lambda e, t: physical.gtid_set(f"{U}:91-104"))
    assert got is None


def test_a_position_that_would_apply_twice_is_refused():
    got = physical.position_proof(
        _My(f"{U}:1-100"), TOKEN, f"{U}:1-90",
        read_after=lambda e, t: physical.gtid_set(f"{U}:90-100"))
    assert got and f"{U}:90" in got and "applied twice" in got, got


def test_a_position_that_would_miss_one_is_refused():
    got = physical.position_proof(
        _My(f"{U}:1-100,{V}:1-2"), TOKEN, f"{U}:1-90",
        read_after=lambda e, t: physical.gtid_set(f"{U}:92-100"))
    assert got and f"{U}:91" in got and f"{V}:1-2" in got, got
    assert "would be missed" in got


# --- as the decision engine reads them ---------------------------------------

def test_the_rungs_are_the_decision_engines_own():
    from migkit import decide
    for r in physical.rungs():
        assert isinstance(r, decide.Rung), r
        assert r.name and r.said and r.engine in ("postgres", "mysql")
        assert {"exact", "whole server"} <= set(r.gives), r.name
        assert r.needs and all(isinstance(n, decide.Need) and n.what
                                for n in r.needs), r.name
    assert len({(r.name, r.engine) for r in physical.rungs()}) == len(
        physical.rungs())


def test_the_decision_engine_climbs_them():
    """`decide.climb` over the rungs as any other list of rungs: the
    pages where every need holds, nothing and the reason where one does
    not - and a need the work asks for that only the pages give."""
    from migkit import decide
    hop = _hop(physical_opts=PG_OPTS)
    pg = physical.rungs("postgres")
    local = [r for r in pg if not r.cloud]
    got = decide.climb("the seed", local, _pg_facts(hop), prove=False)
    assert got.path == PAGES, got
    got = decide.climb("the seed", local, _pg_facts(hop, src_version=150001),
                       prove=False)
    assert got.path is None and "PostgreSQL 15 and the target 16" in \
        got.reason, got
    copier = decide.Rung("builtin", "the table copier")
    got = decide.climb("the seed", [copier] + local, _pg_facts(hop),
                       need=("pages proved",), prove=False)
    assert got.path == PAGES and "the pages must be proved" in got.reason, \
        got
