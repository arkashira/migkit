"""The interlock on the target (backlog R23, 3), rule by rule.

Before a write step migkit reads what each side says it is, shows both in
the plan, and does not write where the target is the source itself, where
it is not the server the plan showed, where it matches the hop's
`production` list without the hop naming it, or where a copy would write
tables that already hold rows the hop has said nothing about. Each rule is
shown here saying "stop" and, beside it, not saying it where the sides
differ.
"""
import sqlite3

import pytest
from click.testing import CliRunner

from migkit import interlock
from migkit.config import Endpoint, Hop

SERVER_A = {"id": {"cluster": "7431"}, "running": "2026-09-29 01:00",
            "name": "10.0.0.5", "version": "16.4", "bytes": 8_000_000}
SERVER_B = {"id": {"cluster": "9902"}, "running": "2026-09-29 02:00",
            "name": "10.0.0.6", "version": "16.4", "bytes": 7_000_000}


class Eng:
    """Two sides whose servers say what the test tells them to."""
    CANON_ENGINE = "postgres"

    def __init__(self, src, dst, held=()):
        self.said = {"src": src, "dst": dst}
        self.held = {tuple(t) for t in held}
        self.emptied = []

    def server_identity(self, side, db):
        got = self.said[side]
        if isinstance(got, Exception):
            raise got
        return dict(got)

    def rows_held(self, db, tables):
        return None if self.held is None else \
            {tuple(t) for t in tables if tuple(t) in self.held}

    def neutral_empty(self, side, db, table, where=None):
        assert side == "dst"
        self.emptied.append(table)
        return 3


@pytest.fixture(autouse=True)
def reachable(monkeypatch):
    # the addresses are examples; what the servers say is the test's
    from migkit import clouds
    monkeypatch.setattr(interlock, "_unreached", lambda hop, side, ep: None)
    monkeypatch.setattr(clouds, "of", lambda eng, side, ep: clouds.Where())


def _hop(tmp_path, src="10.0.0.1", dst="10.0.0.2", **kw):
    hop = Hop(name="h", engine="postgres",
              source=Endpoint(host=src, port=5432, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host=dst, port=5432, user="u",
                              password="CHANGE_ME"),
              databases=["app"], **kw)
    hop.report_dir = lambda db=None: tmp_path
    return hop


def _said():
    lines = []
    return lines, lines.append


def test_the_same_server_by_its_own_words_is_the_source(tmp_path):
    lines, log = _said()
    with pytest.raises(SystemExit) as got:
        interlock.before_write(_hop(tmp_path), Eng(SERVER_A, SERVER_A),
                               "app", True, log)
    assert "the target is the source itself" in str(got.value), got.value
    assert "nothing was written" in str(got.value)
    # both identities were shown before it stopped
    assert any(ln.startswith("source: postgres 16.4") for ln in lines), lines
    assert any("cluster 7431" in ln for ln in lines if ln.startswith(
        "target:")), lines


def test_a_clone_of_the_source_is_not_the_source(tmp_path):
    """A server restored from the source's backup keeps its system
    identifier; the process answering is its own."""
    clone = dict(SERVER_A, running="2026-09-29 03:00", name="10.0.0.9")
    lines, log = _said()
    interlock.before_write(_hop(tmp_path), Eng(SERVER_A, clone), "app",
                           True, log)
    assert not [ln for ln in lines if "source itself" in ln], lines


def test_the_same_address_and_database_is_the_source_whatever_it_says(
        tmp_path):
    silent = RuntimeError("permission denied")
    hop = _hop(tmp_path, src="LocalHost", dst="127.0.0.1")
    with pytest.raises(SystemExit) as got:
        interlock.before_write(hop, Eng(silent, silent), "app", True,
                               lambda m: None)
    assert "both are reached at 127.0.0.1:5432" in str(got.value), got.value


def test_the_same_server_into_another_database_is_a_target(tmp_path):
    hop = _hop(tmp_path, src="127.0.0.1", dst="127.0.0.1",
               db_map={"app": "app_new"})
    dst, _ = interlock.before_write(hop, Eng(SERVER_A, SERVER_A), "app",
                                    True, lambda m: None)
    assert dst["database"] == "app_new"


def test_a_target_that_is_not_the_one_planned_is_not_written(tmp_path):
    hop = _hop(tmp_path)
    lines, log = _said()
    interlock.before_write(hop, Eng(SERVER_A, SERVER_B), "app", False, log)
    # the hop pointed elsewhere between the plan and --go
    other = dict(SERVER_B, id={"cluster": "5550"}, name="10.0.0.7")
    with pytest.raises(SystemExit) as got:
        interlock.before_write(_hop(tmp_path), Eng(SERVER_A, other), "app",
                               True, log)
    said = str(got.value)
    assert "not the server the plan showed" in said, said
    assert "cluster 9902" in said and "cluster 5550" in said, said
    # the plan shown again takes the one there now
    interlock.before_write(_hop(tmp_path), Eng(SERVER_A, other), "app",
                           False, log)
    interlock.before_write(_hop(tmp_path), Eng(SERVER_A, other), "app",
                           True, log)


def test_the_same_target_again_is_written(tmp_path):
    interlock.before_write(_hop(tmp_path), Eng(SERVER_A, SERVER_B), "app",
                           False, lambda m: None)
    # the size is shown, not held: a write changes it
    grown = dict(SERVER_B, bytes=9_000_000)
    interlock.before_write(_hop(tmp_path), Eng(SERVER_A, grown), "app",
                           True, lambda m: None)


def test_a_restart_or_failover_is_seen_at_the_next_write_step(tmp_path):
    """A first --go with no plan shown keeps what it wrote to; the next
    write step is held to it."""
    interlock.before_write(_hop(tmp_path), Eng(SERVER_A, SERVER_B), "app",
                           True, lambda m: None)
    failed_over = dict(SERVER_B, running="2026-09-29 04:00",
                       name="10.0.0.8")
    with pytest.raises(SystemExit) as got:
        interlock.before_write(_hop(tmp_path), Eng(SERVER_A, failed_over),
                               "app", True, lambda m: None)
    assert "not the server the plan showed" in str(got.value)


def test_a_production_target_is_written_only_when_the_hop_names_it(
        tmp_path):
    prod = dict(SERVER_B, name="prod-db-1.example.com")
    hop = _hop(tmp_path, options={"production": ["prod-*"]})
    with pytest.raises(SystemExit) as got:
        interlock.before_write(hop, Eng(SERVER_A, prod), "app", True,
                               lambda m: None)
    said = str(got.value)
    assert "looks like production" in said, said
    assert "production_target: prod-db-1.example.com" in said, said
    # the hop's own address matching counts as well
    hop = _hop(tmp_path, dst="prod-db-2.example.com",
               options={"production": ["PROD-*"]})
    with pytest.raises(SystemExit):
        interlock.before_write(hop, Eng(SERVER_A, SERVER_B), "app", True,
                               lambda m: None)
    # named, it is written
    lines, log = _said()
    hop = _hop(tmp_path, options={"production": ["prod-*"],
                                  "production_target":
                                      "prod-db-1.example.com"})
    interlock.before_write(hop, Eng(SERVER_A, prod), "app", False, log)
    interlock.before_write(hop, Eng(SERVER_A, prod), "app", True, log)
    assert any("the hop names it as its target" in ln for ln in lines)
    # naming another production server does not open this one
    hop = _hop(tmp_path, options={"production": ["prod-*"],
                                  "production_target": "prod-db-9"})
    with pytest.raises(SystemExit):
        interlock.before_write(hop, Eng(SERVER_A, prod), "app", True,
                               lambda m: None)


def test_a_target_outside_the_production_list_is_written(tmp_path):
    hop = _hop(tmp_path, options={"production": ["prod-*"]})
    interlock.before_write(hop, Eng(SERVER_A, SERVER_B), "app", True,
                           lambda m: None)


TABLES = [("public", "orders"), ("public", "items"), ("public", "audit")]


def test_rows_the_hop_says_nothing_about_are_not_written_over(tmp_path):
    eng = Eng(SERVER_A, SERVER_B, held=[("public", "orders")])
    lines, log = _said()
    assert interlock.before_copy(_hop(tmp_path), eng, "app", TABLES, False,
                                 log) == set()
    assert any("with --go this stops: 1 of the 3 tables this copy writes"
               " already holds rows on the target: public.orders" in ln
               for ln in lines), lines
    with pytest.raises(SystemExit) as got:
        interlock.before_copy(_hop(tmp_path), eng, "app", TABLES, True, log)
    said = str(got.value)
    assert "public.orders" in said and "target_rows: replace" in said, said
    assert "target_rows: keep" in said, said


def test_an_empty_target_is_copied_into(tmp_path):
    eng = Eng(SERVER_A, SERVER_B, held=[])
    assert interlock.before_copy(_hop(tmp_path), eng, "app", TABLES, True,
                                 lambda m: None) == set()


def test_replace_and_keep_say_what_happens_to_them(tmp_path):
    held = [("public", "orders"), ("public", "items")]
    lines, log = _said()
    hop = _hop(tmp_path, options={"target_rows": "replace"})
    assert interlock.before_copy(hop, Eng(SERVER_A, SERVER_B, held), "app",
                                 TABLES, True, log) == set()
    assert any("removed before the source's rows are written" in ln
               for ln in lines), lines
    (tmp_path / "k").mkdir()
    hop = _hop(tmp_path / "k", options={"target_rows": "keep"})
    assert interlock.before_copy(hop, Eng(SERVER_A, SERVER_B, held), "app",
                                 TABLES, True, log) == set(held)
    interlock.leave_out(hop, "app", set(held))
    assert hop.excluded("app", "public", "orders")
    assert not hop.excluded("app", "public", "audit")
    assert not hop.excluded("other", "public", "orders")


def test_a_word_the_option_does_not_know_is_refused(tmp_path):
    hop = _hop(tmp_path, options={"target_rows": "merge"})
    with pytest.raises(SystemExit) as got:
        interlock.before_copy(hop, Eng(SERVER_A, SERVER_B, TABLES[:1]),
                              "app", TABLES, True, lambda m: None)
    assert "refuse, replace, keep" in str(got.value)


def test_rows_this_hop_wrote_on_this_target_are_its_own(tmp_path):
    """A move stopped halfway and run again finds its own rows."""
    interlock.before_copy(_hop(tmp_path), Eng(SERVER_A, SERVER_B, []),
                          "app", TABLES, True, lambda m: None)
    lines, log = _said()
    again = Eng(SERVER_A, SERVER_B, TABLES[:2])
    assert interlock.before_copy(_hop(tmp_path), again, "app", TABLES, True,
                                 log) == set()
    assert any("2 tables hold rows an earlier run of this hop wrote" in ln
               for ln in lines), lines
    # a restart is the same target: its rows are still this hop's, once
    # the plan has been shown again
    restarted = dict(SERVER_B, running="2026-09-29 05:00")
    eng = Eng(SERVER_A, restarted, TABLES[:2])
    interlock.before_copy(_hop(tmp_path), eng, "app", TABLES, False,
                          lambda m: None)
    interlock.before_copy(_hop(tmp_path), eng, "app", TABLES, True,
                          lambda m: None)
    # another server's rows are not, whatever this hop wrote elsewhere
    other = dict(SERVER_B, id={"cluster": "5550"})
    eng = Eng(SERVER_A, other, TABLES[:2])
    interlock.before_copy(_hop(tmp_path), eng, "app", TABLES, False,
                          lambda m: None)
    with pytest.raises(SystemExit) as got:
        interlock.before_copy(_hop(tmp_path), eng, "app", TABLES, True,
                              lambda m: None)
    assert "2 of the 3 tables" in str(got.value), got.value


def test_what_the_hop_kept_stays_the_targets_own(tmp_path):
    keep = _hop(tmp_path, options={"target_rows": "keep"})
    interlock.before_copy(keep, Eng(SERVER_A, SERVER_B, TABLES[:1]), "app",
                          TABLES, True, lambda m: None)
    # the next run finds orders (kept) and items (copied by this hop), and
    # the option gone: only orders is the target's own
    with pytest.raises(SystemExit) as got:
        interlock.before_copy(_hop(tmp_path),
                              Eng(SERVER_A, SERVER_B, TABLES[:2]), "app",
                              TABLES, True, lambda m: None)
    said = str(got.value)
    assert "1 of the 3 tables" in said and "public.orders" in said, said
    assert "public.items" not in said, said


def test_a_copy_of_the_whole_server_cannot_keep_and_empties_to_replace(
        tmp_path):
    held = TABLES[:1]
    keep = _hop(tmp_path, options={"target_rows": "keep"})
    with pytest.raises(SystemExit) as got:
        interlock.before_copy(keep, Eng(SERVER_A, SERVER_B, held), "app",
                              TABLES, True, lambda m: None, whole=True)
    assert "cannot leave one out" in str(got.value)
    eng = Eng(SERVER_A, SERVER_B, held)
    replace = _hop(tmp_path, options={"target_rows": "replace"})
    interlock.before_copy(replace, eng, "app", TABLES, False,
                          lambda m: None, whole=True)
    assert eng.emptied == [], "a plan empties nothing"
    interlock.before_copy(_hop(tmp_path, options={"target_rows":
                                                  "replace"}),
                          eng, "app", TABLES, True, lambda m: None,
                          whole=True)
    assert eng.emptied == ["public.orders"]
    # a replica of the whole server replaces what it holds by itself
    eng = Eng(SERVER_A, SERVER_B, held)
    eng.REPLICATES_THE_SERVER = True
    (tmp_path / "r").mkdir()
    replica = _hop(tmp_path, options={"target_rows": "replace"})
    replica.report_dir = lambda db=None: tmp_path / "r"
    interlock.before_copy(replica, eng, "app", TABLES, True, lambda m: None,
                          whole=True)
    assert eng.emptied == []


def test_rows_that_cannot_be_asked_are_said(tmp_path):
    eng = Eng(SERVER_A, SERVER_B)
    eng.held = None
    lines, log = _said()
    assert interlock.before_copy(_hop(tmp_path), eng, "app", TABLES, True,
                                 log) == set()
    assert any("could not be asked" in ln for ln in lines), lines


def test_a_target_nothing_answers_at_is_said_quickly(tmp_path, monkeypatch):
    import time
    monkeypatch.undo()
    hop = _hop(tmp_path, dst="127.0.0.1")
    hop.target.port = 9     # the discard port: nothing listens there
    began = time.monotonic()
    got = interlock.identity(hop, Eng(SERVER_A, SERVER_B), "dst", "app")
    assert time.monotonic() - began < 6
    assert got["unread"].startswith("nothing answers at 127.0.0.1:9"), got


def test_the_database_is_the_one_the_engine_writes(tmp_path, monkeypatch):
    """PostgreSQL never maps its maintenance database: a hop mapping it
    to another name on the source's own server writes the source's own
    database, and a label read from the hop called it another one."""
    from migkit.engines.postgres import PostgresEngine
    from migkit.engines.redis import RedisEngine
    monkeypatch.setattr(PostgresEngine, "server_identity",
                        lambda self, side, db: {})
    hop = _hop(tmp_path, dst="10.0.0.1", db_map={"postgres": "other"})
    with pytest.raises(SystemExit) as got:
        interlock.before_write(hop, PostgresEngine(hop), "postgres", True,
                               lambda m: None)
    assert "the target is the source itself" in str(got.value), got.value
    # a database it does map is another database on the same server
    hop = _hop(tmp_path, dst="10.0.0.1", db_map={"app": "other"})
    dst, _ = interlock.before_write(hop, PostgresEngine(hop), "app", True,
                                    lambda m: None)
    assert dst["database"] == "other"
    # a keyspace keeps its number on both sides, whatever db_map says
    monkeypatch.setattr(RedisEngine, "server_identity",
                        lambda self, side, db: {})
    (tmp_path / "r").mkdir()
    hop = _hop(tmp_path / "r", dst="10.0.0.1", db_map={"0": "1"})
    with pytest.raises(SystemExit) as got:
        interlock.before_write(hop, RedisEngine(hop), "0", True,
                               lambda m: None)
    assert "the target is the source itself" in str(got.value), got.value


def test_the_plan_names_the_managed_service(tmp_path, monkeypatch):
    from migkit import clouds
    monkeypatch.setattr(clouds, "of", lambda eng, side, ep: clouds.Where(
        "aws", "aurora-reader") if side == "dst" else clouds.Where())
    lines, log = _said()
    interlock.before_write(_hop(tmp_path), Eng(SERVER_A, SERVER_B), "app",
                           False, log)
    target = [ln for ln in lines if ln.startswith("target:")]
    assert "Amazon Aurora (its reader endpoint)" in target[0], lines
    source = [ln for ln in lines if ln.startswith("source:")]
    assert "Amazon" not in source[0], lines


def test_the_options_are_checked_before_anything_runs():
    from migkit import hopkeys
    raw = {"engine": "postgres",
           "source": {"host": "10.0.0.1", "user": "u"},
           "target": {"host": "10.0.0.2", "user": "u"},
           "options": {"target_rows": "merge",
                       "production_target": "db1.example.com"}}
    got = {p.where: p for p in hopkeys.check_hop("h", raw)}
    assert got["hops.h.options.target_rows"].level == "error", got
    assert "refuse, replace, keep" in got["hops.h.options.target_rows"].text
    assert got["hops.h.options.production_target"].level == "warn", got
    raw["options"] = {"target_rows": "keep", "production": ["prod-*"],
                      "production_target": "prod-db-1.example.com"}
    assert not [p for p in hopkeys.check_hop("h", raw)
                if "target_rows" in p.where or "production" in p.where]


# --- the whole command, on files -------------------------------------------

def _files(tmp_path, target_rows=None, target="b.db"):
    (tmp_path / "hops.yaml").write_text(
        "hops:\n  lite:\n    engine: sqlite\n"
        f"    source: {{host: {tmp_path / 'a.db'}, user: x, password: x}}\n"
        f"    target: {{host: {tmp_path / target}, user: x, password: x}}\n"
        "    databases: [main]\n"
        + (f"    options: {{target_rows: {target_rows}}}\n"
           if target_rows else ""))


def _db(path, rows):
    con = sqlite3.connect(path)
    for t in ("t", "u"):
        con.execute(f"create table if not exists {t}"
                    " (id integer primary key, v text)")
    for t, got in rows.items():
        con.executemany(f"insert into {t} values (?, ?)", got)
    con.commit()
    con.close()


def _rows(path, table):
    con = sqlite3.connect(path)
    try:
        return con.execute(f"select id, v from {table} order by id"
                           ).fetchall()
    finally:
        con.close()


@pytest.fixture
def files(tmp_path, monkeypatch):
    import migkit.config as cfg
    monkeypatch.setattr(cfg, "CONF", str(tmp_path / "hops.yaml"))
    monkeypatch.setattr(cfg, "REPORTS", tmp_path / "reports")
    _db(tmp_path / "a.db", {"t": [(1, "x"), (2, "y")], "u": [(5, "z")]})
    return tmp_path


def _move(*argv):
    from migkit import cli
    got = CliRunner().invoke(cli.main, ["move", "lite", *argv])
    return got, " ".join((got.output + str(got.exception or "")).split())


def test_a_move_into_an_empty_target_is_as_it_was(files):
    _db(files / "b.db", {})
    _files(files)
    got, said = _move("--go")
    assert got.exit_code == 0, said
    assert _rows(files / "b.db", "t") == [(1, "x"), (2, "y")]
    assert "target: sqlite at" in said and "database main" in said, said


def test_a_move_over_rows_it_was_not_told_about_stops(files):
    _db(files / "b.db", {"t": [(9, "theirs")]})
    _files(files)
    got, said = _move("--go")
    assert got.exit_code != 0, said
    assert "already holds rows on the target: t." in said, said
    assert _rows(files / "b.db", "t") == [(9, "theirs")]
    assert _rows(files / "b.db", "u") == [], "nothing was written"


def test_replace_and_keep_on_the_whole_command(files):
    _db(files / "b.db", {"t": [(9, "theirs")]})
    _files(files, "keep")
    got, said = _move("--go")
    assert got.exit_code == 0, said
    assert _rows(files / "b.db", "t") == [(9, "theirs")]
    assert _rows(files / "b.db", "u") == [(5, "z")]
    _files(files, "replace")
    got, said = _move("--go")
    assert got.exit_code == 0, said
    assert _rows(files / "b.db", "t") == [(1, "x"), (2, "y")]


def test_a_move_run_again_carries_on_with_its_own_rows(files):
    _db(files / "b.db", {})
    _files(files)
    assert _move("--go")[0].exit_code == 0
    got, said = _move("--go")
    assert got.exit_code == 0, said


def test_the_source_as_the_target_stops(files):
    _files(files, target="a.db")
    got, said = _move("--go")
    assert got.exit_code != 0, said
    assert "the target is the source itself" in said, said
    assert _rows(files / "a.db", "t") == [(1, "x"), (2, "y")]


def test_a_target_changed_after_the_plan_stops(files):
    _db(files / "b.db", {})
    _db(files / "c.db", {})
    _files(files)
    assert _move()[0].exit_code == 0
    _files(files, target="c.db")
    got, said = _move("--go")
    assert got.exit_code != 0, said
    assert "not the server the plan showed" in said, said
    assert _rows(files / "c.db", "t") == []
    assert _move()[0].exit_code == 0
    got, said = _move("--go")
    assert got.exit_code == 0, said
    assert _rows(files / "c.db", "t") == [(1, "x"), (2, "y")]
