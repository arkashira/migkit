"""Which way a cross-engine table's rows go, without a server (backlog
R20.13, R20.14): the source rendering the target's load format, the
compiled mover, or the table copier - rungs of the engine's own chooser
(`Engine._trials`, `measured_ways`), each unit of a table climbing them on
the table's own measurements: a way never timed tried on one unit, the
units started meanwhile going on to the next never timed, then each unit
the cheapest; a way that cannot carry a column exactly or leaves one out of
the digest passed over with the reason; a way whose unit did not prove
itself given up for the rest of the work; and a pair no way joins going
and reading exactly as it did.
"""
import pytest

from migkit import crossmove as cm
from migkit import loadtext
from migkit.config import Endpoint, Hop
from migkit.engines.hetero import HeteroEngine
from tests.test_the_report_does_not_name_its_tools import TOOLS

#: (source name, source type, target name, target type, class): what a
#: MySQL table of every class the two ways carry looks like to them
EVERY = [
    ("id", "bigint", "id", "bigint", "integer"),
    ("n", "int unsigned", "n", "bigint", "integer"),
    ("amount", "decimal(14,4)", "amount", "numeric(14,4)", "decimal"),
    ("ratio", "double", "ratio", "double precision", "float"),
    ("single", "float", "single", "double precision", "float"),
    ("name", "varchar(100)", "name", "character varying(100)", "text"),
    ("body", "longtext", "body", "text", "text"),
    ("raw", "varbinary(64)", "raw", "bytea", "bytes"),
    ("day", "date", "day", "date", "date"),
    ("at", "datetime(6)", "at", "timestamp(6) without time zone",
     "timestamp"),
    ("seen", "timestamp(6)", "seen", "timestamp(6) with time zone",
     "timestamp"),
    ("span", "time(6)", "span", "time(6) without time zone", "time"),
    ("doc", "json", "doc", "jsonb", "json"),
]

#: the same classes the other way, PostgreSQL into MySQL, as migkit builds
#: the MySQL table from the PostgreSQL one
BACK = [
    ("id", "bigint", "id", "bigint", "integer"),
    ("amount", "numeric(14,4)", "amount", "decimal(14,4)", "decimal"),
    ("ratio", "double precision", "ratio", "double", "float"),
    ("flag", "boolean", "flag", "tinyint(1)", "boolean"),
    ("name", "character varying(100)", "name", "varchar(100)", "text"),
    ("raw", "bytea", "raw", "longblob", "bytes"),
    ("at", "timestamp with time zone", "at", "datetime(6)", "timestamp"),
    ("u", "uuid", "u", "char(36)", "uuid"),
    ("doc", "jsonb", "doc", "json", "json"),
]


def _eng(tmp_path, monkeypatch, cols=EVERY, unproved=(), src="mysql",
         dst="postgres"):
    hop = Hop(name="xe", engine="hetero",
              source=Endpoint(host="10.0.0.1", port=3306, user="u",
                              password="CHANGE_ME"),
              target=Endpoint(host="10.0.0.2", port=5432, user="u",
                              password="CHANGE_ME"),
              databases=["app"],
              options={"source_engine": src, "target_engine": dst})
    hop.report_dir = lambda db="", _p=tmp_path: _p
    eng = HeteroEngine(hop)
    monkeypatch.setattr(cm, "columns",
                        lambda e, db, s, d: (list(cols), list(unproved)))
    monkeypatch.setattr(cm, "installed_extensions",
                        lambda: {"mysql_scanner", "postgres_scanner"})
    monkeypatch.delenv("MIGKIT_TABLE_COPY", raising=False)
    # a MySQL target that takes the client's load, as it is asked
    eng.dst_engine._loads_here = lambda: True
    return eng


def _way(eng, said=None, whole=False, table="t"):
    return cm.TableWay(eng, "app", table, f"public.{table}",
                       (said if said is not None else []).append,
                       f"app.{table}", whole=whole)


def test_a_pair_no_way_joins_goes_as_before_and_says_nothing(
        tmp_path, monkeypatch):
    for src, dst in (("sqlite", "sqlite"), ("mongodb", "postgres"),
                     ("postgres", "postgres"), ("mysql", "sqlite")):
        said = []
        w = _way(_eng(tmp_path, monkeypatch, src=src, dst=dst), said=said)
        assert not w.joined and not w.others(), (src, dst)
        assert [w.pick() for _ in range(3)] == [cm.COPIER] * 3, (src, dst)
        assert said == [], said


def test_a_range_at_a_time_never_timed_first_then_the_cheapest(
        tmp_path, monkeypatch):
    said = []
    eng = _eng(tmp_path, monkeypatch)
    w = _way(eng, said)
    assert w.others()
    # three ranges handed over at once: one on each way never timed, in
    # the order the ways were measured, none tried twice meanwhile
    assert [w.pick(), w.pick(), w.pick()] == [cm.RENDERED, cm.COMPILED,
                                              cm.COPIER]
    assert said == ["app.t: the source rendering the target's load format"
                    " - measured fastest here: both servers do the work,"
                    " and the bytes between them are only passed on"], said
    w.took(cm.RENDERED, 250_000, 4.0)
    w.took(cm.COMPILED, 250_000, 3.0)
    w.took(cm.COPIER, 250_000, 12.0)
    # timed on this table's own ranges, the cheapest from here on
    assert [w.pick(), w.pick()] == [cm.COMPILED, cm.COMPILED]
    assert said[-1].startswith("app.t: each way timed on its own units -"
                               " compiled 12.0 us a row, rendered 16.0 us"
                               " a row, copier 48.0 us a row"), said
    # what the engine reports it measured, as every way it times
    got = [m for m in eng.measured_ways() if m["table"] == "app.t"]
    assert got and got[0]["work"] == cm.WORK and \
        got[0]["chosen"] == cm.COMPILED, got
    # another table is timed on its own ranges
    assert _way(eng, table="u").pick() == cm.RENDERED


def test_whole_tables_are_held_to_one_another(tmp_path, monkeypatch):
    eng = _eng(tmp_path, monkeypatch)
    first = _way(eng, whole=True, table="a")
    assert first.pick() == cm.RENDERED
    first.took(cm.RENDERED, 1000, 0.5)
    second = _way(eng, whole=True, table="b")
    assert second.pick() == cm.COMPILED
    second.took(cm.COMPILED, 1000, 0.2)
    third = _way(eng, whole=True, table="c")
    assert third.pick() == cm.COPIER
    third.took(cm.COPIER, 1000, 3.0)
    assert _way(eng, whole=True, table="d").pick() == cm.COMPILED


def test_a_column_one_way_cannot_carry_passes_it_over_with_the_reason(
        tmp_path, monkeypatch):
    # a bit field: its number is rendered by the source, and the compiled
    # mover reads it as bytes
    bits = EVERY + [("b", "bit(8)", "b", "bigint", "integer")]
    w = _way(_eng(tmp_path, monkeypatch, cols=bits))
    assert w.pick() == cm.RENDERED
    assert w.pick() == cm.COPIER, "the compiled mover is not offered"
    # a double into MySQL's single float is rounded, and the compiled
    # mover writes into PostgreSQL only: the table copier, and why
    said = []
    narrowed = BACK + [("r", "double precision", "r", "float", "float")]
    w = _way(_eng(tmp_path, monkeypatch, cols=narrowed, src="postgres",
                  dst="mysql"), said)
    assert w.pick() == cm.COPIER
    assert said == ["app.t: the table copier - r is a double, which a float"
                    " holds only rounded; the compiled mover does not join"
                    " these two engines"], said
    # a type no way carries: the table copier, with each way's reason
    said = []
    geo = EVERY + [("g", "geometry", "g", "text", None)]
    w = _way(_eng(tmp_path, monkeypatch, cols=geo), said)
    assert w.pick() == cm.COPIER and not w.others()
    assert said == ["app.t: the table copier - g is geometry, whose"
                    " rendering as PostgreSQL's load format is not proved;"
                    " g is geometry and lands in text, a value of another"
                    " kind, which the compiled mover does not turn into it"
                    " exactly"], said
    # past the compiled mover's 38 digits, from a file into PostgreSQL
    said = []
    wide = [("id", "integer", "id", "bigint", "integer"),
            ("w", "decimal(65,30)", "w", "numeric(65,30)", "decimal")]
    w = _way(_eng(tmp_path, monkeypatch, cols=wide, src="duckdb",
                  dst="postgres"), said)
    assert w.pick() == cm.COPIER
    assert said[0].startswith(
        "app.t: the table copier - the source renders the target's load"
        " format between MySQL and PostgreSQL only; w: decimal(65,30) into"
        " numeric(65,30): past 38 digits"), said


def test_postgres_into_mysql_every_class_by_the_source_rendering(
        tmp_path, monkeypatch):
    said = []
    w = _way(_eng(tmp_path, monkeypatch, cols=BACK, src="postgres",
                  dst="mysql"), said)
    assert w.pick() == cm.RENDERED, said
    # the compiled mover does not write into MySQL: the next unit, on the
    # one way left to time
    assert w.pick() == cm.COPIER
    # a target that does not take the client's load: the table copier, and
    # migkit says why rather than turning the setting on
    said = []
    eng = _eng(tmp_path, monkeypatch, cols=BACK, src="postgres",
               dst="mysql")
    eng.dst_engine._loads_here = lambda: False
    assert _way(eng, said).pick() == cm.COPIER
    assert "local_infile" in said[0] and "does not change a server's" \
        " settings" in said[0], said


def test_postgres_renders_mysqls_load_format():
    """What PostgreSQL is asked for, a column at a time: a text as the hex
    of its UTF-8 (a form feed and a vertical tab would come out of COPY as
    escapes the load reads as letters), bytes as hex, an instant as UTC's
    wall clock, a float as the double it is - and how the load turns each
    field back."""
    sql, kinds = loadtext.copy_out('"public"."t"', [
        (n, t, c) for n, t, _, _, c in BACK] + [
        ("r", "real", "float")], '"id" > 7')
    assert sql == (
        'copy (select ("id")::text, ("amount"::numeric)::text,'
        ' ("ratio"::float8)::text, "flag"::int::text,'
        " encode(convert_to(\"name\"::text, 'UTF8'), 'hex'),"
        " encode(\"raw\", 'hex'),"
        " to_char((\"at\" at time zone 'UTC'), 'YYYY-MM-DD"
        " HH24:MI:SS.US'), \"u\"::text, \"doc\"::text,"
        ' ("r"::float8)::text from "public"."t" where "id" > 7) to stdout'
    ), sql
    assert kinds == ["text", "text", "text", "text", "hex", "hex", "text",
                     "text", "text", "text"]
    assert loadtext.refused(BACK, ("postgres", "mysql")) is None
    for bad, words in (
            (("t", "text", "t", "char(10)", "text"), "own input rules"),
            (("b", "bytea", "b", "binary(4)", "bytes"), "own input rules"),
            (("r", "double precision", "r", "float", "float"),
             "holds only rounded"),
            (("i", "interval", "i", "varchar(20)", None), "not proved")):
        got = loadtext.refused(BACK + [bad], ("postgres", "mysql"))
        assert got and words in got and got.startswith(bad[0]), (bad, got)


def test_a_column_the_digest_leaves_out_keeps_both_ways_away(
        tmp_path, monkeypatch):
    said = []
    w = _way(_eng(tmp_path, monkeypatch, unproved=["doc"]), said)
    assert w.pick() == cm.COPIER
    assert "doc is not compared on both sides" in said[0], said


def test_without_its_parts_installed_the_compiled_mover_is_passed_over(
        tmp_path, monkeypatch):
    lite = [("id", "integer", "id", "bigint", "integer"),
            ("v", "text", "v", "text", "text")]
    eng = _eng(tmp_path, monkeypatch, cols=lite, src="sqlite")
    monkeypatch.setattr(cm, "installed_extensions", lambda: {"postgres"})
    said = []
    w = _way(eng, said)
    assert w.pick() == cm.COPIER
    assert "doctor --install" in said[0], said
    # with them, it takes the table
    monkeypatch.setattr(cm, "installed_extensions",
                        lambda: {"sqlite_scanner", "postgres_scanner"})
    assert _way(eng, table="u").pick() == cm.COMPILED


def test_a_way_that_did_not_prove_itself_is_given_up_for_the_rest(
        tmp_path, monkeypatch):
    said = []
    w = _way(_eng(tmp_path, monkeypatch), said)
    assert w.pick() == cm.RENDERED
    w.fell(cm.RENDERED, "the source holds 10 rows there and the target 9")
    assert said[-1] == (
        "app.t: the source rendering the target's load format did not"
        " prove itself (the source holds 10 rows there and the target 9);"
        " the range was copied again by the table copier, and the rest"
        " climb again without it")
    assert w.pick() == cm.COMPILED
    w.fell(cm.COMPILED, "it stopped: lost connection")
    assert [w.pick(), w.pick()] == [cm.COPIER, cm.COPIER]
    # the copier is the floor: nothing to give up to
    w.fell(cm.COPIER, "anything")
    assert w.pick() == cm.COPIER


def test_one_way_named_for_measuring(tmp_path, monkeypatch):
    eng = _eng(tmp_path, monkeypatch)
    monkeypatch.setenv("MIGKIT_TABLE_COPY", cm.COMPILED)
    w = _way(eng)
    # every unit on it, however many start at once
    assert [w.pick(), w.pick(), w.pick()] == [cm.COMPILED] * 3
    # given up on, or not holding: the table copier
    w.fell(cm.COMPILED, "it stopped: lost connection")
    assert w.pick() == cm.COPIER
    said = []
    assert _way(_eng(tmp_path, monkeypatch, unproved=["doc"]),
                said).pick() == cm.COPIER
    assert said[0].startswith("app.t: the table copier - doc is not"), said
    monkeypatch.setenv("MIGKIT_TABLE_COPY", "fastest")
    with pytest.raises(SystemExit, match="not one of"):
        _way(eng)


def test_the_load_format_refuses_what_it_has_not_proved():
    assert loadtext.refused(EVERY) is None
    for bad, words in (
            (("u", "char(36)", "u", "uuid", "text"), "own input rules"),
            (("f", "double", "f", "real", "float"), "holds only rounded"),
            (("g", "geometry", "g", "bytea", "bytes"), "not proved"),
            (("t", "time", "t", "time with time zone", "time"),
             "own input rules")):
        got = loadtext.refused(EVERY + [bad])
        assert got and words in got and got.startswith(bad[0]), (bad, got)
    # a single float as the double it is, into either
    assert loadtext.refused([("f", "float", "f", "real", "float"),
                             ("g", "float", "g", "double precision",
                              "float")]) is None
    assert "(`f` * 1e0)" in loadtext.cell("f", "float", "float")


def test_the_attach_carries_the_endpoints_tls(tmp_path, monkeypatch):
    eng = _eng(tmp_path, monkeypatch)
    eng.src_engine.hop.source.options.update(
        {"ssl_ca": "/etc/ca.pem", "ssl_cert": "/c.pem", "ssl_key": "/k.pem"})
    got = cm._tls(eng.src_engine, "src").split()
    assert sorted(got) == ["ssl_ca=/etc/ca.pem", "ssl_cert=/c.pem",
                           "ssl_key=/k.pem", "ssl_mode=verify_identity"]
    eng.src_engine.hop.source.options["ssl_verify_identity"] = False
    assert "ssl_mode=verify_ca" in cm._tls(eng.src_engine, "src")
    eng.src_engine.hop.source.options.clear()
    assert cm._tls(eng.src_engine, "src") == ""
    eng.src_engine.hop.source.options["ssl"] = True
    assert cm._tls(eng.src_engine, "src") == "ssl_mode=required"
    eng.dst_engine.hop.target.options.update(
        {"sslmode": "verify-full", "sslrootcert": "/etc/root.pem"})
    assert cm._tls(eng.dst_engine, "dst") == \
        "sslmode=verify-full sslrootcert=/etc/root.pem"


def test_a_source_is_asked_for_each_column_as_it_is_moved(
        tmp_path, monkeypatch):
    """A single float as the double it is and an instant as seconds since
    the epoch, which no session zone moves - made a UTC timestamp again
    by the compiled mover."""
    eng = _eng(tmp_path, monkeypatch)
    cols = [c for c in EVERY if c[0] in ("id", "single", "seen")]
    got = cm._source_query(eng, "app", "t", cols, "`id` > 7")
    assert got == ("mysql_query('migkit_src', 'select `id`, (`single` * 1e0)"
                   " as `single`, unix_timestamp(`seen`) as `seen` from"
                   " `app`.`t` where `id` > 7')"), got
    assert [cm._cast(i, c, eng) for i, c in enumerate(cols)] == [
        'migkit_rows."id"', 'migkit_rows."single"',
        'make_timestamp(cast(migkit_rows."seen" * 1000000 as bigint))']


def test_doctor_install_puts_the_compiled_movers_parts_in_place(
        monkeypatch):
    from migkit import tools
    monkeypatch.setattr(tools, "which", lambda n: "/bin/" + n)
    monkeypatch.setattr(tools, "second_reader_present", lambda: True)
    monkeypatch.setattr(tools, "compiled_mover_present", lambda: False)
    states = {n: st for n, _, st, _ in tools.capabilities()}
    assert states["Cross-engine: server to server copy"] == "reduced"
    asked, said = [], []
    monkeypatch.setattr(cm, "install_extensions",
                        lambda log=None: asked.append(1) or True)
    # everything else in place, and no other terms to answer: what is said
    # is the compiled mover's own line
    monkeypatch.setattr(tools, "_present",
                        lambda c: c != tools.COMPILED_MOVER)
    monkeypatch.setattr(tools, "accept_terms", lambda *a, **k: {})
    tools.install_missing(said.append)
    assert asked == [1], said
    assert "installing the server to server copy's parts ..." in said, said
    low = " ".join(said).lower()
    assert not [t for t in TOOLS + ("duckdb",) if t in low], said
    # nothing to name by hand: migkit installs it wherever it runs
    assert tools.by_hand([tools.COMPILED_MOVER]) == []


def test_the_one_pass_load_does_not_ask_a_mysql_source_for_large_objects(
        tmp_path, monkeypatch):
    """What the one-pass load's numbers above were measured through: it
    emptied the target and then asked the source, as PostgreSQL, for its
    large objects - psql speaking to MySQL's port - and stopped."""
    from migkit import movers
    asked = []

    def sh(argv, env=None, log=None, **kw):
        asked.append(argv)
        return type("P", (), {"stdout": ""})()
    monkeypatch.setattr(movers, "_sh", sh)
    hetero = _eng(tmp_path, monkeypatch).hop
    movers._pg_clear_carried_objects(hetero, "app", {})
    assert asked == []
    pg = Hop(name="pg", engine="aurora-postgres",
             source=Endpoint(host="10.0.0.1", port=5432, user="u",
                             password="CHANGE_ME"),
             target=Endpoint(host="10.0.0.2", port=5432, user="u",
                             password="CHANGE_ME"), databases=["app"])
    movers._pg_clear_carried_objects(pg, "app", {})
    assert asked and asked[0][:3] == ["psql", "-h", "10.0.0.1"], asked


def test_a_one_pass_load_that_says_error_and_exits_0_has_failed(
        tmp_path, monkeypatch):
    """Measured on MySQL 8.4: the load program could not sign in, logged
    it as an ERROR, loaded nothing and exited 0."""
    import subprocess

    from migkit import movers
    said = ("2026-09-30T15:00:12 LOG Migrating from mysql\n2026-09-30T15:00"
            ":12 ERROR mysql: Failed to connect to mysql at \"10.0.0.1\" as"
            " user \"u\": The value of SEQUENCE is not of type SIMPLE-ARRAY")
    monkeypatch.setattr(movers, "_pg_truncate_target", lambda *a: "")
    monkeypatch.setattr(movers, "_sh", lambda argv, env=None, log=None:
                        subprocess.CompletedProcess(argv, 0, said, ""))
    hop = _eng(tmp_path, monkeypatch).hop
    with pytest.raises(RuntimeError) as e:
        movers.pgloader_move(hop, "app", 4, True, None)
    words = str(e.value).lower()
    assert "failed to connect" in words, words
    assert not [t for t in TOOLS if t in words], words
    # its log is not left behind with the passwords in it
    assert not (tmp_path / "pgloader.load").exists()
    # a log with no error is a load that ran
    monkeypatch.setattr(movers, "_sh", lambda argv, env=None, log=None:
                        subprocess.CompletedProcess(argv, 0, "LOG done",
                                                    ""))
    assert movers.pgloader_move(hop, "app", 4, True, None)


def test_no_way_is_said_by_a_programs_name():
    words = " ".join(f"{r.said} {r.because}" for r in cm.ways()).lower()
    assert not [t for t in TOOLS + ("duckdb",) if t in words], words
