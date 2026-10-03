"""Every PostgreSQL path carries the hop's TLS, as it was decided, in the
form that path takes it (audit 2026-10-01, SEC2, SEC13, SC17, DU12).

The hop's TLS reached migkit's own connections and was dropped on the main
data paths while `doctor` said verified: the native subscription's
connection string had no `sslmode` and its password raw in a SQL literal,
the programs on libpq were started with whatever the environment held. One
`connect` and one `_client` make every connection and every program now,
from `Endpoint.tls_forms`; the target dialling the source carries what a
server can (`far`), and `doctor` says verified only of a path that checked.
Without a server: what each path is handed is read back here.
"""
import pytest

from migkit.config import Endpoint, Hop

PASSWORD = "pa'ss\\word w@:/"


def _hop(src_options=None, engine="postgres", **extra):
    src = Endpoint(host="10.0.0.5", port=5432, user="app",
                   password=PASSWORD, options=dict(src_options or {}))
    dst = Endpoint(host="10.0.0.6", port=5433, user="app",
                   password=PASSWORD, options={"sslmode": "require"})
    return Hop(name="tls", engine=engine, source=src, target=dst,
               databases=["shop"], db_map={"shop": "shop_v2"}, **extra)


def _parsed(conninfo):
    from psycopg.conninfo import conninfo_to_dict
    return conninfo_to_dict(conninfo)


def _unliteral(lit):
    """A string constant's text back: the password holds a backslash, so
    it is the escape form, `E'...'`."""
    assert lit.startswith("E'") and lit.endswith("'"), lit
    return lit[2:-1].replace("''", "'").replace("\\\\", "\\")


def test_the_target_dials_the_source_with_its_tls_and_quoted(tmp_path):
    from migkit import legs
    hop = _hop({"sslmode": "verify-full",
                "sslrootcert": str(tmp_path / "ca.pem")})
    got = _parsed(legs.conninfo(hop, "shop", "10.0.0.5", 5432))
    # a quote, a backslash and a space in the password read back whole
    assert got["password"] == PASSWORD, got
    # verified here, carried as far as a server can: encrypted
    assert got["sslmode"] == "require", got
    assert "sslrootcert" not in got
    assert got["dbname"] == "shop" and got["keepalives"] == "1"
    plain = _parsed(legs.conninfo(_hop({"sslmode": "disable"}), "shop",
                                  "10.0.0.5", 5432))
    assert plain["sslmode"] == "disable", plain


def test_the_page_copy_dials_the_source_with_its_tls():
    """The copy of the pages runs on the target's host and dialled the
    source with whatever that host's environment held."""
    from migkit import physical
    opts = {"data": "/var/lib/postgresql/data", "source": "10.0.0.5:5432"}
    script = physical._pg_script(_hop({"sslmode": "verify-full"}), opts,
                                 False)
    assert "export PGSSLMODE=require;" in script, script
    assert PASSWORD not in script
    plain = physical._pg_script(_hop({"sslmode": "disable"}), opts, False)
    assert "export PGSSLMODE=disable;" in plain, plain


def test_the_servers_own_replication_is_built_the_same_way(monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop({"sslmode": "verify-full",
                               "sslrootcert": "/nowhere/ca.pem"}))
    monkeypatch.setattr(PostgresEngine, "published_tables",
                        lambda self, db: ["public.t"])
    monkeypatch.setattr(PostgresEngine, "_subscription_streaming",
                        lambda self, db: None)
    monkeypatch.setattr(PostgresEngine, "_refresh_sql",
                        lambda self, db, name, tables: [])
    sql = eng.replicate_sql("shop", copy_data=False)
    for stmt in (sql["dst"][0], sql["arm_dst"][0]):
        _, _, rest = stmt.partition(" connection ")
        got = _parsed(_unliteral(rest[:rest.index(" publication ")]))
        assert got["password"] == PASSWORD and got["sslmode"] == "require", got
        assert got["host"] == "10.0.0.5" and got["dbname"] == "shop"


def test_a_dial_asks_not_to_be_logged_and_is_bounded(monkeypatch):
    from migkit.engines import postgres
    monkeypatch.delenv("MIGKIT_SUBSCRIBE_TIMEOUT", raising=False)
    ran = []

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *e):
            return False

        def execute(self, sql, *a):
            ran.append(sql)

    class Conn:
        autocommit = False

        def cursor(self):
            return Cursor()

        def close(self):
            pass
    monkeypatch.setattr(postgres, "connect", lambda *a, **k: Conn())
    eng = postgres.PostgresEngine(_hop())
    eng.apply_replication_stmt("dst", "shop", "create subscription s"
                                              " connection 'x' publication p")
    assert ran == ["set log_statement = 'none'", "set statement_timeout ="
                   " 45000", "create subscription s connection 'x'"
                   " publication p"], ran


def test_every_connection_is_made_one_way(monkeypatch):
    import psycopg2

    from migkit.engines import postgres
    monkeypatch.setenv("MIGKIT_KEEPALIVE_IDLE", "7")
    seen = []
    monkeypatch.setattr(psycopg2, "connect", lambda **kw: seen.append(kw))
    hop = _hop({"sslmode": "verify-ca", "sslrootcert": "/etc/ca.pem"})
    eng = postgres.PostgresEngine(hop)
    eng._connect("dst", "shop", "probe")
    eng._connect("src", "shop", "probe")
    dst, src = seen
    # the target's own name, mapped once; the source's as it is
    assert dst["dbname"] == "shop_v2" and src["dbname"] == "shop"
    assert dst["sslmode"] == "require"
    assert src["sslmode"] == "verify-ca" and src["sslrootcert"] == \
        "/etc/ca.pem"
    for kw in seen:
        assert kw["keepalives_idle"] == 7 and kw["keepalives"] == 1
        assert "-c TimeZone=UTC" in kw["options"], kw
        assert kw["application_name"] == "migkit"
    monkeypatch.setenv("MIGKIT_KEEPALIVE_IDLE", "soon")
    with pytest.raises(SystemExit, match="MIGKIT_KEEPALIVE_IDLE"):
        eng._connect("src", "shop", "probe")


def test_every_program_is_started_one_way():
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop({"sslmode": "verify-full",
                               "sslrootcert": "/etc/ca.pem"}))
    argv, env = eng._client("src", "shop", "pg_dump", "--schema-only")
    assert env["PGSSLMODE"] == "verify-full"
    assert env["PGSSLROOTCERT"] == "/etc/ca.pem"
    assert env["PGPASSWORD"] == PASSWORD
    assert "-c TimeZone=UTC" in env["PGOPTIONS"]
    got = _parsed(argv[argv.index("-d") + 1])
    assert got["dbname"] == "shop" and got["keepalives"] == "1"
    argv, env = eng._client("dst", "shop", "psql")
    assert _parsed(argv[argv.index("-d") + 1])["dbname"] == "shop_v2"
    assert env["PGSSLMODE"] == "require"


def test_the_pass_through_connects_the_one_way(monkeypatch):
    from migkit.engines import pg_pass, postgres
    seen = []
    monkeypatch.setattr(postgres, "connect",
                        lambda ep, dbname, driver, options, **kw:
                        seen.append((ep, dbname, driver, options, kw)))
    eng = postgres.PostgresEngine(_hop())
    pg_pass.connect(eng, "dst", "shop", pg_pass.LOAD_OPTIONS)
    ep, dbname, driver, options, kw = seen[0]
    assert ep is eng.hop.target and dbname == "shop_v2"
    assert driver == "psycopg" and kw == {"autocommit": True}
    assert options == pg_pass.LOAD_OPTIONS


def test_doctor_says_verified_only_of_the_paths_that_checked(monkeypatch):
    from migkit import tls
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.setattr(tls, "posture", lambda ep, family: {
        "state": "verified", "detail": "sslmode verify-full"})
    monkeypatch.delenv("MIGKIT_CDC", raising=False)
    eng = PostgresEngine(_hop({"sslmode": "verify-full"},
                              options={"server_to_server": True}))
    rows = eng._leg_items()
    said = [r["item"] for r in rows]
    assert "source connection verified" in said, said
    assert ("source connection NOT verified on the target pulling the rows"
            " from the source") in said, said
    assert ("source connection NOT verified on the servers' own"
            " replication") in said, said
    # a hop whose changes are followed from here, with no pull: nothing
    # the target dials
    monkeypatch.setenv("MIGKIT_CDC", "follow")
    eng = PostgresEngine(_hop({"sslmode": "verify-full"}))
    assert eng.far_paths() == []
    assert not any("NOT verified" in r["item"] for r in eng._leg_items())
    # a hop to another engine has no server-to-server path
    monkeypatch.delenv("MIGKIT_CDC")
    assert PostgresEngine(_hop(engine="mysql")).far_paths() == []


def test_the_leg_says_whether_its_own_connection_checked(monkeypatch):
    from migkit.engines.postgres import PostgresEngine
    monkeypatch.delenv("MIGKIT_CDC", raising=False)

    def made(mode):
        class Info:
            dsn_parameters = {"sslmode": mode}

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, *e):
                return False

            def execute(self, sql):
                pass

            def fetchone(self):
                return (True, "TLSv1.3", "TLS_AES_256_GCM_SHA384")

        class Conn:
            info = Info()

            def cursor(self):
                return Cursor()

            def close(self):
                pass
        return lambda self, side, db, purpose="read", **kw: Conn()
    monkeypatch.setattr(PostgresEngine, "_connect", made("verify-full"))
    got = PostgresEngine(_hop()).leg_encryption("src", "shop")
    assert got["verified"] is True
    assert got["unverified"] == ["the servers' own replication"], got
    assert "not on the servers' own replication" in got["how"], got
    monkeypatch.setattr(PostgresEngine, "_connect", made("require"))
    got = PostgresEngine(_hop()).leg_encryption("src", "shop")
    assert got["verified"] is False and got["unverified"] == [], got


def test_the_plan_says_the_target_cannot_check_what_migkit_does(
        monkeypatch):
    from migkit import legs
    note = legs._tls_note(_hop({"sslmode": "verify-full"}))
    assert note and "cannot check it" in note[0], note
    assert legs._tls_note(_hop({"sslmode": "require"})) == []


def test_a_text_repair_writes_as_the_load_writes(monkeypatch):
    """A text repair's updates ran with the target's triggers firing,
    where every other write into the target was quiet."""
    import contextlib

    from migkit.engines.base import RepairAction
    from migkit.engines.postgres import PostgresEngine
    eng = PostgresEngine(_hop())
    events = []

    @contextlib.contextmanager
    def window(db, log=None, tables=None):
        events.append(("window", db, frozenset(tables or ())))
        yield
        events.append(("closed",))

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *e):
            return False

        def execute(self, sql, *a):
            events.append(("ran", sql))

    class Conn:
        def transaction(self):
            return contextlib.nullcontext()

        def cursor(self):
            return Cursor()

        def close(self):
            events.append(("close",))
    monkeypatch.setattr(PostgresEngine, "load_window",
                        lambda self, db, log=None, tables=None:
                        window(db, log, tables))
    monkeypatch.setattr(PostgresEngine, "_connect",
                        lambda self, side, db, purpose="read", **kw: Conn())
    stmts = ['update "public"."t" set "v" = \'é\' where "id" = 1 and "v" ='
             ' \'Ã©\'']
    eng.apply("shop", RepairAction("shop", "text", stmts, [], ""))
    assert events[0] == ("window", "shop", frozenset({"public.t"}))
    assert ("ran", stmts[0]) in events
    assert events.index(("ran", stmts[0])) < events.index(("closed",))

    # refused by the window: nothing is written
    events.clear()

    def refused(self, db, log=None, tables=None):
        raise SystemExit("public.t have triggers on the target")
    monkeypatch.setattr(PostgresEngine, "load_window", refused)
    with pytest.raises(SystemExit, match="have triggers"):
        eng.apply("shop", RepairAction("shop", "text", stmts, [], ""))
    assert events == [], events


def test_what_may_leave_its_session_behind_is_told_apart():
    from migkit.engines.postgres import _changes_shape, _plain_read
    assert _plain_read("select 1") and _plain_read(" show timezone ")
    for sql in ("select pg_advisory_lock(1)", "set search_path = x",
                "select 1; select 2", "select nextval('s')",
                "with d as (delete from t returning *) select * from d",
                "select * into t2 from t", "begin; select 1; commit"):
        assert not _plain_read(sql), sql
    assert _changes_shape("alter table t add column c int")
    assert _changes_shape("drop table t")
    assert not _changes_shape("create temp table x (id int)")
    assert not _changes_shape("select 1")
