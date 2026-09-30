"""Oracle to Oracle, server to server: the target's Data Pump pulls the
tables over a database link to the source, as of one SCN, no row through
this process (backlog F5; the ClickHouse `remote()` idea, in Oracle's own
words).

Chosen where it can run, and said where it cannot: the target has Data
Pump and the account may use it for another schema's tables (the
DATAPUMP_IMP_FULL_DATABASE role), may make a database link, and the link
reaches the source with an account that may export them
(DATAPUMP_EXP_FULL_DATABASE) - measured, without the last the job stops
with ORA-31631 before it moves a row. Where one of those is missing, the
table copier carries the tables instead, its batches written by direct
path loads (`oracle.OracleEngine.neutral_write`).

The link is made for the move and dropped after it; its name is migkit's
own, so one left by a move that was stopped is found and replaced. Every
table is read as of one SCN taken before the job, so the copy is one
consistent moment of the source, and each table's rows are counted at
that SCN on both sides once the job is done: a job that says it finished
is not taken at its word.
"""
import hashlib
import json

LINK_PREFIX = "MIGKIT$L_"


def _link_name(eng, db):
    digest = hashlib.sha256(f"{eng.hop.name}:{db}".encode(),
                            usedforsecurity=False).hexdigest()[:16]
    return f"{LINK_PREFIX}{digest.upper()}"


def why_not(eng, db):
    """Why the target cannot pull the tables itself, in words, or None."""
    from .oracle_logminer import granted
    try:
        have = granted(eng, "dst", db)
        pump = eng._rows("dst", db, "select count(*) from all_objects where"
                                    " object_name = 'DBMS_DATAPUMP' and"
                                    " object_type = 'PACKAGE'")
    except Exception as e:  # noqa: BLE001 - said
        return f"the target's roles could not be read: {str(e)[:80]}"
    if not pump or not pump[0][0]:
        return "the target has no Data Pump"
    if not have & {"DATAPUMP_IMP_FULL_DATABASE", "IMP_FULL_DATABASE",
                   "DBA"}:
        return ("the target's account may not import another schema's"
                " tables (DATAPUMP_IMP_FULL_DATABASE)")
    if "CREATE DATABASE LINK" not in have:
        return "the target's account may not make a database link"
    try:
        src = granted(eng, "src", db)
    except Exception as e:  # noqa: BLE001 - said
        return f"the source's roles could not be read: {str(e)[:80]}"
    if not src & {"DATAPUMP_EXP_FULL_DATABASE", "EXP_FULL_DATABASE", "DBA"}:
        return ("the source's account may not export another schema's"
                " tables (DATAPUMP_EXP_FULL_DATABASE)")
    return None


def _link_dsn(eng):
    """Where the target reaches the source: `link_dsn` on the source where
    the target sees it by another address than this machine does."""
    ep = eng.hop.source
    return ep.options.get("link_dsn") or eng._dsn(ep)


def _make_link(eng, db, cur):
    name = _link_name(eng, db)
    cur.execute("select count(*) from user_db_links where db_link like :1",
                [name + "%"])
    if cur.fetchone()[0]:
        cur.execute(f"drop database link {name}")
    ep = eng.hop.source
    secret = str(ep.password).replace('"', "")
    user = eng._q(ep.user)
    cur.execute(f"create database link {name} connect to {user}"
                f' identified by "{secret}" using'
                f" '{_link_dsn(eng).replace(chr(39), '')}'")
    return name


JOB = """declare
  h number; s varchar2(30); sts ku$_Status; le ku$_LogEntry;
  m varchar2(32767);
begin
  h := dbms_datapump.open('IMPORT', 'TABLE', :link);
  dbms_datapump.metadata_filter(h, 'SCHEMA_EXPR', 'IN (''' || :src || ''')');
  dbms_datapump.metadata_filter(h, 'NAME_LIST', :names);
  if :src <> :dst then
    dbms_datapump.metadata_remap(h, 'REMAP_SCHEMA', :src, :dst);
  end if;
  dbms_datapump.set_parameter(h, 'TABLE_EXISTS_ACTION', 'TRUNCATE');
  dbms_datapump.set_parameter(h, 'FLASHBACK_SCN', :scn);
  dbms_datapump.start_job(h);
  s := 'UNDEFINED';
  while s not in ('COMPLETED', 'STOPPED') loop
    dbms_datapump.get_status(h, dbms_datapump.ku$_status_job_error +
      dbms_datapump.ku$_status_job_status, -1, s, sts);
    if bitand(sts.mask, dbms_datapump.ku$_status_job_error) != 0 then
      le := sts.error;
      if le is not null then
        for i in le.first .. le.last loop
          m := substr(m || le(i).LogText || chr(10), 1, 32000);
        end loop;
      end if;
    end if;
  end loop;
  dbms_datapump.detach(h);
  :state := s;
  :errors := m;
end;"""


def ways():
    """How Oracle is moved to Oracle, the first that holds taken."""
    from ..decide import Need, Rung
    return (
        Rung("pump", "the target's Data Pump over a database link",
             gives=frozenset({"consistent as of"}),
             needs=(Need("Data Pump and what it needs on both sides",
                         lambda f: f["why not"] is None,
                         lambda f: f["why not"]),),
             because="the target pulls every table as of one SCN, no row"
                     " through this process"),
        Rung("copier", "the table copier", gives=frozenset(
            {"resume by key", "row filter", "column mapping"})))


def bulk(eng, db, tables, go, log, shape_only=()):
    """Steps said, and with `go` the tables copied - by Data Pump where it
    can run, by the table copier otherwise."""
    from ..decide import Facts, climb
    way = climb(db, ways(), Facts(**{"why not": lambda: why_not(eng, db)}))
    if way.path == "copier":
        steps = [f"{t}: copied table by table ({way.reason})"
                 for t in tables]
        if go and tables:
            _by_the_copier(eng, db, tables, log)
        return steps
    steps = [f"{t}: pulled by the target's Data Pump over a database link,"
             " as of one SCN" for t in tables]
    if not go or not tables:
        return steps
    import oracledb
    src_owner, dst_owner = eng._owner("src", db), eng._owner("dst", db)
    scn = eng.scn_after_ddl("src", db)
    conn = eng._dedicated("dst")
    try:
        cur = conn.cursor()
        link = _make_link(eng, db, cur)
        try:
            cur.execute(f"select 1 from dual@{link}")
        except Exception as e:  # noqa: BLE001 - said, then the copier
            _drop_link(eng, conn, link)
            log(f"the target cannot reach the source over a database link"
                f" ({str(e).splitlines()[0][:80]}); copying table by table")
            _by_the_copier(eng, db, tables, log)
            return steps
        try:
            names = ", ".join("'" + eng._q(t)[1:-1].replace("'", "''") + "'"
                              for t in tables)
            state = cur.var(str)
            errors = cur.var(oracledb.DB_TYPE_VARCHAR, 32767)
            cur.execute(JOB, link=link, src=src_owner, dst=dst_owner,
                        names=names, scn=scn, state=state, errors=errors)
            said = (errors.getvalue() or "").strip()
            if state.getvalue() != "COMPLETED":
                raise SystemExit(f"{db}: the target's Data Pump stopped"
                                 f" ({state.getvalue()}): {said[:300]}")
            short = []
            for t in tables:
                q = eng._q(t)
                cur.execute(f"select count(*) from {eng._q(src_owner)}.{q}"
                            f"@{link} as of scn {scn}")
                want = cur.fetchone()[0]
                cur.execute(f"select count(*) from {eng._q(dst_owner)}.{q}")
                have = cur.fetchone()[0]
                if want != have:
                    short.append(f"{t} ({have:,} of {want:,})")
                elif log:
                    log(f"{t}: {have:,} rows, as of SCN {scn}")
            if short:
                raise SystemExit(
                    f"{db}: the target's Data Pump said it finished, and"
                    f" {', '.join(short[:6])} hold other than the source"
                    f" held at SCN {scn}"
                    + (f" - it said: {said[:200]}" if said else ""))
        finally:
            _drop_link(eng, conn, link)
    finally:
        if conn.is_healthy():
            conn.close()
    # the copy is the source as of this SCN exactly: what a later tail
    # would need to start from nothing earlier
    (eng.hop.report_dir(db) / "dump-position.json").write_text(
        json.dumps({"scn": scn}))
    return steps


def _drop_link(eng, conn, link):
    """The link dropped from a session that never used it: the session
    that did holds a connection over it until it ends, and the link cannot
    be dropped while it does (measured, ORA-02018, a rollback and `alter
    session close database link` first or not)."""
    if conn.is_healthy():
        conn.close()
    other = eng._dedicated("dst")
    try:
        other.cursor().execute(f"drop database link {link}")
    finally:
        other.close()


def _by_the_copier(eng, db, tables, log):
    """The tables carried by the table copier, which resumes by key and
    writes through the target's own writer."""
    from ..cli import _checkpoint
    from .hetero import HeteroEngine
    ck = _checkpoint(eng.hop, eng.hop.report_dir(db) / "move-native.json")
    with eng.load_window(db, log, set(tables)):
        for t in tables:
            eng.move_table(db, "", t, HeteroEngine.READ_ROWS, ck,
                           log or (lambda m: None))
