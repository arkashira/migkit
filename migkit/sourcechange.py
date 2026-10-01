"""A change on the source a way needs: offered with what it buys, never
assumed and never hidden (the owner, 2026-09-30).

Many teams allow no change on a production source - the managed
migration services make none - so "no" is a normal answer here, not an
error. A way that needs one says so in `decide` (`Rung.changes`); this
module holds the rest, once, for every engine:

* **One list of changes** (`DEFINITIONS`): what each is in words, the
  statements that make it and take it off again, what a cloud changes
  outside the server, whether it takes a restart, and what not having it
  means. `dba`'s scripts are written from the same list, so handing a
  change to the DBA is the third answer, not a second wording.
* **Where it stands is read on the server** by the rung's probe (a
  plugin's status, the grants the user holds, whether the server loads a
  plugin at all, a refusal the server gave before) - a cloud's name is at
  most a hint said beside it, never the proof.
* **The offer, in one step** (`offer_lines`): what changes on the source,
  that it is taken off again, and what it saves - minutes from what this
  hop measured, or estimated from migkit's own measurements and said so -
  and the load it spares the source. The dry run says it; `--go` at a
  terminal asks it (yes, no, dba); a hop answers ahead with
  `source_changes` (`physical: {source_plugin: install}` still means yes
  to the clone plugin). An answer given at a terminal is recorded and
  stands for the hop's later runs; the hop's own option overrides it.
* **Yes** (`making`): behind the hop's approvals (`source-setting`),
  recorded before it acts, taken off again after the work or on any
  failure; a run killed in between has it taken off by the next one
  (`undo_left`) and `doctor` names it until then. A server that refuses
  it is recorded, and from then on the change is impossible there - the
  server's own answer - and the next way runs.
* **No, or impossible**: the next way, said with its time and its load on
  the source; nothing is asked where nothing could be done.
* **A requirement, not a speed-up** (following changes needs a log that
  carries rows): the same words of consequence (`consequence`), and it
  stays the DBA's to make.
"""
import contextlib
import json
import time
from dataclasses import dataclass, field

from . import decide

#: the hop option an answer is given ahead under
OPTION = "source_changes"
#: the approvals step a change on the source waits for
STEP = "source-setting"

#: what not following the source's changes means, for every requirement of
#: following them
FOLLOW_WITHOUT = ("without it the source's changes cannot be followed: the"
                  " only way left is a copy taken while nothing writes to the"
                  " source - the application stopped for the whole copy")


@dataclass
class Ctx:
    """What a change's statements are written with, on one side."""
    who: str = ""          #: the grantee, as the server writes it
    me: str = ""           #: the same, unquoted
    where: object = None   #: `clouds.Where`
    label: str = "the source"
    db: str = ""
    tables: list = field(default_factory=list)
    owner: str = ""
    name: str = ""         #: a publication's name
    short: list = field(default_factory=list)   #: privileges it lacks
    docdb: bool = False

    @property
    def cloud(self):
        return getattr(self.where, "cloud", "") or ""

    @property
    def aws(self):
        return self.cloud == "aws"

    @property
    def aurora(self):
        return self.aws and "aurora" in (getattr(self.where, "service", "")
                                         or "")


@dataclass(frozen=True)
class Definition:
    """One change on a source: what it is, and how it is made and taken
    off, per the side's cloud."""
    name: str
    engine: str
    said: str
    make: object                 #: Ctx -> [statement]
    undo: object = None          #: Ctx -> [statement]; None: never by migkit
    outside: object = None       #: Ctx -> [line a cloud changes outside]
    restart: object = False      #: bool, or Ctx -> bool
    speeds: bool = False         #: a faster way's, not a requirement
    without: str = FOLLOW_WITHOUT
    after: str = "after the copy"

    def statements(self, ctx):
        return list(self.make(ctx) or ())

    def taking_off(self, ctx):
        return list(self.undo(ctx) or ()) if self.undo else []

    def elsewhere(self, ctx):
        return list(self.outside(ctx) or ()) if self.outside else []

    def restarts(self, ctx):
        return bool(self.restart(ctx) if callable(self.restart)
                    else self.restart)


def _setting(engine, name, value, restart=False):
    """make and outside for a server setting, in the side's cloud's words
    (`dba._setting`, the one place that spells them)."""
    def both(c):
        from .dba import _setting as spell
        r = restart(c) if callable(restart) else restart
        return spell(engine, c.where, name, value, r, c.label)
    return (lambda c: both(c)[0]), (lambda c: both(c)[1])


def _ms(name):
    return "[" + str(name).replace("]", "]]") + "]"


def _ora(name):
    return '"' + str(name).replace('"', '""') + '"'


_WAL = _setting("postgres", "wal_level", "logical", True)
_SLOTS = _setting("postgres", "max_replication_slots", "10", True)
_SENDERS = _setting("postgres", "max_wal_senders", "10", True)
_LOG_BIN = _setting("mysql", "log_bin", "ON", True)
_FORMAT = _setting("mysql", "binlog_format", "ROW", lambda c: c.aurora)
_IMAGE = _setting("mysql", "binlog_row_image", "FULL", lambda c: c.aurora)
_EXPIRE = _setting("mysql", "binlog_expire_logs_seconds", "86400")

#: MySQL's own clone, put on a source and taken off again
CLONE_MAKE = "INSTALL PLUGIN clone SONAME 'mysql_clone.so'"
CLONE_UNDO = "UNINSTALL PLUGIN clone"

def _ora_mining(c):
    """The grants the LogMiner reader lacks (`dba.ORA_FOLLOW`, read into
    `c.short`), as the side's cloud gives them."""
    from .dba import ORA_RDS_OBJECTS
    if c.aws:
        # on RDS the SYS objects are granted through its own procedure,
        # the master user holding no grant option
        return [f"grant {p.lower()} to {c.who};" for p in c.short] + [
            f"exec rdsadmin.rdsadmin_util.grant_sys_object('{v}',"
            f" '{c.me}');" for v in ORA_RDS_OBJECTS]
    return ([f"grant {p.lower()} to {c.who};" for p in c.short
             if p != "SELECT_CATALOG_ROLE"]
            + ([f"grant select_catalog_role to {c.who};"]
               if "SELECT_CATALOG_ROLE" in c.short else [])
            + [f"grant execute on sys.dbms_logmnr to {c.who};",
               f"grant execute on sys.dbms_flashback to {c.who};"])


def _ora_redo_files(c):
    from .dba import ORA_REDO_VIEWS
    if not c.tables:
        return []
    return ([f"grant select, flashback on sys.{_ora(t)} to {c.who};"
             for t in c.tables]
            + [f"grant select on sys.{v} to {c.who};" for v in ORA_REDO_VIEWS]
            + [f"grant select, flashback on xdb.xdb$ttset to {c.who};",
               "-- and select, flashback on each xdb.x$nm, x$qn and x$pt"
               " table of xdb.xdb$ttset's suffixes"])


#: every change on a source migkit knows, once: the rungs' offers and the
#: DBA's scripts are written from these
DEFINITIONS = {d.name: d for d in (
    # -- a faster way's -----------------------------------------------------
    Definition(
        "clone_plugin", "mysql", "the clone plugin",
        make=lambda c: [CLONE_MAKE], undo=lambda c: [CLONE_UNDO],
        speeds=True,
        without="the target is filled by a logical copy, which reads every"
                " row of the source"),
    # the temporary subscription's copy (`legs`), there for its rung to
    # declare: a publication of the hop's tables and the slot the
    # subscription makes, dropped once every table is copied
    Definition(
        "temporary_publication", "postgres",
        "a publication of the hop's tables and a slot, for the copy only",
        make=lambda c: [f"-- in database {c.db}:",
                        f"create publication {c.name} for table "
                        + ", ".join(c.tables) + ";"],
        undo=lambda c: [f"-- in database {c.db}:",
                        f"drop publication if exists {c.name};",
                        f"select pg_drop_replication_slot('{c.name}');"],
        speeds=True,
        without="the rows go through migkit's own copy, read from the"
                " source row by row"),
    Definition(
        "redo_file_grants", "oracle",
        "the redo-file reader's reading of the dictionary",
        make=_ora_redo_files, speeds=True,
        without="the redo is read back through LogMiner, which needs none"
                " of these grants and reads at the rate the database mines"),
    # -- PostgreSQL ---------------------------------------------------------
    Definition("wal_level", "postgres",
               "a log that carries rows (wal_level logical)",
               make=_WAL[0], outside=_WAL[1], restart=True),
    Definition("replication_slots", "postgres",
               "a free replication slot (max_replication_slots)",
               make=_SLOTS[0], outside=_SLOTS[1], restart=True),
    Definition("wal_senders", "postgres",
               "a free WAL sender (max_wal_senders)",
               make=_SENDERS[0], outside=_SENDERS[1], restart=True),
    Definition(
        "replication_role", "postgres", "the right to read the change log"
        " (replication)",
        make=lambda c: ([f"grant rds_replication to {c.who};"] if c.aws
                        else [f"alter role {c.who} with replication;"]),
        undo=lambda c: ([f"revoke rds_replication from {c.who};"] if c.aws
                        else [f"alter role {c.who} with noreplication;"])),
    Definition(
        "publication", "postgres", "a publication of every table's changes",
        make=lambda c: [f"-- in database {c.db}:",
                        f"create publication {c.name} for all tables;"],
        undo=lambda c: [f"-- in database {c.db}:",
                        f"drop publication if exists {c.name};"]),
    # -- MySQL --------------------------------------------------------------
    Definition(
        "log_bin", "mysql", "a binary log (log_bin ON)",
        make=_LOG_BIN[0], restart=True,
        outside=lambda c: {
            "aws": ["binary logging is on while automated backups are kept:"
                    " set the backup retention above 0"],
            "gcp": ["gcloud sql instances patch <instance>"
                    " --enable-bin-log"]}.get(c.cloud) or _LOG_BIN[1](c)),
    Definition("binlog_format", "mysql", "rows in the log (binlog_format ROW)",
               make=_FORMAT[0], outside=_FORMAT[1]),
    Definition("binlog_row_image", "mysql",
               "whole rows in the log (binlog_row_image FULL)",
               make=_IMAGE[0], outside=_IMAGE[1]),
    Definition(
        "binlog_retention", "mysql", "a log kept a day or more",
        make=lambda c: (["call mysql.rds_set_configuration('binlog retention"
                         " hours', 24);"] if c.aws else _EXPIRE[0](c)),
        outside=lambda c: [] if c.aws else _EXPIRE[1](c),
        without="without it the log a stream reads from may be gone before"
                " it is read, and the copy is made again"),
    Definition(
        "replication_grants", "mysql",
        "the right to read the binary log (replication)",
        make=lambda c: [f"grant replication slave, replication client on"
                        f" *.* to {c.who};"]),
    # -- MongoDB ------------------------------------------------------------
    Definition(
        "replica_set", "mongodb", "a change stream (a replica set)",
        make=lambda c: ["rs.initiate();"], restart=True,
        outside=lambda c: ["start the server as a replica set member"
                           " (replSetName in its configuration) and restart"
                           " it"]),
    Definition(
        "change_streams", "mongodb", "change streams on the database",
        make=lambda c: ([f'db.adminCommand({{modifyChangeStreams: 1,'
                         f' database: {json.dumps(str(c.db))}, collection:'
                         ' "", enable: true});'] if c.docdb else []),
        undo=lambda c: ([f'db.adminCommand({{modifyChangeStreams: 1,'
                         f' database: {json.dumps(str(c.db))}, collection:'
                         ' "", enable: false});'] if c.docdb else [])),
    # -- SQL Server ---------------------------------------------------------
    Definition(
        "change_tracking", "mssql", "change tracking on the database",
        make=lambda c: [f"alter database {_ms(c.db)} set change_tracking ="
                        " on (change_retention = 2 days, auto_cleanup ="
                        " on);"],
        undo=lambda c: [f"alter database {_ms(c.db)} set change_tracking ="
                        " off;"]),
    Definition(
        "table_tracking", "mssql", "change tracking on every table",
        make=lambda c: [f"use {_ms(c.db)};"] + [
            f"alter table {_ms(s)}.{_ms(t)} enable change_tracking;"
            for s, t in c.tables],
        undo=lambda c: [f"use {_ms(c.db)};"] + [
            f"alter table {_ms(s)}.{_ms(t)} disable change_tracking;"
            for s, t in c.tables]),
    Definition(
        "tracking_grant", "mssql", "the right to read change tracking",
        make=lambda c: [f"use {_ms(c.db)};"] + [
            f"grant view change tracking on schema::{_ms(s)} to {c.who};"
            for s in c.tables]),
    # -- Oracle -------------------------------------------------------------
    Definition(
        "archivelog", "oracle", "the redo archived (ARCHIVELOG)",
        make=lambda c: [] if c.aws else [
            "shutdown immediate;", "startup mount;",
            "alter database archivelog;", "alter database open;"],
        outside=lambda c: ["the automated backups' retention above 0 keeps"
                           " the redo archived, and `exec rdsadmin."
                           "rdsadmin_util.set_configuration('archivelog"
                           " retention hours', 24);` keeps it for a day"]
        if c.aws else [],
        restart=lambda c: not c.aws),
    Definition(
        "supplemental_logging", "oracle", "minimal supplemental logging",
        make=lambda c: (["exec rdsadmin.rdsadmin_util."
                         "alter_supplemental_logging(p_action => 'ADD');"]
                        if c.aws else
                        ["alter database add supplemental log data;"]),
        undo=lambda c: (["exec rdsadmin.rdsadmin_util."
                         "alter_supplemental_logging(p_action => 'DROP');"]
                        if c.aws else
                        ["alter database drop supplemental log data;"])),
    Definition(
        "table_key_logging", "oracle", "every table logging its key",
        make=lambda c: [f"alter table {_ora(c.owner)}.{t} add supplemental"
                        " log data (primary key) columns;" for t in c.tables],
        undo=lambda c: [f"alter table {_ora(c.owner)}.{t} drop supplemental"
                        " log data (primary key) columns;"
                        for t in c.tables]),
    Definition("logmining_grants", "oracle", "the right to read the redo back",
               make=_ora_mining),
    Definition(
        "table_logging", "oracle", "no table skipping the redo",
        make=lambda c: [f"alter table {_ora(c.owner)}.{_ora(t)} logging;"
                        for t in c.tables]),
    # -- Db2 for i: CL commands, run through QSYS2.QCMDEXC ------------------
    # `c.tables` are (file, library) system names
    Definition(
        "table_journaling", "db2i", "every table journaled",
        make=lambda c: [_cl(f"STRJRNPF FILE({lib}/{file}) JRN({lib}/QSQJRN)"
                            " IMAGES(*BOTH)") for file, lib in c.tables]),
    Definition(
        "before_images", "db2i", "before images journaled",
        make=lambda c: [_cl(f"CHGJRNOBJ OBJ(({lib}/{file} *FILE))"
                            " ATR(*IMAGES) IMAGES(*BOTH)")
                        for file, lib in c.tables]),
    Definition(
        "whole_entries", "db2i", "a journal that keeps whole entries",
        make=lambda c: [_cl(f"CHGJRN JRN({c.name}) MINENTDTA(*NONE)")]),
    # -- the cloud-native services (P0-CLOUDS part 2) -----------------------
    # `c.name` the change stream, slot or instance; `c.tables` the tables
    Definition(
        "spanner_change_stream", "spanner",
        "a change stream watching every table in scope",
        make=lambda c: [f"create change stream {c.name} for all options"
                        " (retention_period = '1d');"],
        undo=lambda c: [f"drop change stream {c.name};"]),
    Definition(
        "bigtable_change_stream", "bigtable",
        "a change stream on every table in scope",
        make=lambda c: [f"gcloud bigtable instances tables update {t}"
                        f" --instance={c.db} --change-stream-retention-period"
                        "=1d" for t in c.tables],
        undo=lambda c: [f"gcloud bigtable instances tables update {t}"
                        f" --instance={c.db} --clear-change-stream-retention"
                        "-period" for t in c.tables]),
    Definition(
        "keyspaces_cdc", "keyspaces", "a CDC stream on every table in scope",
        make=lambda c: [f"alter table {c.db}.{t} with cdc = true and"
                        " custom_properties = {'cdc_specification':"
                        " {'view_type': 'NEW_IMAGE'}};" for t in c.tables],
        undo=lambda c: [f"alter table {c.db}.{t} with cdc = false;"
                        for t in c.tables]),
    Definition(
        "neptune_streams", "neptune", "Neptune Streams on for the cluster",
        make=lambda c: [], restart=True,
        outside=lambda c: ["set neptune_streams to 1 in the cluster's DB"
                           " cluster parameter group, and reboot its"
                           " writer"]),
    Definition(
        "gauss_wal_level", "gaussdb",
        "a log that carries rows (wal_level logical)",
        make=lambda c: [] if c.cloud else
        ["alter system set wal_level = logical;"], restart=True,
        outside=lambda c: ["set wal_level to logical in the instance's"
                           " parameters on the console, and restart it"]
        if c.cloud else []),
    Definition(
        "gauss_replication", "gaussdb",
        "the right to read the change log (replication)",
        make=lambda c: [f"alter role {c.who} with replication;"],
        undo=lambda c: [f"alter role {c.who} with noreplication;"]),
    # -- an archive's (`archive`): rows, not a setting ---------------------
    # the rows a hop's rule archives, removed from the source once the
    # archive holds them proved; put back from the archive by the hop's
    # `archive: {restore: true}`. `c.tables` are the tables, `c.name` the
    # rule's words
    Definition(
        "archived_rows", "any",
        "the archived rows removed from the source",
        make=lambda c: [f"-- delete from {t} the rows archived by its rule,"
                        " each proved on the archive first" for t in
                        c.tables],
        undo=lambda c: [f"-- the removed rows of {t} put back from the"
                        " archive (archive: {restore: true})"
                        for t in c.tables],
        speeds=False, after="never by itself",
        without="the archived rows stay on the source as well as in the"
                " archive"),
)}


def _cl(command):
    from .dba import _cl as cl
    return cl(command)


def statements(name, ctx):
    """The statements that make the change `name` on one side."""
    return DEFINITIONS[name].statements(ctx)


def outside(name, ctx):
    """What a cloud changes outside the server for `name`."""
    return DEFINITIONS[name].elsewhere(ctx)


def restarts(name, ctx):
    return DEFINITIONS[name].restarts(ctx)


def consequence(name):
    """What not having the change `name` means, in words."""
    return DEFINITIONS[name].without


def requirement(name, now, ctx=None):
    """A requirement as every refusal and assessment says it: what is
    missing and how it stands, the statements that make it (or what the
    side's cloud changes), and what not having it means."""
    d = DEFINITIONS[name]
    ctx = ctx or Ctx()
    how = (" ".join(decide.terminated(s) for s in d.statements(ctx))
           or "; ".join(d.elsewhere(ctx))).rstrip(";")
    return (f"{d.said} is needed ({now}): {how}"
            + (" - it takes a restart" if d.restarts(ctx) else "")
            + f"; {d.without}")


class Defined(decide.Change):
    """A `decide.Change` written from one of `DEFINITIONS`: its statements
    from the definition, with what `ctx(facts)` gives them."""

    def __init__(self, name, probe, ctx, after=None, said=None):
        d = DEFINITIONS[name]
        object.__setattr__(self, "definition", d)
        object.__setattr__(self, "ctx", ctx)
        super().__init__(
            name=name, said=said or d.said, probe=probe,
            make=lambda f: d.statements(ctx(f)),
            undo=lambda f: d.taking_off(ctx(f)),
            after=after or d.after, speeds=d.speeds)

    def outside(self, facts):
        return self.definition.elsewhere(self.ctx(facts))

    def restarts(self, facts):
        return self.definition.restarts(self.ctx(facts))

    def __eq__(self, other):
        return isinstance(other, decide.Change) and other.name == self.name

    def __hash__(self):
        return hash(self.name)


def change(name, probe, ctx, after=None, said=None):
    """The `decide.Change` a rung declares for `name`."""
    return Defined(name, probe, ctx, after, said)


# --- the answer --------------------------------------------------------------

_WORDS = {"install": "yes", "yes": "yes", "true": "yes", "on": "yes",
          "no": "no", "false": "no", "off": "no", "dba": "dba"}


def answer_word(value):
    """An answer as the hop writes it - YAML reads `yes` and `no` as true
    and false - or None where it is none of them."""
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return _WORDS.get(str(value).strip().lower())


def answers(hop):
    """{change name: (answer, where it was given)} - the hop's own
    (`source_changes`, `physical.source_plugin` for the clone plugin and
    `server_to_server` for the temporary subscription's publication) over
    what an operator answered at a terminal. "*" is an answer given
    for every change."""
    out = {}
    for name, got in (_record(hop).get("answers") or {}).items():
        a = answer_word(got.get("answer"))
        if a:
            out[name] = (a, f"answered at a terminal by {got.get('by') or '?'}"
                            f" on {got.get('at') or '?'}")
    opts = hop.options or {}
    phys = opts.get("physical")
    if isinstance(phys, dict) and "source_plugin" in phys:
        a = answer_word(phys["source_plugin"])
        if a:
            out["clone_plugin"] = (a, "the hop's physical.source_plugin")
    if opts.get("server_to_server"):
        # the hop's leave for what the servers' own copy makes on them for
        # the copy and drops after (`legs`)
        out["temporary_publication"] = ("yes", "the hop's server_to_server")
    raw = opts.get("source_changes")
    if isinstance(raw, dict):
        for k, v in raw.items():
            a = answer_word(v)
            if a:
                out[str(k)] = (a, f"the hop's {OPTION}")
    elif raw is not None and answer_word(raw):
        out = {"*": (answer_word(raw), f"the hop's {OPTION}")}
    return out


def answer_of(hop, name, got=None):
    """(answer, where it was given) for the change `name`, or (None, "")."""
    got = answers(hop) if got is None else got
    return got.get(name) or got.get("*") or (None, "")


def put_answers(hop, facts, rungs):
    """The hop's answers for the changes `rungs` declare, as facts."""
    got = answers(hop)
    for r in rungs:
        for c in r.changes:
            a, _ = answer_of(hop, c.name, got)
            if a:
                facts[decide.answer_key(c.name)] = a


def record_answer(hop, changes, answer):
    """An answer given at a terminal, kept for the hop's later runs and in
    its changelog."""
    import getpass
    try:
        who = getpass.getuser()
    except Exception:  # noqa: BLE001 - nobody to name
        who = ""
    rec = _record(hop)
    for c in changes:
        rec.setdefault("answers", {})[c.name] = {
            "answer": answer, "at": time.strftime("%Y-%m-%d %H:%M:%S"),
            "by": who, "source": _source(hop)}
    _save(hop, rec)
    _audit(hop, {"op": "source-change-answer", "answer": answer,
                 "changes": [c.name for c in changes]})


# --- the offer, in words -----------------------------------------------------

@dataclass
class Saving:
    """What a change buys, as the caller measured or estimated it."""
    faster: float = None     #: seconds on the way the change allows
    slower: float = None     #: seconds on the way without it
    how: str = ""            #: measured on this hop, or estimated from what
    instead: str = ""        #: the way without it, in words
    spared: str = ""         #: the load on the source the change spares


def took(secs):
    """A duration as a plan says it."""
    secs = max(float(secs or 0), 0)
    if secs >= 3600:
        return f"{secs / 3600:,.1f} hours"
    n, unit = (round(secs / 60), "minute") if secs >= 60 else (
        max(round(secs), 1), "second")
    return f"{n:,} {unit}{'' if n == 1 else 's'}"


def without(saving, change=None):
    """What not taking the faster way means: the next way, its time and
    its load on the source."""
    way = saving.instead or "the next way"
    if saving.slower is not None:
        way += f", about {took(saving.slower)}" + (
            f" ({saving.how})" if saving.how else "")
    if saving.spared:
        way += f" - {saving.spared}"
    if saving.instead:
        return way
    return change.definition.without if isinstance(change, Defined) else way


def offer_lines(offer, facts, saving):
    """The offer, as the plan says it and a terminal asks it."""
    names = ", ".join(c.name for c in offer.changes)
    out = [f"{offer.rung.said} is the faster way here, and it needs a change"
           " on the source - offered, never assumed:"]
    for c in offer.changes:
        make = "; ".join(c.statements(facts))
        undo = "; ".join(c.statements(facts, "undo"))
        out.append(f"  on the source: {c.said} put on ({make}), taken off"
                   f" again {c.after}" + (f" ({undo})" if undo else ""))
    if saving.faster is not None and saving.slower is not None:
        gain = saving.slower - saving.faster
        instead = saving.instead or "the next way"
        out.append(f"  it saves about {took(gain)}: about"
                   f" {took(saving.faster)} against about"
                   f" {took(saving.slower)} for {instead} ({saving.how})"
                   if gain > 0 else
                   f"  it saves no time here: about {took(saving.faster)}"
                   f" against about {took(saving.slower)} ({saving.how})")
    if saving.spared:
        out.append(f"  and the source is spared {saving.spared}")
    out.append(f"  without it: {without(saving, offer.changes[0])}")
    after = offer.changes[0].after
    out.append("  yes - migkit makes it, behind the hop's approvals"
               f" ({STEP}), records it, and takes it off again {after} or"
               " on any failure; no - the source is left as it is and the"
               " next way runs; dba - the statements go to the DBA's"
               " script, and the next way runs until the change is there")
    out.append(f"  answered ahead in the hop: {OPTION}: {{{names}: yes |"
               " no | dba}")
    return out


QUESTION = "change the source for this? yes / no / dba"


def settle(hop, offer, facts, saving, go, ask, log):
    """The offer said, and answered where it can be: asked at a terminal
    under `--go` (`ask(question)` -> "yes", "no" or "dba"), the answer
    recorded and put in the facts. Returns the answer, or None - and then
    the source is left as it is and the next way runs, said so."""
    for line in offer_lines(offer, facts, saving):
        log(line)
    if go and ask is not None:
        got = answer_word(ask(QUESTION))
        if got is None:
            got = "no"
        record_answer(hop, offer.changes, got)
        for c in offer.changes:
            facts[decide.answer_key(c.name)] = got
        log(f"answered {got}, recorded for this hop's later runs")
        return got
    if go:
        log("not answered - there is no terminal to ask at, and the hop"
            " answers nothing ahead: the source is left as it is, and"
            f" {saving.instead or 'the next way'} runs")
    else:
        log("dry-run: --go at a terminal asks it")
    return None


@dataclass(frozen=True)
class _Named:
    name: str


def settle_named(hop, name, lines, go, ask, log):
    """A change on the source that is no faster way's - the archived rows
    removed - offered in `lines` and answered as `settle` answers one:
    the hop's own answer for it by name (never a word given for every
    change), else asked at a terminal under `--go` and recorded. Returns
    the answer, or None: the source is left as it is."""
    got = answers(hop).get(name)
    if got is not None:
        log(f"{DEFINITIONS[name].said}: answered {got[0]} ({got[1]})")
        return got[0]
    for line in lines:
        log(line)
    log(f"  answered ahead in the hop: {OPTION}: {{{name}: yes | no}}")
    if go and ask is not None:
        answer = answer_word(ask(QUESTION)) or "no"
        record_answer(hop, [_Named(name)], answer)
        log(f"answered {answer}, recorded for this hop's later runs")
        return answer
    log("not answered - the source is left as it is" if go else
        "dry-run: --go at a terminal asks it")
    return None


def written(hop, entry):
    """A write on the source that is no setting (`archive`'s rows),
    recorded beside the changes in the hop's ledger and its changelog -
    in states of its own, so nothing that takes a change off again reads
    it as one."""
    rec = _record(hop)
    rec.setdefault("written", []).append(entry)
    _save(hop, rec)
    _audit(hop, {"op": "source-write", **{k: entry.get(k) for k in (
        "id", "change", "state", "table", "rows")}})


# --- the record -------------------------------------------------------------

FILE = "source-changes.json"


def _path(hop):
    return hop.report_dir() / FILE


def _record(hop):
    from . import config
    if "report_dir" not in vars(hop) \
            and not (config.REPORTS / str(hop.name) / FILE).exists():
        # nothing recorded: asked without making the hop's directory
        return {}
    try:
        got = json.loads(_path(hop).read_text())
    except (OSError, ValueError):
        return {}
    return got if isinstance(got, dict) else {}


def _save(hop, rec):
    import os
    path = _path(hop)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(rec, indent=1, default=str))
    os.replace(tmp, path)


def _audit(hop, entry):
    from . import audit
    audit.append(hop.report_dir() / "changelog.jsonl", entry)


def _source(hop):
    return f"{hop.source.host}:{hop.source.port}"


def refused_here(hop, name, version=""):
    """The server's refusal of `name` recorded for this source at this
    version - its words and when - or None: a server upgraded, or another
    source, is asked again."""
    got = (_record(hop).get("refused") or {}).get(name)
    if not got or got.get("source") != _source(hop) \
            or str(got.get("version") or "") != str(version or ""):
        return None
    return got


def hand_over(hop, rungs, facts):
    """The changes of `rungs` a DBA is to make - only a DBA can, or the
    answer is dba - kept for the DBA's script (`for_the_dba`), and the ones
    now there taken out of it. Only a rung whose own needs hold hands its
    changes over: one out for another reason needs nothing of the DBA."""
    rec = _record(hop)
    handed = dict(rec.get("handed") or {})
    before = dict(handed)
    for r in rungs:
        if not r.changes:
            # nothing of the source's to hand over, and nothing asked
            continue
        out = any(n.missing(r, facts) for n in r.needs)
        for c in r.changes:
            stand = None if out else c.stand(facts)
            if out or stand.state == decide.PRESENT:
                handed.pop(c.name, None)
                continue
            if stand.state == decide.BY_DBA or (
                    stand.state == decide.POSSIBLE
                    and c.answer(facts) == "dba"):
                handed[c.name] = {
                    "said": c.said, "why": stand.why or "answered dba",
                    "make": c.statements(facts),
                    "undo": c.statements(facts, "undo"), "after": c.after,
                    "outside": c.outside(facts) if isinstance(c, Defined)
                    else [],
                    "restart": c.restarts(facts) if isinstance(c, Defined)
                    else False,
                    "without": c.definition.without
                    if isinstance(c, Defined) else "",
                    "for": r.said, "at": time.strftime("%Y-%m-%d %H:%M")}
    if handed != before:
        rec["handed"] = handed
        _save(hop, rec)
    return handed


def for_the_dba(hop):
    """`dba.Need`s for the changes handed to the DBA, as the last plan that
    asked the server found them: the DBA's script carries them with the
    statements that take them off again."""
    from .dba import Need
    out = []
    for name, h in sorted((_record(hop).get("handed") or {}).items()):
        undo = list(h.get("undo") or [])
        said = list(h.get("outside") or [])
        if undo:
            said.append(f"taken off again {h.get('after') or 'after'}:"
                        f" {' '.join(undo)}")
        out.append(Need("source", f"{h.get('said', name)} (for"
                                  f" {h.get('for', 'a faster way')})", False,
                        f"{h.get('why') or 'handed to the DBA'} - as of"
                        f" {h.get('at', '?')}",
                        [decide.terminated(s) for s in h.get("make") or []],
                        said, restart=bool(h.get("restart")),
                        without=h.get("without") or ""))
    return out


# --- yes: made, and taken off again ------------------------------------------

class Refused(decide.Blocked):
    """The server refused a change migkit was allowed to make: recorded,
    and the way that needed it is not taken."""


#: a MySQL client's codes for a connection lost or never made: the server
#: said nothing about the change
LOST = (2003, 2006, 2013, 2055)


def refused_by_server(e):
    """Whether `e` is the server's own answer to a statement - an access
    denied, a library it cannot load - rather than a connection lost."""
    mod = (type(e).__module__ or "").split(".")[0]
    if mod not in decide._DRIVERS:
        return False
    code = e.args[0] if getattr(e, "args", None) and isinstance(
        e.args[0], int) else None
    return code not in LOST


def _gone(e):
    """An undo that found nothing to take off."""
    text = str(e).lower()
    return any(w in text for w in ("does not exist", "doesn't exist",
                                   "not exist", "not installed",
                                   "is not enabled", "not loaded"))


def _ledger(hop, entry):
    rec = _record(hop)
    rec.setdefault("made", []).append(entry)
    _save(hop, rec)
    _audit(hop, {"op": "source-change", **{k: entry[k] for k in (
        "id", "change", "state", "make", "undo")}})


def _state(hop, entry_id, state, **more):
    rec = _record(hop)
    for e in rec.get("made") or []:
        if e.get("id") == entry_id:
            e["state"] = state
            e.update(more)
            _audit(hop, {"op": "source-change", "id": entry_id,
                         "change": e.get("change"), "state": state, **more})
    _save(hop, rec)


def _take_off(hop, entry, run, log):
    """The change's undo statements run; a statement that finds nothing to
    take off counts as taken off. What cannot be is recorded as left on
    the source, said, and taken off by the next run (`undo_left`)."""
    try:
        for s in entry.get("undo") or []:
            try:
                run(s)
            except Exception as e:  # noqa: BLE001 - classified below
                if not _gone(e):
                    raise
    except Exception as e:  # noqa: BLE001 - said and recorded
        _state(hop, entry["id"], "left", error=decide._first(e))
        log(f"[red]{entry['said']} could not be taken off the source"
            f" ({decide._first(e)}); it is recorded, the next run takes"
            f" it off, and until then a DBA can: {'; '.join(entry['undo'])}"
            "[/red]")
        return False
    _state(hop, entry["id"], "undone")
    log(f"on the source: {entry['said']} taken off again")
    return True


def undo_left(hop, run, log):
    """A change an earlier run made and did not take off - killed in
    between, or its undo refused - taken off now. True where nothing is
    left."""
    ok = True
    for e in list(_record(hop).get("made") or []):
        if e.get("state") in ("making", "made", "left") \
                and e.get("source") == _source(hop):
            log(f"an earlier run left {e['said']} on the source; taking it"
                " off first")
            ok = _take_off(hop, e, run, log) and ok
    return ok


def left_on_source(hop):
    """The changes recorded as made and not taken off: for `doctor`."""
    return [e for e in _record(hop).get("made") or []
            if e.get("state") in ("making", "made", "left")]


@contextlib.contextmanager
def making(hop, change, facts, run, db, log, version=""):
    """`change` made on the source for the length of the block: behind the
    hop's approvals, recorded before it acts, taken off again after the
    block or on any failure. `run(statement)` runs one statement on the
    source. The server refusing it is recorded (`refused_here`) and raised
    as `Refused`, what it had made taken off first."""
    from . import approvals
    make = change.statements(facts)
    undo = change.statements(facts, "undo")
    approvals.require(hop, STEP, db, log, detail={
        "change": change.name, "make": make, "undo": undo})
    if not undo_left(hop, run, log):
        raise SystemExit("a change an earlier run made on the source could"
                         " not be taken off; nothing more is changed there"
                         " until it is")
    entry = {"id": f"{change.name}-{int(time.time() * 1000)}",
             "change": change.name, "said": change.said, "make": make,
             "undo": undo, "state": "making", "source": _source(hop),
             "db": db, "at": time.strftime("%Y-%m-%d %H:%M:%S")}
    _ledger(hop, entry)
    done = 0
    try:
        for s in make:
            run(s)
            done += 1
    except BaseException as e:
        if done:
            # the statements before the one that failed did their part
            _take_off(hop, entry, run, log)
        else:
            _state(hop, entry["id"], "not made", error=decide._first(e))
        if refused_by_server(e):
            why = decide._first(e)
            rec = _record(hop)
            rec.setdefault("refused", {})[change.name] = {
                "why": why, "at": time.strftime("%Y-%m-%d %H:%M:%S"),
                "source": _source(hop), "version": str(version or "")}
            _save(hop, rec)
            _audit(hop, {"op": "source-change-refused",
                         "change": change.name, "why": why})
            raise Refused(f"the source refused {change.said}: {why}")
        raise
    _state(hop, entry["id"], "made")
    log(f"on the source: {change.said} put on ({'; '.join(make)})")
    try:
        yield entry
    finally:
        _take_off(hop, entry, run, log)


def said_by_doctor(hop):
    """Lines for `doctor`: a change migkit made on the source and has not
    taken off, and what is handed to the DBA."""
    out = []
    for e in left_on_source(hop):
        out.append(f"{e['said']} was put on the source on {e.get('at')} and"
                   " not taken off yet; the next run takes it off, or a DBA:"
                   f" {'; '.join(e.get('undo') or [])}")
    for name, h in sorted((_record(hop).get("handed") or {}).items()):
        out.append(f"handed to the DBA: {h.get('said', name)} on the source,"
                   f" for {h.get('for', 'a faster way')}")
    return out
