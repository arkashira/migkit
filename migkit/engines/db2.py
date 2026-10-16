"""Db2 (LUW) as one side of a pair (backlog 34), through `ibm_db`.

A hop's database is a schema; the database to connect to is the
endpoint's `database` option. Names fold to capitals, `CHAR` values come
back padded and decimals at the scale they were stored with, all handled
as Oracle's are (`FoldsToCapitals`). Rows are compared through the
in-process renderer every engine without a canonical SQL rendering
shares.

Written 2026-09-25 without a server to run it against: IBM's Db2 image
runs on x86 only, and this machine is arm64.
"""
from .. import pieces
from . import hooks
from .base import Engine, Result
from .dbapi import MARK, DbapiRows, FoldsToCapitals
from .sqlcatalog import CatalogChecks

#: why the ways below are owed a run: settings, the server's own load,
#: accounts, statistics, the rollback point, fence, confirm and delta
#: (R13-DB2) and the tail through the source's own log (LT.R6), proved on
#: fakes and recorded transcripts only
UNPROVED_ON = "built; not run on a real Db2 LUW server"
#: the ways built here and run against no server: the whole list, the
#: matrix reads it from this source (`capabilities`)
UNPROVED_ITEMS = (
    "db2:tail.quiet "
    "assess.preflight cutover.counters cutover.drain cutover.freeze "
    "cutover.rollback move.bulk move.partitioned scope.copy "
    "scope.counters scope.listings scope.names scope.objects "
    "scope.repair scope.row-filter verify.access.object-ownership verify.confirm "
    "verify.delta verify.fence verify.identity.sequence-collision verify.identity.sequence-parity "
    "verify.movement.target-ahead verify.repair verify.repair-undo verify.structure.collection-options "
    "verify.structure.columns verify.structure.foreign-keys verify.structure.indexes verify.structure.keys "
    "verify.structure.materialized-views verify.structure.objects verify.structure.partitions verify.structure.row-security "
    "verify.structure.schema-diff verify.structure.sharding verify.structure.triggers verify.structure.unvalidated-constraints "
    "verify.table verify.users verify.value.charset verify.value.collation "
    "verify.value.generated verify.value.narrowing verify.value.null-empty verify.value.precision "
    "verify.value.rendering verify.value.timezone tail.follows tail.table-states scope.ops")

#: a column's type: its own, or for a distinct type the built-in type it
#: is made from (`syscat.datatypes`), whose values it holds - a distinct
#: type was a name no other engine knew, and left the column unmapped
_TYPE = ("coalesce((select d.sourcename from syscat.datatypes d where"
         " d.typeschema = c.typeschema and d.typename = c.typename and"
         " d.metatype = 'T'), c.typename)")

#: the declared type of a column of `syscat.columns c`, with the numbers a
#: counterpart needs: a DECFLOAT's digits, 16 or 34, by the 8 or 16 bytes
#: its catalogue gives as its length
DECLARED = ("case when {t} in ('DECIMAL', 'NUMERIC') then {t}"
            " || '(' || c.length || ',' || c.scale || ')'"
            " when {t} in ('CHARACTER', 'VARCHAR', 'GRAPHIC',"
            " 'VARGRAPHIC') and c.codepage = 0 then 'VARBINARY(' || c.length"
            " || ')'"
            " when {t} in ('CHARACTER', 'VARCHAR', 'GRAPHIC',"
            " 'VARGRAPHIC') then {t} || '(' || c.length || ')'"
            " when {t} = 'TIMESTAMP' then 'TIMESTAMP(' || c.scale || ')'"
            " when {t} = 'DECFLOAT' then 'DECFLOAT(' || case when c.length"
            " = 8 then '16' else '34' end || ')'"
            " else {t} end").format(t=_TYPE)


#: the engine each Db2 platform is, and how it is said
PLATFORM_ENGINE = {"luw": "db2", "zos": "db2zos", "i": "db2i"}
PLATFORM_WORDS = {"luw": "Db2 for Linux, UNIX and Windows",
                  "zos": "Db2 for z/OS", "i": "Db2 for i (IBM i)"}


def platform_of(dbms_name):
    """The Db2 platform a server is, by the product name its driver
    reports: `DB2/<system>` for Linux, UNIX and Windows (`DB2/LINUXX8664`),
    `DB2` for z/OS (product DSN), `AS` over DRDA and `DB2/400 SQL` over IBM
    i's own ODBC driver for Db2 for i (product QSQ); None for a name none of
    them gives. Taken from IBM's documentation of each driver - proved on
    none of these servers here."""
    n = str(dbms_name or "").strip().upper()
    if n.startswith(("DB2/400", "AS/400", "AS ", "DB2 FOR I")) or n == "AS" \
            or n.startswith("QSQ"):
        return "i"
    if n.startswith("DB2/"):
        return "luw"
    if n in ("DB2", "DSN") or n.startswith(("DSN", "DB2 FOR Z")):
        return "zos"
    return None


def licence_refused(error):
    """What a connection refused for want of a Db2 Connect licence
    (SQL1598N) means, in words; None for any other failure."""
    if "SQL1598N" not in str(error):
        return None
    return ("the server answers only a client licensed for Db2 Connect"
            " (SQL1598N): the operator's licence file in the IBM data server"
            " driver's licence directory - migkit doctor --install puts it"
            " there once its terms are accepted, from the file"
            " MIGKIT_DB2_CONNECT_LICENSE names - or the server activated"
            " for Db2 Connect by its own administrator")


def db2_connect_licence():
    """The Db2 Connect licence file in place beside the IBM data server
    driver `ibm_db` runs on, or None."""
    import glob
    import os
    try:
        import ibm_db
    except ImportError:
        return None
    here = os.path.dirname(os.path.abspath(ibm_db.__file__))
    found = sorted(glob.glob(os.path.join(here, "clidriver", "license",
                                          "db2consv_*.lic")))
    return found[0] if found else None


def large_values(sample=None):
    """What Db2 does with its large values (`pieces`), on each platform: a
    value's length as the server keeps it (`LENGTH`, off a LOB's
    descriptor), a byte value past a piece read by `SUBSTR` of it; a LOB
    is bound whole by the driver when written, and a wide value is hashed
    here as it is folded - no platform's MD5 takes every length. `sample`
    reads a share of a large table, where the platform has one."""
    return pieces.Sql(
        "sized by the server, a byte value read a piece at a time",
        length="length({c})",
        types=("blob", "clob", "dbclob", "varchar", "varbinary", "varbin",
               "vargraphic", "long varchar", "long vargraphic"),
        piece="substr({c}, {at}, {n})", sample=sample)


def _decfloat(text):
    """A DECFLOAT from the server's own text of it (`char`), which writes
    every digit it holds and its NaN and infinities by name: the driver's
    reading of one went through a double."""
    from decimal import Decimal
    return Decimal(str(text).strip())


def _twelve(text):
    """A Db2 timestamp's text of twelve digits after the second as the
    value, nine kept; one with a digit past the ninth, which no other
    engine holds and no rendering here writes, as uncomparable - never
    as the value cut to nine, which would meet a target holding the cut
    value."""
    from .. import canon, nanotime
    head, _, frac = str(text).partition(".")
    if frac[9:].strip("0"):
        return canon.uncomparable(f"{head}.{frac}")
    return nanotime.parse(f"{head}.{frac[:9]}" if frac else head)


def granted(table, schema_col, name_col, privileges, where=""):
    """One row a privilege held, of a Db2 `*AUTH` catalogue whose columns
    say each privilege as `Y` or `G` (granted, and granted with the right
    to grant it on) - every Db2 platform keeps them so."""
    return " union all ".join(
        f"select {name_col}, grantee, '{word}' from {table} where"
        f" {schema_col} = {{schema}} and {col} in ('Y', 'G'){where}"
        for col, word in privileges)


#: Db2 for Linux, UNIX and Windows's catalogue (`SYSCAT`), as IBM documents
#: it, answering `sqlcatalog.QUESTIONS`
CATALOGUE = {
    "keyless": (
        "select t.tabname from syscat.tables t where t.tabschema = {schema}"
        " and t.type = 'T' and not exists (select 1 from syscat.tabconst c"
        " where c.tabschema = t.tabschema and c.tabname = t.tabname and"
        " c.type in ('P', 'U')) and not exists (select 1 from"
        " syscat.indexes i where i.tabschema = t.tabschema and i.tabname ="
        " t.tabname and i.uniquerule in ('P', 'U')) order by 1"),
    "fks": (
        "select r.tabname, r.constname, k.colname, r.reftabname, p.colname,"
        " c.enforced from syscat.references r join syscat.tabconst c on"
        " c.tabschema = r.tabschema and c.tabname = r.tabname and"
        " c.constname = r.constname join syscat.keycoluse k on k.tabschema"
        " = r.tabschema and k.tabname = r.tabname and k.constname ="
        " r.constname join syscat.keycoluse p on p.tabschema ="
        " r.reftabschema and p.tabname = r.reftabname and p.constname ="
        " r.refkeyname and p.colseq = k.colseq where r.tabschema = {schema}"
        " order by r.tabname, r.constname, k.colseq"),
    # Db2 keeps no switch to turn a trigger off: one a dropped dependency
    # left inoperative (`VALID` X) is the one that does not fire
    "triggers": (
        "select tabname, trigname, case when valid = 'Y' then 'Y' else 'N'"
        " end from syscat.triggers where tabschema = {schema} order by 1,"
        " 2"),
    "indexes": (
        "select i.tabname, i.indname, case when i.uniquerule in ('U', 'P')"
        " then 'Y' else 'N' end, u.colname, case when exists (select 1"
        " from sysibmadm.admintabinfo a where a.tabschema = i.tabschema and"
        " a.tabname = i.tabname and a.indexes_require_rebuild = 'Y') then"
        " 'N' else 'Y' end from syscat.indexes i join syscat.indexcoluse u"
        " on u.indschema = i.indschema and u.indname = i.indname where"
        " i.tabschema = {schema} order by i.tabname, i.indname, u.colseq"),
    "checks": (
        "select c.tabname, c.constname, case c.type when 'F' then 'foreign"
        " key' when 'K' then 'check' when 'U' then 'unique' else 'primary"
        " key' end, case when c.enforced = 'N' then 'not enforced' when"
        " t.status = 'C' then 'set integrity pending' else '' end from"
        " syscat.tabconst c join syscat.tables t on t.tabschema ="
        " c.tabschema and t.tabname = c.tabname where c.tabschema ="
        " {schema} and c.type in ('F', 'K', 'U', 'P') order by 1, 2"),
    # a partition's rows are counted by DATAPARTITIONNUM, which names its
    # SEQNO (`_partition_rows`)
    "partitions": (
        "select p.tabname, p.datapartitionname, coalesce(p.lowvalue, '') ||"
        " ' - ' || coalesce(p.highvalue, ''), p.seqno from"
        " syscat.datapartitions p join syscat.tables t on t.tabschema ="
        " p.tabschema and t.tabname = p.tabname where p.tabschema = {schema}"
        " and t.partition_mode = 'R' order by 1, 4"),
    "matviews": (
        "select tabname, case status when 'C' then 'not populated (set"
        " integrity pending)' when 'X' then 'inoperative' else '' end, ''"
        " from syscat.tables where tabschema = {schema} and type = 'S'"
        " order by 1"),
    "rls": (
        "select c.tabname, c.controlname || case c.controltype when 'R'"
        " then ' (row permission)' else ' (column mask)' end, case when"
        " c.enable = 'Y' and ((c.controltype = 'R' and t.control in ('R',"
        " 'B')) or (c.controltype = 'C' and t.control in ('C', 'B'))) then"
        " 'Y' else 'N' end from syscat.controls c join syscat.tables t on"
        " t.tabschema = c.tabschema and t.tabname = c.tabname where"
        " c.tabschema = {schema} and c.controltype in ('R', 'C') order by"
        " 1, 2"),
    "generated": (
        "select tabname, colname, case when rowchangetimestamp = 'Y' then"
        " 'row change timestamp' when rowbegin = 'Y' then 'row begin' when"
        " rowend = 'Y' then 'row end' when transactionstartid = 'Y' then"
        " 'transaction start id' else generated || ' ' ||"
        " cast(substr(text, 1, 4000) as varchar(4000)) end from"
        " syscat.columns where tabschema = {schema} and generated <> ' '"
        " and identity = 'N' order by 1, 2"),
    "collation": (
        "select tabname, colname, collationschema || '.' || collationname"
        " from syscat.columns where tabschema = {schema} and collationname"
        " is not null order by 1, 2"),
    "charset": (
        "select tabname, colname, cast(codepage as varchar(10)) from"
        " syscat.columns where tabschema = {schema} and typename in"
        " ('CHARACTER', 'VARCHAR', 'LONG VARCHAR', 'CLOB', 'GRAPHIC',"
        " 'VARGRAPHIC', 'LONG VARGRAPHIC', 'DBCLOB') order by 1, 2"),
    "owners": (
        "select 'table', tabname, owner from syscat.tables where tabschema"
        " = {schema} and type in ('T', 'S') union all select 'view',"
        " tabname, owner from syscat.tables where tabschema = {schema} and"
        " type = 'V' union all select case routinetype when 'P' then"
        " 'procedure' else 'function' end, routinename, owner from"
        " syscat.routines where routineschema = {schema} and origin <> 'S'"
        " union all select 'sequence', seqname, owner from syscat.sequences"
        " where seqschema = {schema} and seqtype = 'S'"),
    "grants": granted("syscat.tabauth", "tabschema", "tabname", (
        ("controlauth", "CONTROL"), ("alterauth", "ALTER"),
        ("deleteauth", "DELETE"), ("indexauth", "INDEX"),
        ("insertauth", "INSERT"), ("refauth", "REFERENCES"),
        ("selectauth", "SELECT"), ("updateauth", "UPDATE"))),
    "seq_grants": granted("syscat.sequenceauth", "seqschema", "seqname", (
        ("usageauth", "USAGE"), ("alterauth", "ALTER"))),
    # the first value of the server's next cache: no value at or past it
    # has been handed out
    "sequences": (
        "select seqname, nextcachefirstvalue from syscat.sequences where"
        " seqschema = {schema} and seqtype = 'S'"),
    "identity": (
        "select tabname, colname, nextcachefirstvalue from"
        " syscat.colidentattributes where tabschema = {schema}"),
    "objects": (
        "select 'view', tabname, '' from syscat.tables where tabschema ="
        " {schema} and type = 'V' union all select 'alias', tabname, '' from"
        " syscat.tables where tabschema = {schema} and type = 'A' union all"
        " select case routinetype when 'P' then 'procedure' else 'function'"
        " end, specificname, '' from syscat.routines where routineschema ="
        " {schema} and origin <> 'S' union all select 'trigger', trigname,"
        " tabname from syscat.triggers where tabschema = {schema} union all"
        " select 'sequence', seqname, '' from syscat.sequences where"
        " seqschema = {schema} and seqtype = 'S'"),
    "options": " union all ".join(
        f"select tabname, '{word}', {col} from syscat.tables where tabschema"
        " = {schema} and type = 'T'" for word, col in (
            ("organize by", "tableorg"), ("compression", "compression"),
            ("row compression", "rowcompmode"), ("append", "append_mode"),
            ("volatile", "volatile"), ("data capture", "datacapture"),
            ("lock size", "locksize"),
            ("not logged initially", "log_attribute"))),
    # the distribution key of a database partitioned across members; a
    # table with none lives on one
    "spread": (
        "select tabname, listagg(colname, ',') within group (order by"
        " partkeyseq) from syscat.columns where tabschema = {schema} and"
        " partkeyseq > 0 group by tabname"),
    "ids": (
        "select tabname, cast(tbspaceid as varchar(10)) || '.' ||"
        " cast(tableid as varchar(10)) from syscat.tables where tabschema"
        " = {schema} and type = 'T'"),
}


class Db2Engine(CatalogChecks, FoldsToCapitals, DbapiRows, Engine):
    checks = ("schema", "counts", "autoinc", "data")
    #: values one statement may carry: at most 32,767 host-variable or
    #: parameter-marker references (Db2 docs, SQL and XML limits)
    STATEMENT_VALUES = 32767

    CATALOGUE = CATALOGUE
    #: the types whose text the server writes in a way of its own
    RENDERED = ("xml", "decfloat")
    CANON_ENGINE = "db2"
    #: the position is read off the log and nothing is consumed by asking
    #: (`engines.db2_log`), so a copy's start may be taken before it
    CHANGE_POINT_READS_ONLY = True
    CHARSETS = True
    SQL_DIALECT = None
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " a Db2 one nobody has run")
    OVER_NETWORK = True
    PARAMSTYLE = "qmark"
    LIMIT = "fetch"
    PADDED = ("CHARACTER", "CHAR", "GRAPHIC")
    SCALED = ("DECIMAL", "NUMERIC")
    #: large values: Linux, UNIX and Windows sizes a large table over a
    #: sample of its pages
    LARGE_VALUES = large_values("tablesample system ({pct})")

    #: the log pages the database's logger wrote, every member summed
    TAIL_WROTE_SQL = ("select sum(log_writes)"
                      " from table(mon_get_transaction_log(-2))")

    def tail_where(self, db, token):
        """What the tail's quiet line stands on (`tailwhere.where_from`): the
        tail's position, the source's now (`log_position`) and the counter
        `TAIL_WROTE_SQL` read since the hop's last carried change, in one
        statement - unread where the server refuses it. The source lists
        no reader of the tail's to ask: attached is said unread."""
        from .. import tailwhere

        def wrote():
            got = self._rows("src", db, self.TAIL_WROTE_SQL)
            return got[0][0] if got else None
        return tailwhere.where_from(self, db, token, wrote, "log pages")

    def _nanos_text(self, quoted, declared):
        """A `TIMESTAMP(p)` of more than six digits as the server writes
        it, every digit kept: the driver hands a timestamp over as a
        Python datetime, six digits. Db2 keeps up to twelve; `FF12` writes
        them all, and a value with a digit past the ninth - which no other
        engine holds - is read as uncomparable (`_twelve`)."""
        from .. import canon
        name, nums = canon._split_declared(declared)
        digits = nums[0] if nums else 6
        if name != "timestamp" or digits <= 6:
            return None
        return (f"varchar_format({quoted}, 'YYYY-MM-DD HH24:MI:SS."
                f"FF{12 if digits > 9 else 9}')")

    def _read_as(self, quoted, declared):
        """A TIMESTAMP of more than nine digits (`_twelve`), a DECFLOAT as
        its own text (`_decfloat`), an XML as its document's text."""
        from .. import canon
        name, nums = canon._split_declared(declared)
        if name == "timestamp" and nums and nums[0] > 9:
            return (self._nanos_text(quoted, declared), _twelve)
        if name == "decfloat":
            return (f"rtrim(char({quoted}))", _decfloat)
        if name == "xml":
            return (f"xmlserialize({quoted} as clob(2G))", str)
        return None

    #: the platform this engine's hops are on, as `platform_of` names it
    PLATFORM = "luw"
    #: the port a DRDA server listens on where the endpoint names none
    DRDA_PORT = 50000

    def _connect(self, side, db):
        import ibm_db_dbi
        ep = self.hop.source if side == "src" else self.hop.target
        name = ep.options.get("database")
        if not name:
            raise SystemExit(f"the {'source' if side == 'src' else 'target'}"
                             " needs `database`, the Db2 database to connect"
                             " to; the hop's databases are its schemas")
        try:
            conn = ibm_db_dbi.connect(
                f"DATABASE={name};HOSTNAME={ep.host};"
                f"PORT={ep.port or self.DRDA_PORT};PROTOCOL=TCPIP;"
                f"UID={ep.user};PWD={ep.password};CONNECTTIMEOUT=15;", "", "")
        except Exception as e:  # noqa: BLE001 - said in migkit's words
            why = licence_refused(e)
            if why:
                raise SystemExit(why) from None
            raise
        return self._confirmed(conn, side, conn.server_info()[0])

    def _confirmed(self, conn, side, dbms_name):
        """The connection, once the server says it is the platform this
        engine is for - asked once a side. A Db2 of another platform has
        another catalogue and another log, and is its own engine."""
        seen = self.__dict__.setdefault("_platforms", {})
        if side not in seen:
            seen[side] = platform_of(dbms_name)
        got = seen[side]
        if got is not None and got != self.PLATFORM:
            conn.close()
            raise SystemExit(
                f"the {'source' if side == 'src' else 'target'} is"
                f" {PLATFORM_WORDS[got]} ({dbms_name}), and this hop names"
                f" it {self.CANON_ENGINE}: name its engine"
                f" {PLATFORM_ENGINE[got]}")
        return conn

    def databases(self):
        return list(self.hop.databases)

    def neutral_tables(self, side, db):
        return [t for t in (self._said(n.strip()) for (n,) in self._rows(
            side, db, "select tabname from syscat.tables where tabschema ="
                      f" {MARK} and type = 'T' order by 1",
            (self._owner(side, db),)))
            if self.in_scope(db, t)]

    def neutral_columns(self, side, db, table):
        return [(self._said(n.strip()), t.strip()) for n, t in self._rows(
            side, db, f"select c.colname, {DECLARED} from syscat.columns c"
                      f" where c.tabschema = {MARK} and c.tabname = {MARK}"
                      " order by c.colno",
            (self._owner(side, db), self._q(table)[1:-1]))]

    def neutral_key(self, side, db, table):
        return [self._said(n.strip()) for (n,) in self._rows(
            side, db, "select k.colname from syscat.tabconst c join"
                      " syscat.keycoluse k on k.constname = c.constname and"
                      " k.tabschema = c.tabschema and k.tabname = c.tabname"
                      f" where c.tabschema = {MARK} and c.tabname = {MARK}"
                      " and c.type = 'P' order by k.colseq",
            (self._owner(side, db), self._q(table)[1:-1]))]

    def table_facts(self, side, db):
        out = {}
        for name, card in self._rows(
                side, db, "select tabname, card from syscat.tables where"
                          f" tabschema = {MARK} and type = 'T'",
                (self._owner(side, db),)):
            out[self._said(name.strip())] = {
                "rows": None if card is None or card < 0 else card,
                "bytes": None, "key": None}
        return out

    def check_schema(self, db):
        verifiers = hooks.workflow("verifiers")
        return verifiers.compare_schema(self._as_pair(), db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def check_deep(self, db):
        """What a load left pending on the target (`_pending`, each
        platform's own), text stored twice as UTF-8 on the source, and
        every question Db2's catalogue answers (`sqlcatalog`): keys,
        foreign keys and their orphans, triggers, indexes, constraints not
        held to their rows, partitions, materialized query tables, row
        permissions and column masks, generated columns, the columns and
        the whole structure, the objects beside the tables, each table's
        options and distribution, collations and code pages, NULL against
        empty text, floats, narrower columns, shifted timestamps, the
        server's own renderings, keys past the source's, owners and
        grants."""
        return self._pending(db) + [self._mojibake(db)] + self._deep(
            db, self._keyless, self._foreign_keys_found,
            self._triggers_found, self._indexes_found,
            self._constraints_found, self._partitions_found,
            self._matviews_found, self._row_security_found,
            self._generated_found, self._columns_found,
            self._structure_found, self._objects_found,
            self._table_options_found, self._spread_found,
            self._collations_found, self._charsets_found,
            self._null_empty_found, self._floats_found,
            self._narrowing_found, self._timeshift_found,
            self._rendering_found, self._boundary_found,
            self._owners_found, self._grants_found)

    #: what `assess` runs before anything moves: the checks whose answer
    #: is about the move to come - a table with no key, a column the
    #: target declares narrower, text stored twice, a policy filtering
    #: what migkit reads, a collation or code page the target sorts or
    #: holds otherwise
    PREFLIGHT = ("_keyless", "_narrowing_found", "_mojibake",
                 "_row_security_found", "_collations_found")

    def _partition_rows(self, side, db, table, partition, said):
        """A partition's rows, counted by the server: DATAPARTITIONNUM
        names the partition (its SEQNO) each row is in - one scan a
        table, asked once."""
        known = self.__dict__.setdefault("_partition_counts", {})
        at = (side, db, str(table))
        if at not in known:
            first = self._q(self.neutral_columns(side, db, table)[0][0])
            known[at] = {int(n): int(c) for n, c in self._rows(
                side, db, f"select datapartitionnum({first}), count(*) from"
                          f" {self._qualified(side, db, table)} group by"
                          f" datapartitionnum({first})")}
        return known[at].get(int(said), 0)

    def _as_text(self, quoted, declared):
        """An XML value as the server serializes it, any other as its
        VARCHAR."""
        if str(declared).split("(")[0].strip().upper() == "XML":
            return f"xmlserialize({quoted} as clob(1M))"
        return f"varchar({quoted})"

    def _restart_sql(self, db, name, value):
        """A sequence, or an identity column (`table.column`), set to hand
        out `value` next."""
        owner = self._q(self._owner("dst", db))
        table, _, column = str(name).rpartition(".")
        if table:
            return (f"alter table {owner}.{self._q(table)} alter column"
                    f" {self._q(column)} restart with {int(value)}")
        return (f"alter sequence {owner}.{self._q(name)} restart with"
                f" {int(value)}")

    def _held_in_scope(self, db, rows):
        """The rows of a target listing whose first value is a table the
        hop takes (`in_scope`): a table it leaves out is the target's own,
        whatever state a load of someone else's left it in."""
        return [r for r in rows
                if self.in_scope(db, self._said(str(r[0]).strip()))]

    def _pending(self, db):
        """Tables a load left in check-pending or unavailable on the
        target: Db2 refuses reads of them, or answers from a table whose
        constraints nobody checked."""
        try:
            held = self._rows("dst", db, "select tabname, status,"
                                         " access_mode from syscat.tables"
                                         f" where tabschema = {MARK} and"
                                         " type = 'T' and (status <> 'N'"
                                         " or access_mode <> 'F')",
                              (self._owner("dst", db),))
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} target tables", "error",
                           "could not read the target's tables:"
                           f" {str(e).splitlines()[0][:90]}")]
        held = self._held_in_scope(db, held)
        if held:
            return [Result("deep", f"{db} target tables", "diff",
                           f"{len(held)} tables not in normal state:"
                           f" {', '.join(r[0].strip() for r in held[:6])}"
                           " - left pending by a load", "",
                           "set integrity for those tables immediate"
                           " checked")]
        return [Result("deep", f"{db} target tables", "ok",
                       "every table in normal state")]

    # ---- settings, accounts, statistics, the rollback point, the server's
    # ---- own load and the rows changed (backlog R13-DB2). Built on what
    # ---- IBM documents, not run on a server (`UNPROVED_ON`) -----------------

    #: the settings that change what a value is or how it compares: the
    #: code page and territory, string units, rounding and the
    #: compatibility switches that make '' NULL or a DATE a timestamp
    CRITICAL_PARAMS = ("codepage", "codeset", "territory", "string_units",
                       "decflt_rounding", "date_compat", "varchar2_compat",
                       "number_compat", "dec_to_char_fmt", "dft_sqlmathwarn",
                       "extended_row_sz", "alt_collate")
    #: every setting of the database (on this member) and of its instance
    SETTINGS = ("select name, value from sysibmadm.dbcfg where member ="
                " current member union all select 'instance.' || name,"
                " value from sysibmadm.dbmcfg")

    def _db2_settings(self, side, db):
        """{setting: value} of `side`, or what stopped the reading."""
        try:
            return {str(n).strip(): None if v is None else str(v).strip()
                    for n, v in self._rows(side, db, self.SETTINGS)}
        except Exception as e:  # noqa: BLE001 - said, as the reading's
            return {self.UNREADABLE: str(e).splitlines()[0][:160]
                    if str(e) else type(e).__name__}

    def check_params(self, db):
        """Both servers' settings through the report every engine's go
        through: one that changes what a value is or how it compares
        (`CRITICAL_PARAMS`) fails it. A target of another engine keeps no
        Db2 settings: said, its values compared by check."""
        from . import same_engine
        if not same_engine(self):
            return [Result("params", f"{db} params", "skip",
                           "the target is another engine: its settings are"
                           " not Db2's, and check compares the values")]
        return self._param_result(
            db, self._db2_settings("src", db), self._db2_settings("dst", db),
            self.CRITICAL_PARAMS, "set the target database's configuration"
            " as the source's (its code page and territory are fixed when"
            " it is created)")

    # accounts: Db2 keeps no passwords - an account signs in through the
    # operating system or a directory - so what is carried is who holds
    # which authority, role and grant
    ACCOUNT_PASSWORDS = ("Db2 keeps no passwords: an account signs in"
                         " through the operating system or a directory,"
                         " where it is made; its authorities, roles and"
                         " grants are carried")
    #: (grantee, type, authority, '') the database grants
    AUTHORITIES = " union all ".join(
        f"select grantee, granteetype, '{w}', '' from syscat.dbauth where"
        f" {c} = 'Y'" for c, w in (
            ("connectauth", "CONNECT"), ("dbadmauth", "DBADM"),
            ("dataaccessauth", "DATAACCESS"),
            ("accessctrlauth", "ACCESSCTRL"), ("secadmauth", "SECADM"),
            ("sqladmauth", "SQLADM"), ("loadauth", "LOAD"),
            ("createtabauth", "CREATETAB"), ("bindaddauth", "BINDADD"),
            ("implschemaauth", "IMPLICIT_SCHEMA")))
    #: (grantee, type, privilege, table) of a schema's tables
    TABLE_GRANTS = " union all ".join(
        f"select grantee, granteetype, '{w}', tabname from syscat.tabauth"
        f" where tabschema = {{schema}} and {c} in ('Y', 'G')" for c, w in (
            ("controlauth", "CONTROL"), ("alterauth", "ALTER"),
            ("deleteauth", "DELETE"), ("indexauth", "INDEX"),
            ("insertauth", "INSERT"), ("refauth", "REFERENCES"),
            ("selectauth", "SELECT"), ("updateauth", "UPDATE")))
    #: (grantee, type, role) held, and every role
    ROLES_HELD = "select grantee, granteetype, rolename from syscat.roleauth"
    ROLES = "select rolename from syscat.roles"
    #: the authority that lets an account sign in; None: every one can
    LOGIN = "CONNECT"
    #: a database-wide authority given
    DB_GRANT = "grant {priv} on database to {who}"

    @staticmethod
    def _server_own(name):
        name = str(name).strip().upper()
        return name.startswith("SYS") or name == "PUBLIC"

    def accounts(self, side):
        """The accounts and roles of `side` in `Engine.accounts`' shape:
        roles held, the database's authorities as `<AUTHORITY> ON *.*`,
        the hop's tables' privileges; no password (`ACCOUNT_PASSWORDS`).
        A group is the directory's, and is left to it."""
        dbs = list(self.hop.databases or [])
        if not dbs:
            return {}
        out = {}

        def acct(kind, name):
            key = f"{'role' if str(kind).strip() == 'R' else 'user'}:{name}"
            return out.setdefault(key, {
                "login": self.LOGIN is None and str(kind).strip() != "R",
                "roles": [], "grants": [], "hash": None, "attrs": {}})
        for (r,) in self._rows(side, dbs[0], self.ROLES) if self.ROLES \
                else ():
            if not self._server_own(r):
                acct("R", str(r).strip())
        rows = [(g, k, p, "", None) for g, k, p, _ in self._rows(
            side, dbs[0], self.AUTHORITIES)]
        for db in dbs:
            rows += [(g, k, p, t, db) for g, k, p, t in self._rows(
                side, db, self.TABLE_GRANTS.replace(
                    "{schema}", self._schema_literal(side, db)))]
        for g, kind, priv, table, db in rows:
            if self._server_own(g) or str(kind).strip() == "G":
                continue
            a = acct(kind, str(g).strip())
            if priv == self.LOGIN:
                a["login"] = True
            elif db is None:
                a["grants"].append(f"{priv} ON *.*")
            else:
                a["grants"].append(
                    f"{priv} ON {db}.{self._said(str(table).strip())}")
        for g, kind, role in self._rows(side, dbs[0], self.ROLES_HELD) \
                if self.ROLES_HELD else ():
            if not self._server_own(g) and str(kind).strip() != "G":
                acct(kind, str(g).strip())["roles"].append(str(role).strip())
        for a in out.values():
            a["roles"], a["grants"] = sorted(set(a["roles"])), sorted(
                set(a["grants"]))
        return out

    def _grantee(self, kind, name):
        return f"{'role' if kind == 'role' else 'user'} {self._q(name)}"

    def account_steps(self, src, dst, passwords):
        """What makes the target's roles, roles held and grants the
        source's; an account the target lacks is the operating system's
        or the directory's to make (skipped, said), its grants given all
        the same - Db2 grants to a name it does not yet know."""
        steps, skipped = [], []

        def step(key, run, undo, made=False):
            steps.append({"key": key, "shown": run, "run": run,
                          "undo": undo, "made": made, "given": False})
        for key, a in sorted(src.items()):
            kind, _, name = key.partition(":")
            have = dst.get(key) or {"roles": [], "grants": []}
            who = self._grantee(kind, name)
            if key not in dst:
                if kind == "role":
                    step(key, f"create role {self._q(name)}",
                         f"drop role {self._q(name)}", made=True)
                else:
                    skipped.append((key, "Db2 keeps no passwords: make the"
                                         " account where the target signs"
                                         " its users in; its grants are"
                                         " given here"))
            for r in a.get("roles", []):
                if r not in have["roles"]:
                    step(key, f"grant role {self._q(r)} to {who}",
                         f"revoke role {self._q(r)} from {who}")
            for g in a.get("grants", []):
                if g in have["grants"]:
                    continue
                priv, _, on = g.partition(" ON ")
                where, _, table = on.partition(".")
                if where == "*":
                    run = self.DB_GRANT.format(priv=priv.lower(), who=who)
                    undo = run.replace("grant ", "revoke ", 1).replace(
                        " to ", " from ", 1)
                else:
                    obj = (f"{self._q(self._owner('dst', where))}."
                           f"{self._q(table)}")
                    run = f"grant {priv.lower()} on table {obj} to {who}"
                    undo = f"revoke {priv.lower()} on table {obj} from {who}"
                step(key, run, undo)
        return steps, skipped

    def account_run(self, what):
        conn = self._connect("dst", (self.hop.databases or [""])[0])
        try:
            self._run(conn.cursor(), what)
            conn.commit()
        finally:
            conn.close()

    # statistics: the catalogue's count and indexes of each table, read on
    # the target, decide each (`decide.db2_statistics`)
    STATS_FACTS = ("select t.tabname, t.card, (select count(*) from"
                   " syscat.indexes i where i.tabschema = t.tabschema and"
                   " i.tabname = t.tabname) from syscat.tables t where"
                   f" t.tabschema = {MARK} and t.type = 'T'")

    def _runstats(self, cur, db, table, path, row):
        """The server's own statistics of one table, at the depth chosen:
        RUNSTATS through the administrative command."""
        name = self._qualified("dst", db, table)
        how = (" with distribution and detailed indexes all"
               if path == "distribution" else "")
        self._run(cur, f"call sysproc.admin_cmd({MARK})",
                  (f"runstats on table {name}{how}",))

    def settle_target(self, db, from_source=True):
        """Each table the move wrote whose catalogue statistics stand
        further from its rows than `STALE_STATS_RATIO` gathered again, with
        its distribution and indexes where it has indexes (`decide.
        db2_statistics`)."""
        from .. import decide
        tables = self.neutral_tables("dst", db)
        if not tables:
            return None
        facts = {self._said(str(r[0]).strip()): r for r in self._rows(
            "dst", db, self.STATS_FACTS, (self._owner("dst", db),))}
        done = {}
        conn = self._connect("dst", db)
        try:
            cur = conn.cursor()
            for t in tables:
                row = facts.get(t) or (t, None, 0)
                self._run(cur, "select count_big(*) from"
                               f" {self._qualified('dst', db, t)}")
                got = decide.db2_statistics({
                    "rows": int(cur.fetchall()[0][0]),
                    "card": None if row[1] is None else int(row[1]),
                    "indexes": int(row[2] or 0),
                    "ratio": self.STALE_STATS_RATIO})
                if got.path != "current":
                    self._runstats(cur, db, t, got.path, row)
                    done[got.path] = done.get(got.path, 0) + 1
            conn.commit()
        finally:
            conn.close()
        if not done:
            return f"statistics current on {len(tables)} tables"
        words = {"distribution": "with distribution and indexes",
                 "table": "rows"}
        return "statistics gathered on " + ", ".join(
            f"{n} tables ({words[p]})" for p, n in sorted(done.items()))

    # the rollback point: the target's counters read together at
    # repeatable read, with the log's position where it says it
    #: where the log is now, the highest of every member
    LOG_POSITION = ("select max(current_lsn) from"
                    " table(mon_get_transaction_log(-2)) as t")

    def _counter_sql(self, db):
        """(statement, columns naming a counter) of the target's sequences
        and identity columns, each at repeatable read."""
        return [(self._question("dst", db, q) + " with rr", n)
                for q, n in (("sequences", 1), ("identity", 2))]

    def log_position(self, side, db):
        """The log sequence number the server is at, as an integer."""
        if not self.LOG_POSITION:
            return None
        got = self._rows(side, db, self.LOG_POSITION)
        value = got[0][0] if got else None
        if value is None:
            return None
        text = str(value).strip()
        return int(text) if text.isdigit() else int(text, 16)

    @staticmethod
    def position_reached(have, want):
        def lsn(v):
            v = v.get("lsn") if isinstance(v, dict) else v
            return None if v is None else int(v)
        have, want = lsn(have), lsn(want)
        return None if have is None or want is None else have >= want

    def snapshot_state(self, db, state_dir, kind="all"):
        """The target's sequences and identity columns where they stand,
        read in one unit of work at repeatable read, which a rollback
        sets back (`restore_sequences`), and the log's position kept with
        them where the server says it (`decide.db2_snapshot_mark`); the
        rows a repair writes are kept by the repair's own record."""
        from .. import decide
        conn = self._connect("dst", db)
        try:
            cur, lines = conn.cursor(), []
            for sql, n in self._counter_sql(db):
                self._run(cur, sql)
                for r in cur.fetchall():
                    name = ".".join(str(v).strip() for v in r[:n])
                    lines.append(f"{name}|{int(r[n])}")
            try:
                pos, why = self.log_position("dst", db), None
            except Exception as e:  # noqa: BLE001 - said in the mark
                pos, why = None, str(e).splitlines()[0][:120] or "refused"
            if pos is None and why is None:
                why = "the server keeps no position of its log a query reads"
            got = decide.db2_snapshot_mark({"log": why})
            conn.commit()
        finally:
            conn.close()
        (state_dir / "dst-db2-counters.txt").write_text(
            "\n".join(sorted(lines)) + ("\n" if lines else ""))
        (state_dir / "dst-db2-mark.txt").write_text(
            f"{got.path}|{pos if got.path == 'log mark' else ''}|"
            f"{got.reason}\n")

    def restore_sequences(self, db, state_dir):
        path = state_dir / "dst-db2-counters.txt"
        if not path.exists():
            return []
        return [self._restart_sql(db, name, int(value)) + ";"
                for name, _, value in (line.rpartition("|") for line in
                                       path.read_text().splitlines() if line)]

    # the server's own load (`decide.db2_bulk_way`), else the copier
    #: (table, rows the catalogue counts, average row bytes) of a schema
    BULK_FACTS = ("select tabname, card, avgrowsize from syscat.tables"
                  f" where tabschema = {MARK} and type = 'T'")

    def _bulk_facts(self, db):
        """{table: {"rows", "width"}} of the source's schema, as the
        catalogue counts them (None where statistics were never
        gathered)."""
        return {self._said(str(t).strip()): {
            "rows": None if c is None or int(c) < 0 else int(c),
            "width": w} for t, c, w in self._rows(
                "src", db, self.BULK_FACTS, (self._owner("src", db),))}

    def _same_place(self):
        """Both sides are one server's (one database, subsystem or
        system): its own load reads another's only with that one's
        password written into the command, which its statement history
        keeps."""
        s, t = self.hop.source, self.hop.target
        return ((s.host, s.port, s.options.get("database"))
                == (t.host, t.port, t.options.get("database")))

    def _archived(self):
        """Whether the target archives its log (`logarchmeth1`), None where
        it was not read."""
        got = self._db2_settings("dst", (self.hop.databases or [""])[0])
        if self.UNREADABLE in got or "logarchmeth1" not in got:
            return None
        return str(got["logarchmeth1"]).upper() != "OFF"

    def _bulk_extra(self, db, table):
        """What a load of `table` is told beyond its columns: a value the
        target generates always kept as the source's."""
        always = self._rows("dst", db, "select 1 from syscat.columns where"
                            f" tabschema = {MARK} and tabname = {MARK} and"
                            " generated = 'A' fetch first 1 rows only",
                            (self._owner("dst", db), self._q(table)[1:-1]))
        return {"modified": ["identityoverride", "generatedoverride"]
                if always else [], "always": False}

    def _load(self, db, table, path, extra, log):
        """One table loaded by the server from a cursor on the source,
        each count the load answers checked: a row it read and did not
        load stops the move for the table, never taken as loaded; a table
        it leaves pending its constraints is checked at once."""
        cols = ", ".join(self._q(n) for n, _ in self.neutral_columns(
            "src", db, table))
        mods = " ".join(extra.get("modified") or ())
        cmd = (f"load from (select {cols} from"
               f" {self._qualified('src', db, table)}) of cursor"
               + (f" modified by {mods}" if mods else "")
               + f" messages on server insert into"
               f" {self._qualified('dst', db, table)} ({cols})")
        conn = self._connect("dst", db)
        try:
            cur = conn.cursor()
            self._run(cur, f"call sysproc.admin_cmd({MARK})", (cmd,))
            names = [d[0].lower() for d in cur.description or ()]
            row = dict(zip(names, cur.fetchall()[0])) if names else {}
            read, loaded = row.get("rows_read"), row.get("rows_loaded")
            lost = sum(int(row.get(k) or 0) for k in ("rows_skipped",
                                                      "rows_rejected"))
            if read is None or loaded is None or lost or int(read) != \
                    int(loaded):
                raise SystemExit(
                    f"{table}: the server's load read {read} rows and"
                    f" loaded {loaded} ({lost} skipped or rejected). The"
                    " table is not taken as moved: its messages are on the"
                    " target; move it with the table copier")
            self._run(cur, "select status from syscat.tables where"
                           f" tabschema = {MARK} and tabname = {MARK}",
                      (self._owner("dst", db), self._q(table)[1:-1]))
            if str(cur.fetchall()[0][0]).strip() == "C":
                self._run(cur, f"set integrity for"
                               f" {self._qualified('dst', db, table)}"
                               " immediate checked")
            conn.commit()
        finally:
            conn.close()
        if log:
            log(f"{table}: {loaded} rows loaded by the server")

    def _empty(self, db, table):
        try:
            return not self._rows("dst", db, "select 1 from"
                                  f" {self._qualified('dst', db, table)}"
                                  " fetch first 1 rows only")
        except Exception:  # noqa: BLE001 - no table yet: the copier's
            return False

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each table carried by the server's own load where the decision
        finds it pays and is safe (`decide.db2_bulk_way`, from the
        catalogue's size of it, the target's log and table), else by the
        table copier, which empties the table before it writes: one
        server's tables, an empty target, columns as they are."""
        from .. import decide
        said = log or (lambda m: None)
        facts = self._bulk_facts(db)
        place, archived, steps, pair = self._same_place(), None, [], []
        if self.PLATFORM == "luw":
            archived = self._archived()
        for t in list(tables):
            r, extra = facts.get(t) or {}, self._bulk_extra(db, t)
            got = decide.db2_bulk_way({
                "platform": self.PLATFORM, "rows": r.get("rows"),
                "width": r.get("width"), "empty": self._empty(db, t),
                "mapped": bool(self.through_pair(db, t)),
                "archived": archived, "same_place": place,
                "alone": r.get("alone"), "always": extra.get("always"),
                "copier_rate": None, "load_start": None})
            steps.append(f"{t}: {got.rung.said} - {got.reason}")
            if not go:
                continue
            if got.path == "copier":
                said(f"{t}: copied table by table - {got.reason}")
                if not pair:
                    pair.append(self._as_pair())
                from .mssql import _Unkept
                sch, _, tbl = str(t).rpartition(".")
                pair[0].move_table(db, sch, tbl, pair[0].READ_ROWS,
                                   _Unkept(), said)
            else:
                self._load(db, t, got.path, dict(r, **extra), log)
        return steps

    # the rows changed since a clean pass (`decide.db2_delta_way`)
    #: (table, column) of each row change timestamp of a schema
    STAMPS = ("select tabname, colname from syscat.columns where tabschema"
              f" = {MARK} and rowchangetimestamp = 'Y'")
    NOW = "select current timestamp from sysibm.sysdummy1"

    def change_log(self):
        """The reader of the source's change log, where one is installed
        (`engines.db2_log`, which fills `neutral_changes`, `copy_point`
        and `CHANGE_POINT_READS_ONLY` for the pair's delta and tail): the
        one hook it is reached by."""
        from . import db2_log
        return db2_log

    def _log_readable(self):
        """Whether the source's log can be read from this machine: the
        reader is installed and so is the client it loads."""
        reader = self.change_log()
        return bool(reader and getattr(reader, "neutral_changes", None)
                    and reader.library() is not None)

    def change_point(self, side, db):
        """Where the source's log is now, as the position a tail goes on
        from ({lsn, from}). The reader's refusal is said as it is - the
        client not installed, the log circular, no table in scope - so a
        tail is never started on a log nothing reads; the delta check
        takes its own rung meanwhile (`decide.db2_delta_way`)."""
        reader = self.change_log()
        reader.way(self, side, db)
        return reader.change_point(self, side, db)

    def tail_apply(self, db, go, token_path, log):
        """The change tail is the pair's, reading the source's log
        (`neutral_changes`) and keeping each table in its own state."""
        return self._as_pair().tail_apply(db, go, token_path, log)

    def neutral_changes(self, side, db, token=None, limit=None):
        """The row changes of whole transactions committed after `token`,
        read from the source's log (`engines.db2_log`); a position is
        handed back that a stopped tail resumes from, leaving what was
        already handed on out."""
        return self.change_log().neutral_changes(self, side, db, token,
                                                 limit)

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        """The pair's fence: the tail's saved position read until it has
        reached `lsn` (`position_reached`); nothing to wait on with no
        tail running."""
        return self._as_pair().fence_wait(db, lsn, timeout)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def delta_verify(self, db, limit=20000, log=None):
        """Only the rows changed since the last clean pass, by the way the
        decision finds (`decide.db2_delta_way`): the keys the change log
        names, or the rows whose row change timestamp is past the pass,
        each asked of both sides by key. A table's mark moves only when
        it is clean; deletes are the full check's."""
        from .. import decide, durable
        from . import same_engine
        tables = self.neutral_tables("src", db)
        stamps = {self._said(str(t).strip()): str(c).strip() for t, c in
                  self._rows("src", db, self.STAMPS,
                             (self._owner("src", db),))}
        stamps = {t: c for t, c in stamps.items() if t in tables}
        got = decide.db2_delta_way({
            "log": self._log_readable(),
            "stamped": len(stamps), "tables": len(tables),
            "same": same_engine(self)})
        if got.path == "log":
            return self._as_pair().delta_verify(db, limit, log)
        if got.path == "none":
            return [Result("delta", db, "skip",
                           "not verified by change: " + got.reason)]
        state = self.hop.report_dir(db) / "db2-delta.json"
        held = durable.read_state(state, "position")
        now, res = dict(held or {}), []
        for t in [t for t in tables if t not in stamps]:
            res.append(Result("delta", f"{db}.{t}", "skip",
                              "not verified by change: no row change"
                              " timestamp"))
        for t, col in sorted(stamps.items()):
            mark = str(self._rows("src", db, self.NOW)[0][0])
            if held is None or t not in held:
                now[t] = mark
                res.append(Result("delta", f"{db}.{t}", "ok",
                                  "baseline recorded, changes are tracked"
                                  " from this point on"))
                continue
            res.append(self._stamped_pass(db, t, col, held[t], limit, now,
                                          mark))
        durable.write_json(state, now)
        return res

    def _stamped_pass(self, db, table, col, since, limit, now, mark):
        """One table's rows stamped past `since`, compared by key."""
        key = self.neutral_key("src", db, table)
        if not key:
            return Result("delta", f"{db}.{table}", "skip",
                          "not verified by change: the table has no key")
        cols = [n for n, _ in self.neutral_columns("src", db, table)]
        sel = ", ".join(self._q(c) for c in cols)
        src = self._rows("src", db, f"select {sel} from"
                         f" {self._qualified('src', db, table)} where"
                         f" {self._q(col)} > {MARK} order by"
                         f" {', '.join(self._q(k) for k in key)} fetch first"
                         f" {int(limit) + 1} rows only", (since,))
        if len(src) > limit:
            return Result("delta", f"{db}.{table}", "warn",
                          f"more than {limit} rows changed since the last"
                          " clean pass: run check, which compares them all")
        at = [cols.index(k) for k in key]
        where = " and ".join(f"{self._q(k)} = {MARK}" for k in key)
        missing, changed = [], []
        for row in src:
            ks = [row[i] for i in at]
            got = self._rows("dst", db, f"select {sel} from"
                             f" {self._qualified('dst', db, table)} where"
                             f" {where}", ks)
            if not got:
                missing.append(ks)
            elif list(got[0]) != list(row):
                changed.append(ks)
        if missing or changed:
            return Result("delta", f"{db}.{table}", "diff",
                          f"{len(src)} changed rows: {len(missing)} missing"
                          f" on the target, {len(changed)} different", "",
                          "run check on the table, and repair")
        now[table] = mark
        return Result("delta", f"{db}.{table}", "ok",
                      f"{len(src)} rows changed since the last clean pass,"
                      " each the same on the target")
