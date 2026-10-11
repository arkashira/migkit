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
from .base import Engine, Result
from .dbapi import MARK, DbapiRows, FoldsToCapitals
from .sqlcatalog import CatalogChecks

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
    CATALOGUE = CATALOGUE
    #: the types whose text the server writes in a way of its own
    RENDERED = ("xml", "decfloat")
    CANON_ENGINE = "db2"
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
        return self._as_pair().check_schema(db)

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
