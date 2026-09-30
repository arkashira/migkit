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
from .base import Engine, Result
from .dbapi import MARK, DbapiRows, FoldsToCapitals

#: the declared type, with the numbers a counterpart needs
DECLARED = ("case when typename in ('DECIMAL', 'NUMERIC') then typename"
            " || '(' || length || ',' || scale || ')'"
            " when typename in ('CHARACTER', 'VARCHAR', 'GRAPHIC',"
            " 'VARGRAPHIC') and codepage = 0 then 'VARBINARY(' || length"
            " || ')'"
            " when typename in ('CHARACTER', 'VARCHAR', 'GRAPHIC',"
            " 'VARGRAPHIC') then typename || '(' || length || ')'"
            " when typename = 'TIMESTAMP' then 'TIMESTAMP(' || scale || ')'"
            " else typename end")


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


class Db2Engine(FoldsToCapitals, DbapiRows, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "db2"
    SQL_DIALECT = None
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " a Db2 one nobody has run")
    OVER_NETWORK = True
    PARAMSTYLE = "qmark"
    LIMIT = "fetch"
    PADDED = ("CHARACTER", "CHAR", "GRAPHIC")
    SCALED = ("DECIMAL", "NUMERIC")

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
        from .. import canon
        name, nums = canon._split_declared(declared)
        if name == "timestamp" and nums and nums[0] > 9:
            return (self._nanos_text(quoted, declared), _twelve)
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
            if not self.hop.excluded(db, t)]

    def neutral_columns(self, side, db, table):
        return [(self._said(n.strip()), t.strip()) for n, t in self._rows(
            side, db, f"select colname, {DECLARED} from syscat.columns"
                      f" where tabschema = {MARK} and tabname = {MARK}"
                      " order by colno",
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
        if held:
            return [Result("deep", f"{db} target tables", "diff",
                           f"{len(held)} tables not in normal state:"
                           f" {', '.join(r[0].strip() for r in held[:6])}"
                           " - left pending by a load", "",
                           "set integrity for those tables immediate"
                           " checked")]
        return [Result("deep", f"{db} target tables", "ok",
                       "every table in normal state")]
