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

    def _connect(self, side, db):
        import ibm_db_dbi
        ep = self.hop.source if side == "src" else self.hop.target
        name = ep.options.get("database")
        if not name:
            raise SystemExit(f"the {'source' if side == 'src' else 'target'}"
                             " needs `database`, the Db2 database to connect"
                             " to; the hop's databases are its schemas")
        return ibm_db_dbi.connect(
            f"DATABASE={name};HOSTNAME={ep.host};PORT={ep.port or 50000};"
            f"PROTOCOL=TCPIP;UID={ep.user};PWD={ep.password};"
            "CONNECTTIMEOUT=15;", "", "")

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
