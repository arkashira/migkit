"""SAP ASE (Sybase) as one side of a pair (backlog 34).

Through FreeTDS's ODBC driver at protocol 5.0, which ASE speaks: the TDS
driver that installs with pip stops at SQL Server's 7.x (measured,
`unrecognized tds version: 5.0`). The endpoint's `odbc_driver` names the
driver's library where it is not in the usual places. A hop's database is
an ASE database, and its tables are named by their owner (`dbo.orders`).
Rows are compared through the in-process renderer every engine without a
canonical SQL rendering shares.

Written 2026-09-25 without a server to run it against: SAP's ASE image
runs on x86 only, and this machine is arm64.
"""
from .base import Engine
from .dbapi import MARK, DbapiRows

#: where FreeTDS's ODBC driver is installed by Homebrew and by apt
DRIVERS = ("/opt/homebrew/lib/libtdsodbc.so", "/usr/local/lib/libtdsodbc.so",
           "/usr/lib/x86_64-linux-gnu/odbc/libtdsodbc.so",
           "/usr/lib/aarch64-linux-gnu/odbc/libtdsodbc.so")

#: the declared type, with the numbers a counterpart needs
DECLARED = ("t.name + case when t.name in ('numeric', 'decimal') then '('"
            " + convert(varchar(4), c.prec) + ',' + convert(varchar(4),"
            " c.scale) + ')' when t.name in ('char', 'varchar', 'binary',"
            " 'varbinary', 'nchar', 'nvarchar', 'unichar', 'univarchar')"
            " then '(' + convert(varchar(6), c.length) + ')' else '' end")


class AseEngine(DbapiRows, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "ase"
    SQL_DIALECT = None
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " an ASE one nobody has run")
    OVER_NETWORK = True
    PARAMSTYLE = "qmark"
    LIMIT = "top"

    def _driver(self, side):
        import os
        ep = self.hop.source if side == "src" else self.hop.target
        named = (ep.options or {}).get("odbc_driver")
        if named:
            return named
        for path in DRIVERS:
            if os.path.exists(path):
                return path
        raise SystemExit("FreeTDS's ODBC driver is not on this machine, and"
                         " SAP ASE is reached through it: brew install"
                         " freetds, or apt-get install tdsodbc - or say"
                         " where it is with odbc_driver in the endpoint's"
                         " options")

    def _connection_string(self, side, db):
        ep = self.hop.source if side == "src" else self.hop.target
        # a value in braces is taken whole; a brace in it is doubled
        secret = "{" + (ep.password or "").replace("}", "}}") + "}"
        return (f"DRIVER={{{self._driver(side)}}};SERVER={ep.host};"
                f"PORT={ep.port or 5000};TDS_Version=5.0;"
                f"UID={ep.user};PWD={secret};"
                f"DATABASE={self._d(side, db)};ClientCharset=UTF-8")

    def _connect(self, side, db):
        try:
            import pyodbc
        except ImportError as e:
            # measured: the module installs, and then cannot load without
            # the ODBC manager (`libodbc.2.dylib ... no such file`)
            raise SystemExit("ODBC is not usable on this machine"
                             f" ({str(e).splitlines()[0][:120]}): brew"
                             " install unixodbc freetds, or apt-get install"
                             " unixodbc tdsodbc")
        conn = pyodbc.connect(self._connection_string(side, db),
                              timeout=15)
        # names in double quotes are names, not strings
        conn.cursor().execute("set quoted_identifier on")
        return conn

    def databases(self):
        return list(self.hop.databases)

    def neutral_tables(self, side, db):
        return [t for (t,) in self._rows(
            side, db, "select user_name(uid) + '.' + name from sysobjects"
                      " where type = 'U' order by 1")
            if not self.hop.excluded(db, *t.split(".", 1))]

    def neutral_columns(self, side, db, table):
        return [(n, t) for n, t in self._rows(
            side, db, f"select c.name, {DECLARED} from syscolumns c join"
                      " systypes t on t.usertype = c.usertype where c.id ="
                      f" object_id({MARK}) order by c.colid",
            (table,))]

    def neutral_key(self, side, db, table):
        """The columns of the index ASE marks as the primary key's."""
        return [n for (n,) in self._rows(
            side, db, "select index_col(object_name(i.id), i.indid,"
                      " n.number) from sysindexes i, master..spt_values n"
                      f" where i.id = object_id({MARK}) and i.status & 2048"
                      " = 2048 and n.type = 'P' and n.number between 1 and"
                      " i.keycnt and index_col(object_name(i.id), i.indid,"
                      " n.number) is not null order by n.number",
            (table,))]

    def _before_insert(self, cur, side, db, table):
        """Explicit values go into an identity column only with identity
        insert on for the table."""
        self._run(cur, "select count(*) from syscolumns where id ="
                       f" object_id({MARK}) and status & 128 = 128", (table,))
        if not cur.fetchone()[0]:
            return None
        quoted = self._qualified(side, db, table)
        self._run(cur, f"set identity_insert {quoted} on")
        return f"set identity_insert {quoted} off"

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)
