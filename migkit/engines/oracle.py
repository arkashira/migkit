"""Oracle as one side of a pair (backlog 11), through python-oracledb in
thin mode: no Instant Client on the machine.

A database is a schema - its owner. Rows are compared through the
in-process renderer every engine without a canonical SQL rendering
shares. Oracle folds an unquoted name to capitals, so a name in capitals
is said in small letters and a name in small letters is written in
capitals, which is how `orders` on one side meets `ORDERS` on the other.
`CHAR` values come back padded, and are compared without the padding, as
every other engine's are. A `NUMBER` keeps no trailing zeros - `12.50` is
`12.5` - so a number is read back at the scale its column declares, and
as the text a decimal of that scale renders on every other engine. An empty string is a null in Oracle: a text
column that held `''` on the other side is a difference, because it is
one.

Written 2026-09-25 without a server to run it against: Oracle Free needs
more memory and disk than this machine's container VM has (measured: it
filled the disk and took 2.5 GB of 3.8). The rendering and the comparison
it feeds are the ones every other engine is held to.
"""
import re

from .base import Engine, Result
from .dbapi import MARK, DbapiRows, FoldsToCapitals

#: the declared type, with the numbers a counterpart needs
DECLARED = ("case when data_type like '%WITH%TIME ZONE' then"
            " 'timestamp with time zone'"
            " when data_type = 'NUMBER' and data_precision is not null then"
            " 'NUMBER(' || data_precision || ',' || nvl(data_scale, 0) || ')'"
            " when data_type in ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR')"
            " then data_type || '(' || char_length || ')'"
            " when data_type = 'RAW' then 'RAW(' || data_length || ')'"
            " else data_type end")


class OracleEngine(FoldsToCapitals, DbapiRows, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "oracle"
    SQL_DIALECT = "oracle"
    FOLDED_BECAUSE = ("is rendered by migkit's own renderer rather than by"
                      " an Oracle one nobody has run")
    OVER_NETWORK = True
    PARAMSTYLE = "numeric"
    LIMIT = "fetch"
    PADDED = ("CHAR", "NCHAR")
    SCALED = ("NUMBER",)

    def _connect(self, side, db):
        import oracledb
        # a LOB as its value, not as a handle to read it through; a number
        # as a decimal, not as a float that has already rounded it
        oracledb.defaults.fetch_lobs = False
        oracledb.defaults.fetch_decimals = True
        ep = self.hop.source if side == "src" else self.hop.target
        service = (ep.options.get("service") or ep.options.get("service_name")
                   or "FREEPDB1")
        return oracledb.connect(user=ep.user, password=ep.password,
                                dsn=f"{ep.host}:{ep.port or 1521}/{service}",
                                tcp_connect_timeout=15)

    def databases(self):
        return list(self.hop.databases)

    # ---- what a table is -------------------------------------------------

    def neutral_tables(self, side, db):
        return [t for t in (self._said(n) for (n,) in self._rows(
            side, db, "select table_name from all_tables where owner ="
                      f" {MARK} and nested = 'NO' and secondary = 'N'"
                      " order by 1", (self._owner(side, db),)))
            if not self.hop.excluded(db, t)]

    def neutral_columns(self, side, db, table):
        return [(self._said(n), t) for n, t in self._rows(
            side, db, f"select column_name, {DECLARED} from all_tab_columns"
                      f" where owner = {MARK} and table_name = {MARK}"
                      " order by column_id",
            (self._owner(side, db), self._q(table)[1:-1]))]

    def neutral_key(self, side, db, table):
        return [self._said(n) for (n,) in self._rows(
            side, db, "select cc.column_name from all_constraints c join"
                      " all_cons_columns cc on cc.owner = c.owner and"
                      " cc.constraint_name = c.constraint_name where"
                      f" c.owner = {MARK} and c.table_name = {MARK} and"
                      " c.constraint_type = 'P' order by cc.position",
            (self._owner(side, db), self._q(table)[1:-1]))]

    def table_facts(self, side, db):
        out = {}
        for name, rows, size in self._rows(
                side, db, "select t.table_name, t.num_rows, s.bytes from"
                          " all_tables t left join user_segments s on"
                          " s.segment_name = t.table_name and"
                          " s.segment_type = 'TABLE' where t.owner ="
                          f" {MARK}", (self._owner(side, db),)):
            out[self._said(name)] = {"rows": rows, "bytes": size,
                                     "key": None}
        return out

    def settle_target(self, db, from_source=True):
        """Statistics for the schema the load wrote."""
        self._target_only("dst", "refresh statistics")
        conn = self._connect("dst", db)
        try:
            self._run(conn.cursor(), "begin dbms_stats.gather_schema_stats("
                                     f"{MARK}); end;",
                      (self._owner("dst", db),))
        finally:
            conn.close()
        return "refreshed the target's statistics"

    # ---- code -----------------------------------------------------------

    def neutral_views(self, side, db):
        return [(self._said(n), text) for n, text in self._rows(
            side, db, f"select view_name, text from all_views where owner ="
                      f" {MARK} order by 1", (self._owner(side, db),))
            if not self.hop.excluded(db, self._said(n))]

    def neutral_functions(self, side, db):
        """Functions whose body is `BEGIN RETURN <expression>; END`; every
        other function and procedure is listed with no expression, to be
        named."""
        import sqlglot
        owner = self._owner(side, db)
        src = {}
        for name, kind, text in self._rows(
                side, db, "select name, type, text from all_source where"
                          f" owner = {MARK} and type in ('FUNCTION',"
                          " 'PROCEDURE') order by name, line", (owner,)):
            src.setdefault((name, kind), []).append(text or "")
        out = []
        for (name, kind), lines in sorted(src.items()):
            params, returns = [], None
            for arg, declared, pos in self._rows(
                    side, db, "select argument_name, data_type, position"
                              " from all_arguments where owner ="
                              f" {MARK} and object_name = {MARK} and"
                              " package_name is null order by position",
                    (owner, name)):
                if pos == 0:
                    returns = declared
                else:
                    params.append((self._said(arg or ""), declared))
            body = " ".join("".join(lines).split())
            m = re.search(r"\bbegin\s+return\s+(.*?);\s*end\b[^;]*;?\s*$",
                          body, re.I | re.S)
            one = None
            if kind == "FUNCTION" and m and ";" not in m.group(1):
                try:
                    one = sqlglot.parse_one(m.group(1), read="oracle").sql(
                        "oracle")
                except Exception:  # noqa: BLE001 - named, not carried
                    one = None
            out.append((self._said(name), params, returns, one))
        return out

    def neutral_function_sql(self, name, params, returns, body):
        # quoted as the body quotes them; Oracle takes no length or scale
        # on an argument or a result
        args = ", ".join(f'"{p}" {t.split("(")[0]}' for p, t in params)
        return (f"create function {self._q(name)}({args}) return"
                f" {returns.split('(')[0]} is begin return {body}; end;")

    # ---- the checks, through the pair every engine compares by --------

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def check_deep(self, db):
        """Objects a load left invalid, and constraints it left disabled or
        not validated, on the target: Oracle keeps using a table under
        them without a word."""
        out = []
        owner = self._owner("dst", db)
        try:
            invalid = self._rows("dst", db, "select object_type || ' ' ||"
                                            " object_name from all_objects"
                                            f" where owner = {MARK} and"
                                            " status = 'INVALID' order by 1",
                                 (owner,))
            loose = self._rows("dst", db, "select constraint_name || ' on '"
                                          " || table_name || ' (' || status"
                                          " || ', ' || validated || ')' from"
                                          " all_constraints where owner ="
                                          f" {MARK} and (status ="
                                          " 'DISABLED' or validated ="
                                          " 'NOT VALIDATED') order by 1",
                               (owner,))
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} target objects", "error",
                           "could not read the target's objects:"
                           f" {str(e).splitlines()[0][:90]}")]
        out.append(Result("deep", f"{db} target objects",
                          "diff" if invalid else "ok",
                          f"{len(invalid)} invalid:"
                          f" {', '.join(r[0] for r in invalid[:6])}"
                          if invalid else "no invalid object on the target",
                          "", "compile them again, and check what each"
                              " depends on" if invalid else ""))
        out.append(Result("deep", f"{db} target constraints",
                          "diff" if loose else "ok",
                          f"{len(loose)} not enforced:"
                          f" {', '.join(r[0] for r in loose[:6])}"
                          if loose else "every constraint enabled and"
                                        " validated", "",
                          "alter table ... enable validate constraint ..."
                          if loose else ""))
        return out
