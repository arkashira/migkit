"""Alibaba Lindorm's wide tables as a side of a pair (backlog P0-CLOUDS),
through LindormTable's SQL on MySQL's wire (its MySQL-protocol endpoint,
33060).

A hop's database is a Lindorm namespace, a table one of its wide tables:
its columns and primary key from `DESCRIBE`, read in key order through
the one DB-API reader, written by `UPSERT` - Lindorm's own statement,
which replaces a row with the same key, so a batch written again lands on
itself. Its SQL has no hash that is proved to render a row as migkit
does, so every table is folded here.

Its changes leave only through Lindorm's own data subscription, in a form
not read here: a Lindorm source is moved while nothing writes and proved
by check. The time-series, search and file engines Lindorm also has are
not this side. None of it has been run against a Lindorm instance: what
it does is LindormTable's documented SQL, over a wire that has been.
"""
from .base import Engine
from .dbapi import MARK, DbapiRows


class LindormEngine(DbapiRows, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "lindorm"
    FOLDED_BECAUSE = "has no hash of its own proved to render a row as migkit"
    OVER_NETWORK = True
    PARAMSTYLE = "pyformat"
    LIMIT = "limit"

    def _connect(self, side, db):
        import pymysql
        ep = self.hop.source if side == "src" else self.hop.target
        return pymysql.connect(host=ep.host, port=int(ep.port or 33060),
                               user=ep.user, password=ep.password,
                               database=self._d(side, db) or None,
                               charset="utf8mb4", connect_timeout=15,
                               **ep.mysql_tls())

    def _q(self, name):
        return "`" + str(name).replace("`", "``") + "`"

    def _qualified(self, side, db, table):
        return f"{self._q(self._d(side, db))}.{self._q(table)}"

    def _create_name(self, side, db, table):
        return self._qualified(side, db, table)

    def databases(self):
        return list(self.hop.databases)

    def _described(self, side, db, table):
        """[{column, type, key}] as `DESCRIBE` answers, by its own column
        names."""
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute(f"describe {self._qualified(side, db, table)}")
            names = [str(d[0]).lower() for d in cur.description]
            rows = [dict(zip(names, r)) for r in cur.fetchall()]
        finally:
            conn.close()
        out = []
        for r in rows:
            name = r.get("column_name") or r.get("field")
            typ = r.get("type") or r.get("type_name") or r.get("data_type")
            key = r.get("is_primary_key", r.get("key"))
            out.append({"column": str(name), "type": str(typ or ""),
                        "key": str(key).lower() in ("true", "1", "pri",
                                                    "yes")})
        return out

    def neutral_tables(self, side, db):
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute("show tables")
            got = sorted(str(r[0]) for r in cur.fetchall())
        finally:
            conn.close()
        return [t for t in got if not self.hop.excluded(db, t)]

    def neutral_columns(self, side, db, table):
        return [(c["column"], c["type"].lower())
                for c in self._described(side, db, table)]

    def neutral_key(self, side, db, table):
        return [c["column"] for c in self._described(side, db, table)
                if c["key"]]

    def table_facts(self, side, db):
        return {t: {"rows": None, "bytes": None, "key": True}
                for t in self.neutral_tables(side, db)}

    def neutral_write(self, side, db, table, columns, rows):
        """Rows by `UPSERT`, which replaces a row with the same key."""
        from .. import canon
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        sql = self._sql(f"upsert into {self._qualified(side, db, table)}"
                        f" ({', '.join(self._q(n) for n in names)}) values"
                        f" ({', '.join([MARK] * len(names))})", True)
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.executemany(sql, [tuple(
                canon.write_value("mysql", c, v)
                for (_, c), v in zip(columns, r)) for r in rows])
            conn.commit()
        finally:
            conn.close()
        return len(rows)

    def _upsert_one(self, cur, side, db, table, key, values):
        from .. import canon
        row = {**values, **key}
        self._run(cur, f"upsert into {self._qualified(side, db, table)}"
                       f" ({', '.join(self._q(n) for n in row)}) values"
                       f" ({', '.join([MARK] * len(row))})",
                  [canon.sql_value(v) for v in row.values()])

    def neutral_create_sql(self, side, db, table, columns, key=()):
        if not key:
            raise SystemExit(f"{table} has no key, and a Lindorm wide table"
                             " keeps every row by its primary key: give the"
                             " table one, or leave it out of this hop")
        return super().neutral_create_sql(side, db, table, columns, key)

    def server_identity(self, side, db):
        conn = self._connect(side, db)
        try:
            version = conn.get_server_info()
        finally:
            conn.close()
        ep = self.hop.source if side == "src" else self.hop.target
        return {"version": str(version), "id": f"{ep.host}:{ep.port}"}

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None):
        return self._as_pair().check_data(db, table, stream)

    def repair_plan(self, db, kind):
        return self._as_pair().repair_plan(db, kind)

    def apply(self, db, action):
        return self._as_pair().apply(db, action)

    def reach_items(self, side):
        items = [{"level": "warn", "scope": "reach",
                  "item": "copied and verified, not proved here",
                  "detail": "LindormTable's SQL over MySQL's wire, rows"
                            " folded here"}]
        if side == "src":
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes are not followed",
                          "detail": "Lindorm's changes leave only through"
                                    " its own data subscription: move it"
                                    " while nothing writes"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")
