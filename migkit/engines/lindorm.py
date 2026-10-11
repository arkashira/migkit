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
from .base import Engine, Result
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
        return [t for t in got if self.in_scope(db, t)]

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

    # ---- a table's attributes, its indexes ---------------------------------

    def _answered(self, side, db, sql):
        """Rows of a statement, each by the names its answer gives, in small
        letters."""
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            cur.execute(sql)
            names = [str(d[0]).lower() for d in cur.description or ()]
            return [dict(zip(names, r)) for r in cur.fetchall()]
        finally:
            conn.close()

    #: what deletes a row, or changes what a write does
    CRITICAL_PARAMS = ("TTL", "MUTABILITY", "CONSISTENCY", "DYNAMIC_COLUMNS",
                       "CHS", "CHS_L2", "CHS_COLUMN")

    def _settings(self, side, db):
        """{table attribute: value} of every table in scope, as `SHOW TABLE
        VARIABLES` says them - its CREATE statement leaves them out."""
        out = {}
        try:
            for t in self.neutral_tables(side, db):
                for r in self._answered(
                        side, db, f"show table variables from"
                                  f" {self._qualified(side, db, t)}"):
                    name = r.get("variable_name") or r.get("name") or next(
                        iter(r.values()), "")
                    value = r.get("value") or r.get("variable_value") or ""
                    out[f"{t} {name}"] = str(value)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return {self.UNREADABLE: str(e).strip().splitlines()[0][:120]}
        return out

    def check_params(self, db):
        """Each table's attributes both sides: its time to live, which
        deletes what the other side keeps, its mutability and consistency,
        whether it takes columns it was not made with, and its cold
        storage."""
        src, dst = self._settings("src", db), self._settings("dst", db)
        critical = tuple(k for k in set(src) | set(dst)
                         if k.rsplit(" ", 1)[-1].upper()
                         in self.CRITICAL_PARAMS)
        return self._param_result(db, src, dst, critical,
                                  "give the target's tables the source's"
                                  " attributes (ALTER TABLE ... SET ...)")

    def _indexes(self, side, db):
        """{(table, index, its type): (state, progress)} as `SHOW INDEX`
        says them."""
        out = {}
        for t in self.neutral_tables(side, db):
            for r in self._answered(
                    side, db, f"show index from"
                              f" {self._qualified(side, db, t)}"):
                name = r.get("index_name") or r.get("key_name")
                if not name or str(name).upper() == "PRIMARY":
                    continue
                out[(t, str(name), str(r.get("index_type") or ""))] = (
                    str(r.get("index_state") or ""),
                    str(r.get("index_progress") or ""))
        return out

    def check_deep(self, db):
        """Every secondary, search and column-store index of the source's on
        the target, of the same kind, active and built whole: a query the
        source answers from one reads the whole table on a target
        without it, or one still building."""
        try:
            src, dst = self._indexes("src", db), self._indexes("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("deep", f"{db} indexes", "error",
                           "could not read the indexes:"
                           f" {str(e).strip().splitlines()[0][:90]}")]
        lacking = [k for k in src if k not in dst]
        idle = [(k, dst[k]) for k in src if k in dst
                and dst[k][0].upper() != "ACTIVE"]
        return [Result(
            "deep", f"{db} indexes", "diff" if lacking or idle else "ok",
            (f"{len(lacking)} of the source's indexes are not on the target:"
             f" {', '.join(f'{t}.{i}' for t, i, _ in lacking[:4])}"
             if lacking else "indexes not active on the target: " + ", ".join(
                 f"{t}.{i} {s} {p}" for (t, i, _), (s, p) in idle[:4])
             if idle else f"every one of the source's {len(src)} indexes is"
                          " on the target, active"), "",
            "create them on the target (CREATE INDEX) and build them"
            if lacking else "build them (BUILD INDEX), or enable them"
            if idle else "")]

    def snapshot_state(self, db, state_dir, kind="all"):
        """What the target held before a repair: each table's attributes and
        rows counted - the rows a repair writes are kept by the repair's
        own record."""
        import json
        out = {"settings": self._settings("dst", db), "rows": {}}
        for t in self.neutral_tables("dst", db):
            out["rows"][t] = self._rows(
                "dst", db, f"select count(*) from"
                           f" {self._qualified('dst', db, t)}")[0][0]
        (state_dir / "dst-lindorm.json").write_text(
            json.dumps(out, indent=2, sort_keys=True, default=str))

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
