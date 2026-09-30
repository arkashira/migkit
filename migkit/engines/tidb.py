"""TiDB as a side of a pair (backlog ER4), over MySQL's wire.

TiDB speaks MySQL's protocol and SQL, so the MySQL engine's connection,
catalogue, reads and writes are this engine's too, and its tables are
digested on the server by MySQL's rendering (`canon.BUILDERS`). What
differs is underneath:

* no binlog, so nothing to replicate from and no binlog position. Its
  changes come from TiCDC, published to a Kafka topic as canal-json with
  `enable-tidb-extension=true` (or TiCDC's simple protocol), read by the
  change-topic reader (`changetopic`): in commit order by each event's
  commit TSO, released at each watermark, each changed row read back from
  TiDB as it stood at the watermark (`@@tidb_snapshot`) - exact whatever
  canal-json made of the values;
* a copy's snapshot and a change's commit are on one clock (the TSO), so a
  change the copy's read already saw is known exactly (`mark_covers`);
* a same-engine move goes through the pair machinery - one keyed copier,
  one comparison, one tail for every engine - with TiDB's own `SHOW CREATE
  TABLE` making the tables (clustered keys, AUTO_RANDOM and all);
* `ADMIN CHECKSUM TABLE` is not the verdict: its CRC64 is over the table's
  KV pairs, whose keys carry the table's own id, and two clusters give the
  same table different ids - equal tables would read as different;
* no triggers, events or stored procedures to carry, and an
  AUTO_INCREMENT that each TiDB server hands out in its own ranges: the
  target's next id is read with `SHOW TABLE ... NEXT_ROW_ID`.

The hop's source says where TiCDC publishes:

    source:
      host: tidb1
      port: 4000
      changes: {servers: kafka1:9092, topic: ticdc-shop, format: canal-json}
"""
from .base import Engine, NotHere, Result
from .mysql import MySQLEngine

#: TiDB's error for a snapshot the garbage collector has passed
GC_PASSED = (9006,)


class TiDBEngine(MySQLEngine):
    CANON_ENGINE = "tidb"
    checks = ("schema", "counts", "autoinc", "data")
    #: the pair's data check counts nothing of its own: counts are asked
    #: apart, as every pair's are
    counts_from_data = False
    #: TiCDC's watermark releases the rows before it; the next batch is
    #: read once this one is applied
    READS_AHEAD = False
    REPLICATES_THE_SERVER = False
    CHANGE_POINT_READS_ONLY = True
    CLIENT_TOOLS = ()

    # MySQL's ways that read a binlog or make a replica: TiDB has neither
    replicate_sql = NotHere()
    apply_replication_stmt = NotHere()
    replication_status = NotHere()
    setup_target_plan = NotHere()
    loops_prevented = NotHere()
    purge_backlog = NotHere()
    migration_pair = NotHere()
    rehearse = NotHere()
    restore_rows = NotHere()
    binlog_names = NotHere()
    origin_mark = NotHere()
    origin_seen = NotHere()
    twoway_uuid = NotHere()
    mark_facts = NotHere()
    mark_committed = NotHere()
    mark_prove = NotHere()
    mark_teardown = NotHere()
    # a snapshot of MySQL's target is its dump program's schema, which is
    # not TiDB's
    snapshot_state = NotHere()
    native_replica_unsafe = Engine.native_replica_unsafe
    stream_writers = Engine.stream_writers
    pause_writer = Engine.pause_writer
    resume_writer = Engine.resume_writer
    who_wrote = Engine.who_wrote
    assess = Engine.assess

    def load_window(self, db, log=None, tables=None):
        """Nothing to set aside: TiDB has no triggers."""
        return Engine.load_window(self, db, log, tables)

    # ---- the server ------------------------------------------------------

    def _tso(self, side="src"):
        """TiDB's clock now: the start TSO of a transaction begun here,
        which is later than every commit a read begun after it sees."""
        conn = self._conn(side)
        try:
            with conn.cursor() as cur:
                cur.execute("start transaction with consistent snapshot")
                cur.execute("select @@tidb_current_ts")
                got = int(cur.fetchone()[0])
        finally:
            conn.rollback()
            conn.close()
        return got

    def _cluster(self, side):
        """The cluster's id, where this TiDB keeps it in `mysql.tidb`."""
        try:
            got = self._q(side, "select variable_value from mysql.tidb where"
                                " variable_name = 'cluster_id'")
        except Exception:  # noqa: BLE001 - an older TiDB
            return None
        return str(got[0][0]) if got else None

    def server_identity(self, side, db):
        name = self._d(side, db)
        got = self._q(side, "select @@hostname, version(), (select"
                            " sum(data_length + index_length) from"
                            " information_schema.tables where table_schema"
                            " = %s), (select count(*) from"
                            " information_schema.schemata where schema_name"
                            " = %s)", (name, name))
        host, version, size, there = got[0]
        if "tidb" not in str(version).lower():
            raise SystemExit(
                f"the {'source' if side == 'src' else 'target'} is not TiDB"
                f" ({version}): name its engine mysql")
        out = {"name": str(host or ""), "version": str(version)}
        if name and not int(there):
            out["absent"] = True
        elif name:
            out["bytes"] = int(size or 0)
        cluster = self._cluster(side)
        if cluster:
            out["id"] = cluster
        return out

    def stream_identity(self, side, db):
        spec = self._changes_spec(side) or {}
        out = {"changes": ",".join(sorted(
            [str(t) for t in (spec.get("topics") or [spec.get("topic")])
             if t]))}
        cluster = self._cluster(side)
        if cluster:
            out["cluster"] = cluster
        return out

    # ---- where TiCDC publishes --------------------------------------------

    def _changes_spec(self, side="src"):
        ep = self.hop.source if side == "src" else self.hop.target
        spec = ep.options.get("changes")
        return dict(spec) if isinstance(spec, dict) else None

    def _follower(self, db):
        from .. import changetopic
        held = self.__dict__.setdefault("_followers", {})
        if db not in held:
            spec = self._changes_spec()
            if not spec:
                raise SystemExit(
                    "TiDB keeps no binlog: its changes are read from the"
                    " Kafka topic TiCDC publishes them to. Say where in the"
                    " source's options - changes: {servers: host:9092,"
                    " topic: <topic>, format: canal-json} - with a"
                    " changefeed made with enable-tidb-extension=true")
            env = changetopic.envelope(spec.get("format", "canal-json"))
            if env.name not in ("canal-json", "simple"):
                raise SystemExit(f"format: {env.name} - TiCDC's changes are"
                                 " read as canal-json or simple")
            # how each writes a binary key: canal-json a character a byte,
            # the simple protocol as JSON writes bytes (base64)
            self.__dict__["_binary"] = ("base64" if env.name == "simple"
                                        else "latin-1")
            held[db] = changetopic.Follower(
                env, changetopic.KafkaLanes(spec, f"{self.hop.name}-changes"),
                name=lambda row, db=db: self._named(db, row),
                read_back=lambda t, rows, v, db=db: self._read_back(
                    db, t, rows, v))
        return held[db]

    def _named(self, db, row):
        if str(row.db) != str(db):
            return None
        tables = self.__dict__.setdefault("_scope", {})
        if db not in tables or row.table not in tables[db]:
            tables[db] = set(self.neutral_tables("src", db))
        return row.table if row.table in tables[db] else None

    def _unkeyed(self, db):
        return [t for t in self.neutral_tables("src", db)
                if not self.neutral_key("src", db, t)]

    def change_point(self, side, db):
        """Now: the topic's ends, then TiDB's TSO - a change published
        before the ends committed before the TSO, and a copy begun after
        it reads every one of those."""
        gone = self._unkeyed(db)
        if gone:
            raise SystemExit(
                f"{db}: {', '.join(gone[:6])} {'has' if len(gone) == 1 else 'have'}"
                " no primary key, and a change TiCDC publishes for such a"
                " table names its hidden row id, which no other server"
                " holds: give each a primary key, or leave it out of the"
                " hop and move it with a copy")
        return self._follower(db).point(fence=lambda: self._tso(side))

    def copy_point(self, side, db):
        return self.change_point(side, db)

    def log_position(self, side, db):
        return {"fence": self._tso(side)}

    @staticmethod
    def position_reached(have, want):
        from ..changetopic import Follower
        return Follower.reached(have, want)

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each row TiCDC says changed, in commit order, as TiDB holds it
        at the watermark its batch ends on."""
        if token is None:
            return [], self.change_point(side, db)
        return self._follower(db).changes(token, limit)

    def release_changes(self):
        for f in self.__dict__.pop("_followers", {}).values():
            f.close()

    def position_lost(self, side, db, token):
        return self._follower(db).lost(token) if token else None

    def stream_room(self, side, db, token):
        return self._follower(db).room(token)

    def snapshot_mark(self, side, db):
        return str(self._tso(side))

    @staticmethod
    def mark_covers(side, db, mark, change):
        """A read begun after the mark sees every commit at or below it:
        the TSO is one clock for both."""
        txn = change.get("txn")
        if mark is None or not isinstance(txn, int):
            return None
        return txn <= int(mark)

    def _read_back(self, db, table, rows, version):
        """[(row, key, values or None)]: each key the feed named, read from
        TiDB as it stood at `version`, or as it is now where the garbage
        collector has passed that - and at `version`, held to what the
        feed said of it."""
        from .. import canon, changetopic
        declared = dict(self.neutral_columns("src", db, table))
        key = self.neutral_key("src", db, table)
        classes = {n: canon.type_class("tidb", t) for n, t in declared.items()}
        names = list(declared)
        cols = ", ".join(canon.read_expr("mysql", f"`{n}`",
                                         classes.get(n) or "text")
                         + f" as `{n}`" for n in names)

        def raw(row):
            vals = []
            for k in key:
                if k not in row.key:
                    raise SystemExit(
                        f"{table}: TiCDC names its changes by"
                        f" {', '.join(row.key) or 'no key'}, and its primary"
                        f" key is {', '.join(key)}: the changefeed's table"
                        " is not the table here")
                v = row.key[k]
                vals.append(changetopic.from_text(
                    "bytes", v, self.__dict__.get("_binary", "latin-1"))
                    if classes.get(k) == "bytes" and v is not None else v)
            return vals

        def text(values):
            return tuple(changetopic.key_text(classes.get(k), v)
                         for k, v in zip(key, values))
        conn = self._conn("src")
        at_fence = True
        found = {}
        try:
            with conn.cursor() as cur:
                try:
                    cur.execute("set @@tidb_snapshot = %s", (str(version),))
                except Exception as e:  # noqa: BLE001 - classified here
                    if getattr(e, "args", [None])[0] not in GC_PASSED:
                        raise
                    at_fence = False
                where = "(" + ", ".join(f"`{k}`" for k in key) + ") in ("
                for i in range(0, len(rows), 500):
                    part = rows[i:i + 500]
                    args = [v for r in part for v in raw(r)]
                    tail = (f" from `{db}`.`{table}` where " + where
                            + ", ".join("(" + ", ".join(["%s"] * len(key))
                                        + ")" for _ in part) + ")")
                    try:
                        cur.execute(f"select {cols}{tail}", args)
                    except Exception as e:  # noqa: BLE001 - classified
                        code = getattr(e, "args", [None])[0]
                        if code in GC_PASSED:
                            # the watermark is older than TiDB keeps: the
                            # rows as they are now, which later changes
                            # level again
                            at_fence = False
                            cur.execute("set @@tidb_snapshot = ''")
                            cur.execute(f"select {cols}{tail}", args)
                        elif code == 1054:
                            # a column added since the watermark: the row
                            # as it stood then has none of it
                            cur.execute(f"select *{tail}", args)
                        else:
                            raise
                    got_names = [d[0] for d in cur.description]
                    for got in cur.fetchall():
                        values = dict(zip(got_names, got))
                        found[text([values[k] for k in key])] = values
                cur.execute("set @@tidb_snapshot = ''")
        finally:
            conn.close()
        out = []
        for r in rows:
            typed = [changetopic.from_text(classes.get(k) or "text", v)
                     for k, v in zip(key, raw(r))]
            values = found.get(text(typed))
            if at_fence and (values is None) != (r.op == "delete"):
                raise SystemExit(
                    f"{db}.{table}: TiCDC says the row keyed"
                    f" {dict(zip(key, raw(r)))} was"
                    f" {'deleted' if r.op == 'delete' else 'written'} by"
                    f" {r.version}, and TiDB as it stood then"
                    f" {'holds' if values is not None else 'does not hold'}"
                    " it: the key as the feed names it is not the key"
                    " here. Nothing of this batch was applied")
            out.append((r, dict(zip(key, typed)) if values is None
                        else {k: values[k] for k in key}, values))
        return out

    # ---- through the pair every engine is carried by ----------------------

    def tail_start(self, db, token_path):
        return self._as_pair().tail_start(db, token_path)

    def tail_apply(self, db, go, token_path, log):
        return self._as_pair().tail_apply(db, go, token_path, log)

    def src_lsn(self, db):
        return self._as_pair().src_lsn(db)

    def fence_wait(self, db, lsn, timeout=300):
        return self._as_pair().fence_wait(db, lsn, timeout)

    def delta_verify(self, db, limit=20000, log=None):
        return self._as_pair().delta_verify(db, limit, log)

    def _compare_pks(self, db, table, keys):
        return self._as_pair()._compare_pks(db, table, keys)

    def _write_pk_files(self, db, table, missing, extra, changed):
        return self._as_pair()._write_pk_files(db, table, missing, extra,
                                                changed)

    def check_schema(self, db):
        return self._as_pair().check_schema(db)

    def check_counts(self, db):
        return self._as_pair().check_counts(db)

    def check_data(self, db, table=None, stream=None, with_counts=False):
        return self._as_pair().check_data(db, table, stream)

    def list_move_tables(self, db):
        return self._as_pair().list_move_tables(db)

    def move_table(self, db, sch, tbl, chunk, ck, log):
        return self._as_pair().move_table(db, sch, tbl, chunk, ck, log)

    def moved_nothing(self, db):
        return self._as_pair().moved_nothing(db)

    def repair_plan(self, db, kind):
        return self._as_pair().repair_plan(db, kind)

    def apply(self, db, action):
        return self._as_pair().apply(db, action)

    # ---- what TiDB keeps of its own ----------------------------------------

    def check_autoinc(self, db):
        """Each AUTO_INCREMENT column's next id on the target past the
        source's highest: each TiDB server hands ids out of its own cached
        range, so the catalogue's AUTO_INCREMENT is not the next id - `SHOW
        TABLE ... NEXT_ROW_ID` is. An AUTO_RANDOM column is left alone: its
        ids are random in their high bits and never run into each other."""
        out = []
        dst = self._d("dst", db)
        rows = self._q("src", "select table_name, column_name from"
                              " information_schema.columns where"
                              " table_schema = %s and extra like"
                              " '%%auto_increment%%'", (db,))
        scope = set(self.neutral_tables("src", db))
        for t, col in rows:
            if t not in scope:
                continue
            hi = self._q("src", f"select max(`{col}`) from `{db}`.`{t}`")[0][0]
            try:
                got = self._q("dst", f"show table `{dst}`.`{t}` next_row_id")
            except Exception as e:  # noqa: BLE001 - said as the error
                out.append(Result("autoinc", f"{db}.{t}", "error",
                                  "could not read the target's next id:"
                                  f" {str(e).splitlines()[0][:90]}"))
                continue
            nxt = next((int(r[3]) for r in got
                        if str(r[2]).lower() == str(col).lower()
                        and str(r[4]).upper() == "AUTO_INCREMENT"), None)
            if hi is None or nxt is None:
                out.append(Result("autoinc", f"{db}.{t}", "ok",
                                  f"{col}: nothing to run into"))
            elif nxt > int(hi):
                out.append(Result("autoinc", f"{db}.{t}", "ok",
                                  f"{col}: the target's next id {nxt} is past"
                                  f" the source's highest {hi}"))
            else:
                out.append(Result(
                    "autoinc", f"{db}.{t}", "diff",
                    f"{col}: the target's next id {nxt} is not past the"
                    f" source's highest {hi} - the application's next insert"
                    " there collides", "",
                    f"alter table `{dst}`.`{t}` auto_increment ="
                    f" {int(hi) + 1}"))
        return out or [Result("autoinc", db, "ok",
                              "no AUTO_INCREMENT column in scope")]

    def check_deep(self, db):
        """Every target table's rows and indexes agreeing (`ADMIN CHECK
        TABLE`): a load that broke an index leaves rows a query by that
        index does not find, and a row-by-row comparison by key reads
        past it."""
        out = []
        dst = self._d("dst", db)
        bad = []
        for t in self.neutral_tables("dst", db):
            try:
                self._q("dst", f"admin check table `{dst}`.`{t}`")
            except Exception as e:  # noqa: BLE001 - its finding
                bad.append(f"{t} ({str(e).splitlines()[0][:60]})")
        if bad:
            out.append(Result("deep", f"{db} target indexes", "diff",
                              f"{len(bad)} tables whose indexes do not agree"
                              f" with their rows: {', '.join(bad[:4])}", "",
                              "rebuild those indexes on the target"))
        else:
            out.append(Result("deep", f"{db} target indexes", "ok",
                              "every table's indexes agree with its rows"))
        return out

    def reach_items(self, side):
        """How deep migkit reaches a TiDB side, for `assess`."""
        from .. import changetopic
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "tables made by TiDB's own definition, copied"
                            " by key and compared on the server"}]
        if side != "src":
            return items
        spec = self._changes_spec()
        if not spec:
            items.append({"level": "warn", "scope": "reach",
                          "item": "changes are not followed",
                          "detail": "TiDB keeps no binlog: name the Kafka"
                                    " topic TiCDC publishes to (the source's"
                                    " `changes`) to follow it"})
            return items
        env = changetopic.envelope(spec.get("format", "canal-json"))
        items.append({"level": "pass", "scope": "reach",
                      "item": "changes followed through TiCDC",
                      "detail": f"{env.name} from {spec.get('topic') or spec.get('topics')},"
                                " in commit order by each watermark; it"
                                f" loses {env.LOSES}"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")
