"""Distributed databases on MySQL's wire as sides of a pair (backlog
P0-CLOUDS): Alibaba PolarDB-X and Tencent TDSQL (distributed), and the
base OceanBase's MySQL mode stands on too (`oceanbase`).

Each takes a MySQL client and MySQL's SQL, so the MySQL engine's
connection, catalogue, reads and writes are theirs. What differs is
underneath: data spread over shards behind a front end, no server a
replica can be made of, and a binlog - where there is one - written by
another component than a MySQL server's. So everything that makes a
native replica, marks two-way traffic in a MySQL binlog, or drives MySQL's
own programs is not theirs (`NotHere`); a table is compared by a digest
their SQL works out only where the table's first rows are proved to
render there as here (`provedsql`), folded here otherwise.

PolarDB-X keeps one binlog for the whole instance - its CDC component's
global binlog, merged from every shard's in the order transactions
committed - which a MySQL binlog reader reads through the front end as
MySQL's: the MySQL engine's own tail follows it. Its row images are
always whole, and it asks no setting of the server. An AUTO_INCREMENT
column takes its numbers from a sequence (`AUTO_SEQ_<table>`), which is
what is compared.

TDSQL spreads a table over its sets by the table's shard key; the proxy
in front keeps no binlog. Each set is a MySQL server with its own, and a
row lives in one set only - a set's binlog carries every change of the
rows it holds, in their order. So a TDSQL source is followed set by set
where the side names each set's primary (`sets: [host:port, ...]`), each
read by the MySQL engine's own tail, the position one per set and reached
when every set's is. A table made on a TDSQL target is sharded by the
first column of its key (`shardkey`), unless the side says a table is not
to be (`shardkey: none`).
"""
from .base import Engine, NotHere, Result
from .mysql import MySQLEngine
from .provedsql import ProvedDigest


class OnMySQLWire(ProvedDigest, MySQLEngine):
    """What a distributed service on MySQL's wire keeps of the MySQL
    engine, and what it does not have."""
    PROVED_DIALECT = "mysql"
    CLIENT_TOOLS = ()
    REPLICATES_THE_SERVER = False
    #: the pair's data check counts nothing of its own: counts are asked
    #: apart, as every pair's are
    counts_from_data = False
    #: the words its version or version comment carry
    BRAND = ()
    #: what it is called where an operator reads it
    SAID = ""

    # ways that make or read a native replica, mark a two-way stream in a
    # MySQL binlog, or run MySQL's own programs: none of them is here
    replicate_sql = NotHere()
    apply_replication_stmt = NotHere()
    replication_status = NotHere()
    setup_target_plan = NotHere()
    loops_prevented = NotHere()
    purge_backlog = NotHere()
    migration_pair = NotHere()
    rehearse = NotHere()
    restore_rows = NotHere()
    origin_mark = NotHere()
    origin_seen = NotHere()
    twoway_uuid = NotHere()
    mark_facts = NotHere()
    mark_committed = NotHere()
    mark_prove = NotHere()
    mark_teardown = NotHere()
    snapshot_state = NotHere()
    native_replica_unsafe = Engine.native_replica_unsafe
    stream_writers = Engine.stream_writers
    pause_writer = Engine.pause_writer
    resume_writer = Engine.resume_writer
    who_wrote = Engine.who_wrote
    assess = Engine.assess

    def load_window(self, db, log=None, tables=None):
        """Nothing to set aside: triggers are not what these keep."""
        return Engine.load_window(self, db, log, tables)

    # ---- the proved digest -------------------------------------------------

    def _proved_rows(self, side, db, sql):
        return [list(r) for r in self._q(side, sql)]

    def _proved_table(self, side, db, table):
        return f"`{self._d(side, db)}`.`{table}`"

    def _proved_quote(self, name):
        return "`" + str(name).replace("`", "``") + "`"

    # ---- who it is -----------------------------------------------------------

    def _identity_probes(self):
        """What names the server or cluster, asked in order, the first that
        answers taken."""
        return ("select @@server_uuid",)

    def _cluster(self, side):
        for q in self._identity_probes():
            try:
                got = self._q(side, q)
            except Exception:  # noqa: BLE001 - not this one's: the next
                continue
            if got and got[0] and got[0][-1] not in (None, ""):
                return str(got[0][-1])
        ep = self.hop.source if side == "src" else self.hop.target
        return f"{ep.host}:{ep.port}"

    def server_identity(self, side, db):
        name = self._d(side, db)
        got = self._q(side, "select @@hostname, version(),"
                            " @@version_comment, (select"
                            " sum(data_length + index_length) from"
                            " information_schema.tables where table_schema ="
                            " %s), (select count(*) from"
                            " information_schema.schemata where schema_name"
                            " = %s)", (name, name))
        host, version, comment, size, there = got[0]
        said = f"{version} {comment}".lower()
        if self.BRAND and not any(b in said for b in self.BRAND):
            raise SystemExit(
                f"the {'source' if side == 'src' else 'target'} is not"
                f" {self.SAID} ({version}, {comment}): name its engine mysql")
        out = {"name": str(host or ""), "version": str(version),
               "id": self._cluster(side)}
        if name and not int(there):
            out["absent"] = True
        elif name:
            out["bytes"] = int(size or 0)
        return out

    def stream_identity(self, side, db):
        return {"server": self._cluster(side)}

    # ---- through the pair every engine is carried by ----------------------

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

    def _assess_extra(self):
        return self.reach_items("src")


class PolarDBXEngine(OnMySQLWire):
    CANON_ENGINE = "polardbx"
    BRAND = ("polardb-x", "polardbx", "tddl")
    SAID = "PolarDB-X"
    #: its global binlog carries whole rows and their columns' names,
    #: whatever a server setting says
    BINLOG_FULL = ()

    def _identity_probes(self):
        return ("select @@polardbx_instance_id",
                "show variables like 'instance_id'",
                "select @@server_uuid")

    def change_point(self, side, db):
        try:
            return super().change_point(side, db)
        except (SystemExit, Exception) as e:  # noqa: BLE001 - said, with where
            raise SystemExit(
                "PolarDB-X answers no binlog position"
                f" ({str(e).strip().splitlines()[0][:80] if str(e).strip() else type(e).__name__}):"
                " its global binlog is written by its CDC component, which"
                " the instance must have running (and the account"
                " REPLICATION SLAVE and REPLICATION CLIENT) before its"
                " changes can be followed") from None

    def _sequences(self, side):
        """{sequence name: its next value}."""
        out = {}
        for r in self._q_named(side, "show sequences"):
            low = {str(k).lower(): v for k, v in r.items()}
            name = low.get("name")
            if name is not None:
                out[str(name).upper()] = low.get("value")
        return out

    def check_autoinc(self, db):
        """Each AUTO_INCREMENT column's sequence on the target past the
        source's highest value: PolarDB-X numbers a table's rows from
        `AUTO_SEQ_<table>`, which the catalogue's AUTO_INCREMENT does not
        show."""
        out = []
        rows = self._q("src", "select table_name, column_name from"
                              " information_schema.columns where"
                              " table_schema = %s and extra like"
                              " '%%auto_increment%%'", (db,))
        scope = set(self.neutral_tables("src", db))
        try:
            seqs = self._sequences("dst")
        except Exception as e:  # noqa: BLE001 - said as the error
            return [Result("autoinc", db, "error", "could not read the"
                           f" target's sequences: {str(e).splitlines()[0][:90]}")]
        for t, col in rows:
            if t not in scope:
                continue
            hi = self._q("src", f"select max(`{col}`) from `{db}`.`{t}`")[0][0]
            name = f"AUTO_SEQ_{t}".upper()
            nxt = seqs.get(name)
            if nxt is None:
                out.append(Result("autoinc", f"{db}.{t}", "diff",
                                  f"{col}: the target has no sequence {name}",
                                  "", "make the table on the target as"
                                      " PolarDB-X makes one with"
                                      " AUTO_INCREMENT"))
            elif hi is None or int(nxt) > int(hi):
                out.append(Result("autoinc", f"{db}.{t}", "ok",
                                  f"{col}: the target's next {nxt} is past the"
                                  f" source's highest {hi}"))
            else:
                out.append(Result(
                    "autoinc", f"{db}.{t}", "diff",
                    f"{col}: the target's next {nxt} is not past the"
                    f" source's highest {hi}", "",
                    f"alter sequence {name} start with {int(hi) + 1}"))
        return out or [Result("autoinc", db, "ok",
                              "no AUTO_INCREMENT column in scope")]

    def check_deep(self, db):
        """Every target table's shards and global indexes agreeing with it
        (`CHECK TABLE`): a table whose physical shards disagree with its
        topology answers queries by some keys and not others."""
        bad = []
        dst = self._d("dst", db)
        for t in self.neutral_tables("dst", db):
            try:
                for r in self._q_named("dst", f"check table `{dst}`.`{t}`"):
                    low = {str(k).lower(): v for k, v in r.items()}
                    if str(low.get("msg_type", "")).lower() == "error":
                        bad.append(f"{t} ({str(low.get('msg_text'))[:60]})")
                        break
            except Exception as e:  # noqa: BLE001 - its finding
                bad.append(f"{t} ({str(e).splitlines()[0][:60]})")
        if bad:
            return [Result("deep", f"{db} target tables", "diff",
                           f"{len(bad)} tables whose shards or indexes do"
                           f" not agree: {', '.join(bad[:4])}", "",
                           "rebuild those tables on the target")]
        return [Result("deep", f"{db} target tables", "ok",
                       "every table's shards and indexes agree")]

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "tables copied by key through the front end,"
                            " digested by its SQL where proved on each table"}]
        if side == "src":
            items.append({"level": "pass", "scope": "reach",
                          "item": "changes followed through its global"
                                  " binlog",
                          "detail": "one binlog for every shard, in commit"
                                    " order, read as MySQL's is"})
        return items


class TdsqlEngine(OnMySQLWire):
    CANON_ENGINE = "dcdb"
    BRAND = ("tdsql",)
    SAID = "TDSQL (distributed)"
    #: the proxy keeps no binlog: a source is followed set by set
    READ_ENDS_INSIDE_TRANSACTIONS = False
    REREADS_FROM_A_POSITION = False

    def _identity_probes(self):
        return ("/*proxy*/show status", "select @@server_uuid")

    # ---- a table made here ---------------------------------------------------

    def neutral_create_sql(self, side, db, table, columns, key=()):
        """MySQL's statement, sharded by the first column of the key - the
        rows of one value of it in one set - unless the side says the
        table is not sharded (`shardkey: none`, or a table's own)."""
        sql = super().neutral_create_sql(side, db, table, columns, key)
        said = (self.hop.target.options or {}).get("shardkey")
        if isinstance(said, dict):
            said = said.get(table)
        if str(said or "").lower() == "none":
            return sql
        col = str(said) if said else (list(key)[0] if key else None)
        if not col:
            raise SystemExit(f"{table} has no key to shard by: give it one,"
                             " or say shardkey: none for it on the target")
        return f"{sql} shardkey={col}"

    # ---- following each set's own binlog --------------------------------------

    def _sets(self):
        """{name: a MySQL engine on one set's primary}, from the source's
        `sets`."""
        from ..config import Endpoint, Hop
        said = self.hop.source.options.get("sets")
        if not said:
            raise SystemExit(
                "TDSQL's proxy keeps no binlog: each set keeps its own. Name"
                " every set's primary in the source's options - sets:"
                " [host:port, ...] - with an account that may read its"
                " binlog (REPLICATION SLAVE, REPLICATION CLIENT)")
        held = self.__dict__.setdefault("_set_engines", {})
        if not held:
            src = self.hop.source
            for one in (said if isinstance(said, list) else str(said)
                        .split(",")):
                one = str(one).strip()
                host, _, port = one.rpartition(":")
                ep = Endpoint(host=host or one, port=int(port or 3306),
                              user=src.user, password=src.password,
                              options={k: v for k, v in src.options.items()
                                       if k.startswith("ssl")})
                hop = Hop(name=f"{self.hop.name}-{one}", engine="mysql",
                          source=ep, target=ep,
                          databases=list(self.hop.databases),
                          options=dict(self.hop.options or {}))
                hop.report_dir = self.hop.report_dir
                held[one] = MySQLEngine(hop)
        return held

    def change_point(self, side, db):
        return {"sets": {name: e.change_point("src", db)
                         for name, e in self._sets().items()}}

    def copy_point(self, side, db):
        return {"sets": {name: e.copy_point("src", db)
                         for name, e in self._sets().items()}}

    def log_position(self, side, db):
        return {"sets": {name: e.log_position("src", db)
                         for name, e in self._sets().items()}}

    @staticmethod
    def position_reached(have, want):
        """Every set's binlog read to or past where `want` put it."""
        if not have or not want:
            return None
        for name, pos in (want.get("sets") or {}).items():
            got = (have.get("sets") or {}).get(name)
            if got is None:
                return False
            ok = MySQLEngine.position_reached(got, pos)
            if not ok:
                return ok
        return True

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each set's changes after its own position, set after set - a
        row's changes are all in its one set, in their order."""
        if token is None:
            return [], self.change_point(side, db)
        out, at = [], {}
        per = max(1, limit // max(1, len(self._sets())))
        for name, e in self._sets().items():
            got, pos = e.neutral_changes(
                "src", db, (token.get("sets") or {}).get(name), per)
            out += got
            at[name] = pos
        return out, {"sets": at}

    def release_changes(self):
        for e in self.__dict__.get("_set_engines", {}).values():
            e.release_changes()

    def position_lost(self, side, db, token):
        for name, e in self._sets().items():
            why = e.position_lost("src", db,
                                  (token.get("sets") or {}).get(name)
                                  if token else None)
            if why:
                return f"set {name}: {why}"
        return None

    def stream_room(self, side, db, token):
        rooms = [e.stream_room("src", db, (token.get("sets") or {}).get(n))
                 for n, e in self._sets().items()] if token else []
        rooms = [r["seconds"] for r in rooms if r and "seconds" in r]
        return {"seconds": min(rooms)} if rooms else None

    def snapshot_mark(self, side, db):
        return None

    @staticmethod
    def mark_covers(side, db, mark, change):
        return None

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "tables copied by key through the proxy, made"
                            " sharded by their key's first column"}]
        if side == "src":
            if self.hop.source.options.get("sets"):
                items.append({"level": "pass", "scope": "reach",
                              "item": "changes followed set by set",
                              "detail": "each set's own binlog, in its order"})
            else:
                items.append({"level": "warn", "scope": "reach",
                              "item": "changes are not followed",
                              "detail": "name each set's primary (the"
                                        " source's `sets`) to follow it"})
        return items
