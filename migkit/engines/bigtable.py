"""Google Cloud Bigtable as a side of a pair (backlog P0-CLOUDS), through its
Python SDK.

A hop's database is a Bigtable instance (`bigtable://project/instance`,
`emulator: host:port` for the emulator), and a table one of its tables. A
row is its key - bytes, which the table is ordered and resumed by - and,
for each column family, one JSON value holding every cell of it: each
qualifier (its text, or `~` and its base64 where it is not UTF-8 or begins
with `~`) with every version it keeps, newest first, as [timestamp in
microseconds, value in base64]. So a table copied to another Bigtable
table comes back cell for cell, versions and timestamps and all, and a
copy that dropped a version or rewrote a timestamp is a difference.

A table migkit made from another engine's rows is kept as that engine's
columns (`migkit.described`): the row filed under its key's text, each
column one qualifier of family `c` holding its value's canonical bytes -
read back by the class it was made from.

Rows are read by key ranges: a digest reads the ranges the table's own
sample of its keys splits it into side by side. A row is written whole:
deleted and its cells set again in one mutation, which the service
applies atomically, so a batch written again lands on itself; each row's
own status is read back, and a refused row stops the write.

Its changes are its change stream's (a table's `change_stream_config`),
read partition by partition by the change-topic reader: each partition a
lane, its low watermark its mark, so rows are handed on in commit order
across partitions; a partition that splits or merges goes on through the
continuation tokens its close gives. Bigtable reads no row as it stood at
a moment, so a changed row is read back as it is when its change is read
- a later change brings it level again. The follow's fence is the clock of
this machine less `SINCE` seconds (`changetopic`): changes of the minutes
before the copy began are carried again, which lands on itself, and none
committed after it can be taken for one the copy read.
"""
import base64
import concurrent.futures
import datetime
import json
import re
import time

from .base import Engine, NeutralCopier, Result

KEY = "row_key"
#: the family a table made from another engine's rows keeps its columns in
FAMILY = "c"
#: rows written in one call, each with its own status back
ROWS = 500


def qualifier_text(q):
    """A qualifier as the text a JSON object keys it by, reversibly."""
    try:
        text = bytes(q).decode("utf-8")
    except UnicodeDecodeError:
        text = None
    if text is None or text.startswith("~"):
        return "~" + base64.b64encode(bytes(q)).decode()
    return text


def qualifier_bytes(text):
    if text.startswith("~"):
        return base64.b64decode(text[1:])
    return text.encode("utf-8")


class BigtableEngine(NeutralCopier, Engine):
    checks = ("schema", "counts", "data")
    CANON_ENGINE = "bigtable"
    FOLDED_BECAUSE = "has no way to hash rows in the service"
    OVER_NETWORK = True
    RESUMES_BY_KEY = True
    CHANGE_POINT_READS_ONLY = True
    READS_AHEAD = False

    # ---- where it is ------------------------------------------------------

    def _ep(self, side):
        return self.hop.source if side == "src" else self.hop.target

    def _d(self, side, db):
        return self.hop.target_db(db) if side == "dst" else db

    def _client(self, side):
        held = self.__dict__.setdefault("_clients", {})
        if side not in held:
            from google.cloud import bigtable

            from . import gcp
            ep = self._ep(side)
            held[side] = gcp.client(bigtable.Client, "BIGTABLE_EMULATOR_HOST",
                                    ep, project=gcp.project(ep), admin=True)
        return held[side]

    def _instance(self, side, db):
        return self._client(side).instance(self._d(side, db))

    def _table(self, side, db, table):
        return self._instance(side, db).table(table)

    def databases(self):
        if self.hop.databases:
            return list(self.hop.databases)
        got, _ = self._client("src").list_instances()
        return sorted(i.instance_id for i in got)

    def _where(self, side, db):
        ep = self._ep(side)
        return (f"{ep.options.get('emulator') or ''}/"
                f"{ep.options.get('project') or ep.host}/{self._d(side, db)}")

    def _described(self, side, db, table):
        from .. import described
        return described.read("bigtable", self._where(side, db), table)

    # ---- what a table is ---------------------------------------------------

    def neutral_tables(self, side, db):
        return sorted(t.table_id for t in self._instance(side, db)
                      .list_tables() if self.in_scope(db, t.table_id))

    def target_missing(self, db):
        return not self._instance("dst", db).exists()

    def _families(self, side, db, table):
        return self._table(side, db, table).list_column_families()

    def neutral_columns(self, side, db, table):
        got = self._described(side, db, table)
        if got is not None:
            return got[0]
        return [(KEY, "bytes")] + [(f, "cells") for f in sorted(
            self._families(side, db, table))]

    def neutral_key(self, side, db, table):
        got = self._described(side, db, table)
        return got[1] if got is not None else [KEY]

    def _classes(self, side, db, table):
        from .. import canon
        return {n: canon.type_class("bigtable", t)
                for n, t in self.neutral_columns(side, db, table)}

    def table_facts(self, side, db):
        return {t: {"rows": None, "bytes": None, "key": True}
                for t in self.neutral_tables(side, db)}

    # ---- rows ---------------------------------------------------------------

    def _row(self, names, classes, described, row):
        """A row as the neutral contract reads it."""
        from .. import described as desc
        if described is not None:
            cols, key = described
            values = desc.key_values(classes, key,
                                     bytes(row.row_key).decode("utf-8"))
            cells = row.cells.get(FAMILY, {})
            for n, _ in cols:
                if n in key:
                    continue
                got = cells.get(n.encode("utf-8"))
                values[n] = desc.value_of(classes.get(n),
                                          got[0].value) if got else None
            return [values.get(n) for n in names]
        out = []
        for n in names:
            if n == KEY:
                out.append(bytes(row.row_key))
                continue
            fam = row.cells.get(n)
            out.append(None if not fam else {
                qualifier_text(q): [[int(c.timestamp_micros),
                                     base64.b64encode(c.value).decode()]
                                    for c in cells]
                for q, cells in fam.items()})
        return out

    def _row_key(self, side, db, table, key_values):
        """The bytes a row is filed under, from its key's values."""
        from .. import described as desc
        got = self._described(side, db, table)
        if got is None:
            return bytes(key_values[0])
        cols, key = got
        classes = dict(cols)
        return desc.key_text(classes, key, dict(zip(key, key_values))) \
            .encode("utf-8")

    def _rows(self, side, db, table, columns, row_set=None, limit=None):
        names = [n for n, _ in columns]
        classes = self._classes(side, db, table)
        described = self._described(side, db, table)
        kw = {"row_set": row_set} if row_set is not None else {}
        if limit:
            kw["limit"] = int(limit)
        for row in self._table(side, db, table).read_rows(**kw):
            yield self._row(names, classes, described, row)

    def neutral_read(self, side, db, table, columns, after=None, limit=1000,
                     where=None):
        from google.cloud.bigtable.row_set import RowSet
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a Bigtable"
                             " table takes none")
        rs = None
        if after is not None:
            rs = RowSet()
            rs.add_row_range_from_keys(
                start_key=self._row_key(side, db, table, list(after)),
                start_inclusive=False)
        rows = list(self._rows(side, db, table, columns, rs, limit))
        key = self.neutral_key(side, db, table)
        names = [n for n, _ in columns]
        if not rows or not all(k in names for k in key):
            return rows, None
        return rows, tuple(rows[-1][names.index(k)] for k in key)

    def neutral_batches(self, side, db, table, columns, size=1000,
                        where=None):
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a Bigtable"
                             " table takes none")
        batch = []
        for row in self._rows(side, db, table, columns):
            batch.append(row)
            if len(batch) >= size:
                yield batch
                batch = []
        if batch:
            yield batch

    def neutral_rows_by_key(self, side, db, table, columns, key, keys,
                            where=None):
        from google.cloud.bigtable.row_set import RowSet
        if not key or not keys:
            return {}
        if list(key) != self.neutral_key(side, db, table):
            raise SystemExit(f"{table} is read by its row key alone")
        found = []
        keys = list(keys)
        for i in range(0, len(keys), 1000):
            rs = RowSet()
            for k in keys[i:i + 1000]:
                rs.add_row_key(self._row_key(side, db, table, list(k)))
            found += list(self._rows(side, db, table, columns, rs))
        return self._by_key_map(columns, key, found)

    def neutral_digest(self, side, db, table, columns, where=None):
        """Folded here a key range at a time, the ranges the table's own
        sample of its keys gives read side by side: the digest is a sum,
        whichever range a row came from."""
        from google.cloud.bigtable.row_set import RowSet

        from .. import canon
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a Bigtable"
                             " table takes none")
        classes = [c for _, c in columns]
        cuts = [bytes(s.row_key) for s in
                self._table(side, db, table).sample_row_keys() if s.row_key]
        bounds = [None] + sorted(set(cuts)) + [None]

        def one(i):
            rs = RowSet()
            rs.add_row_range_from_keys(start_key=bounds[i],
                                       end_key=bounds[i + 1])
            total, n, batch = 0, 0, []
            for row in self._rows(side, db, table, columns, rs):
                batch.append(row)
                if len(batch) >= 5000:
                    k, total = canon.fold_rows(classes, batch, total)
                    n, batch = n + k, []
            if batch:
                k, total = canon.fold_rows(classes, batch, total)
                n += k
            return n, total
        n, total = 0, 0
        with concurrent.futures.ThreadPoolExecutor(
                max(1, min(8, len(bounds) - 1))) as pool:
            for k, t in pool.map(one, range(len(bounds) - 1)):
                n, total = n + k, total + t
        return n, str(total)

    # ---- writing -------------------------------------------------------------

    def neutral_create_sql(self, side, db, table, columns, key=()):
        from . import same_engine
        if not key:
            raise SystemExit(f"{table} has no key, and a Bigtable row is"
                             " filed under its key: give the table one, or"
                             " leave it out of this hop")
        if same_engine(self):
            fams = {n: str(r) for n, r in sorted(self._families(
                "src", db, table).items())}
            return json.dumps({"table": table, "families": fams})
        return json.dumps({"table": table, "families": {FAMILY: "max_versions=1"},
                           "key": list(key),
                           "columns": [[c[0], c[1]] for c in columns]})

    def neutral_create(self, side, db, table, columns, key=()):
        """A table as the source's - its families with their garbage
        collection rules - from another Bigtable; one of family `c`, one
        version a cell, described on this machine, from another engine."""
        from google.cloud.bigtable import column_family

        from .. import described
        from . import same_engine
        self._target_only(side, "create a table")
        if table in self.neutral_tables(side, db):
            raise SystemExit(f"{table} already exists on the target -"
                             " migkit will not alter or replace a table that"
                             " is already there")
        spec = json.loads(self.neutral_create_sql(side, db, table, columns,
                                                  key))
        t = self._table(side, db, table)
        if same_engine(self):
            fams = {n: f.gc_rule for n, f in self._families(
                "src", db, table).items()}
            t.create(column_families=fams)
        else:
            t.create(column_families={
                FAMILY: column_family.MaxVersionsGCRule(1)})
            described.keep("bigtable", self._where(side, db), table,
                           [(c[0], c[1]) for c in columns], list(key))
        return json.dumps(spec)

    def _direct(self, side, db, table, names, classes, values):
        """A row's mutation: the row deleted and every cell set again,
        applied as one."""
        from .. import described as desc
        t = self._table(side, db, table)
        got = self._described(side, db, table)
        row = dict(zip(names, values))
        if got is not None:
            cols, key = got
            kc = dict(cols)
            direct = t.direct_row(desc.key_text(kc, key, row).encode("utf-8"))
            direct.delete()
            for n, v in row.items():
                if n in key or v is None:
                    continue
                direct.set_cell(FAMILY, n.encode("utf-8"),
                                desc.cell_of(kc.get(n), v),
                                timestamp=_EPOCH)
            return direct
        direct = t.direct_row(bytes(row[KEY]))
        direct.delete()
        for fam, cells in row.items():
            if fam == KEY or not cells:
                continue
            if isinstance(cells, str):
                cells = json.loads(cells)
            for q, versions in cells.items():
                for ts, value in versions:
                    direct.set_cell(fam, qualifier_bytes(q),
                                    base64.b64decode(value),
                                    timestamp=_EPOCH + datetime.timedelta(
                                        microseconds=int(ts)))
        return direct

    def _send(self, side, db, table, rows):
        """Rows mutated in calls of `ROWS`, each row's own status read
        back: a row the service refused stops the write."""
        t = self._table(side, db, table)
        for i in range(0, len(rows), ROWS):
            part = rows[i:i + ROWS]
            statuses = t.mutate_rows(part)
            bad = [(r.row_key, s) for r, s in zip(part, statuses)
                   if getattr(s, "code", 0)]
            if bad:
                raise SystemExit(f"{table}: the target refused {len(bad)}"
                                 f" rows: {str(bad[0][1])[:150]}")

    def neutral_write(self, side, db, table, columns, rows):
        self._target_only(side, "write rows")
        if not rows:
            return 0
        names = [n for n, _ in columns]
        classes = self._classes(side, db, table)
        self._send(side, db, table, [self._direct(side, db, table, names,
                                                  classes, r) for r in rows])
        return len(rows)

    def _apply_upserts(self, side, db, table, rows):
        classes = self._classes(side, db, table)
        out = []
        for key, values in rows:
            row = {**values, **key}
            out.append(self._direct(side, db, table, list(row), classes,
                                    list(row.values())))
        self._send(side, db, table, out)

    def _apply_upsert(self, side, db, table, key, values):
        self._apply_upserts(side, db, table, [(key, values)])

    def _apply_deletes(self, side, db, table, keys):
        t = self._table(side, db, table)
        pk = self.neutral_key(side, db, table)
        out = []
        for key in keys:
            direct = t.direct_row(self._row_key(side, db, table,
                                                [key[k] for k in pk]))
            direct.delete()
            out.append(direct)
        self._send(side, db, table, out)

    def _apply_delete(self, side, db, table, key):
        self._apply_deletes(side, db, table, [key])

    def neutral_empty(self, side, db, table, where=None):
        self._target_only(side, "empty a table")
        if where:
            raise SystemExit(f"{table}: a row filter is SQL, and a Bigtable"
                             " table takes none")
        self._table(side, db, table).truncate(timeout=600)
        return None

    # ---- the checks ----------------------------------------------------------

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

    #: how long a cell stays and how many versions of it: another rule
    #: collects other cells
    CRITICAL_PARAMS = ()

    def _settings(self, side, db):
        out = {}
        for t in self.neutral_tables(side, db):
            for fam, f in sorted(self._families(side, db, t).items()):
                out[f"{t}.{fam} gc rule"] = repr(f.gc_rule)
        return out

    def check_params(self, db):
        """Each table's column families and the rule each collects its
        cells by, source against target."""
        try:
            src, dst = self._settings("src", db), self._settings("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            return [Result("params", db, "error", "could not read the"
                           f" families: {str(e).splitlines()[0][:90]}")]
        return self._param_result(db, src, dst, tuple(set(src) | set(dst)),
                                  "give the target's families the source's"
                                  " garbage collection rules")

    def _replication(self, side, db):
        """{table: [cluster whose copy of it is not ready, with its state]}
        of the tables in scope."""
        from google.cloud.bigtable.enums import Table as T
        names = {v: k.lower().replace("_", " ") for k, v in
                 vars(T.ReplicationState).items() if k.isupper()}
        out = {}
        for t in self.neutral_tables(side, db):
            behind = [f"{c} {names.get(s.replication_state, s)}"
                      for c, s in sorted(self._table(side, db, t)
                                         .get_cluster_states().items())
                      if s.replication_state != T.ReplicationState.READY]
            if behind:
                out[t] = behind
        return out

    def check_deep(self, db):
        """Every cluster of the target holding its copy of each table, ready
        - a cluster whose copy is still initializing answers reads from
        part of it - and on the source, each table's change stream on,
        which a follow reads."""
        out = []
        try:
            behind = self._replication("dst", db)
        except Exception as e:  # noqa: BLE001 - said, as the error
            out.append(Result("deep", f"{db} target", "error",
                              "could not read the tables' clusters:"
                              f" {str(e).strip().splitlines()[0][:90]}"))
        else:
            out.append(Result(
                "deep", f"{db} target replication",
                "diff" if behind else "ok",
                ("copies not ready: " + "; ".join(
                    f"{t} ({', '.join(c)})" for t, c in
                    sorted(behind.items())[:4])) if behind else
                "every cluster holds every table, ready", "",
                "wait until every cluster's copy is ready" if behind
                else ""))
        try:
            off = [t for t in self.neutral_tables("src", db)
                   if self._described("src", db, t) is None
                   and self._retention_s(db, t) is None]
        except Exception as e:  # noqa: BLE001 - said, as the error
            out.append(Result("deep", f"{db} source", "error",
                              "could not read the tables' change streams:"
                              f" {str(e).strip().splitlines()[0][:90]}"))
        else:
            if off:
                out.append(Result(
                    "deep", f"{db} source change streams", "warn",
                    f"{len(off)} tables keep no change stream"
                    f" ({', '.join(off[:4])}): their changes cannot be"
                    " followed", "",
                    "turn each table's change stream on, with a retention"
                    " longer than the copy takes"))
        return out

    #: how long a backup taken before a repair is kept
    BACKUP_DAYS = 7

    def snapshot_state(self, db, state_dir, kind="all"):
        """A backup of every target table in scope, taken by the service on
        the instance's first cluster and kept a week, its name kept - each
        table's own, at its own moment; where the instance takes none (the
        emulator), the reason is kept instead."""
        out, now = {}, datetime.datetime.now(datetime.timezone.utc)
        try:
            clusters, _ = self._instance("dst", db).list_clusters()
            cluster = sorted(c.cluster_id for c in clusters)[0]
        except Exception as e:  # noqa: BLE001 - kept as the reason
            cluster, out["no_backup"] = None, str(e).strip().splitlines()[
                0][:160]
        for t in self.neutral_tables("dst", db):
            if cluster is None:
                break
            name = re.sub(r"[^a-zA-Z0-9_.-]+", "-",
                          f"migkit-{state_dir.name}-{t}")[:50]
            try:
                backup = self._table("dst", db, t).backup(
                    name, cluster_id=cluster,
                    expire_time=now + datetime.timedelta(
                        days=self.BACKUP_DAYS))
                backup.create(cluster).result(3600)
                out[t] = {"backup": f"{cluster}/{name}"}
            except Exception as e:  # noqa: BLE001 - kept as the reason
                out[t] = {"no_backup": str(e).strip().splitlines()[0][:160]}
            out[t]["families"] = {f: repr(g.gc_rule) for f, g in sorted(
                self._families("dst", db, t).items())}
        (state_dir / "dst-bigtable.json").write_text(
            json.dumps(out, indent=2, sort_keys=True))

    # ---- who it is -----------------------------------------------------------

    def server_identity(self, side, db):
        ep = self._ep(side)
        inst = self._instance(side, db)
        out = {"name": f"{ep.options.get('project') or ep.host}/"
                       f"{self._d(side, db)}",
               "id": inst.name}
        if not inst.exists():
            out["absent"] = True
        return out

    def stream_identity(self, side, db):
        return {"instance": self._instance(side, db).name}

    # ---- the change stream ---------------------------------------------------

    def _follower(self, db):
        from .. import changetopic
        held = self.__dict__.setdefault("_followers", {})
        if db not in held:
            tables = self.neutral_tables("src", db)
            held[db] = changetopic.Follower(
                changetopic.BigtableChanges(),
                _Tables({t: BigtableLanes(self, db, t) for t in tables}),
                name=lambda row: row.table,
                read_back=lambda t, rows, v, db=db: self._read_back(
                    db, t, rows))
        return held[db]

    def _read_back(self, db, table, rows):
        """Each row the stream named, as it is now: a later change brings
        it level again."""
        cols = self.neutral_columns("src", db, table)
        key = self.neutral_key("src", db, table)
        from google.cloud.bigtable.row_set import RowSet
        rs = RowSet()
        wanted = {}
        for r in rows:
            k = base64.b64decode(r.key[KEY])
            wanted[k] = r
            rs.add_row_key(k)
        found = {}
        names = [n for n, _ in cols]
        for row in self._rows("src", db, table, cols, rs):
            found[bytes(row[names.index(KEY)])] = row
        out = []
        for k, r in wanted.items():
            got = found.get(k)
            out.append((r, {KEY: k} if got is None else
                        {c: got[names.index(c)] for c in key},
                        None if got is None else dict(zip(names, got))))
        return out

    def change_point(self, side, db):
        from ..changetopic import SINCE
        made = [t for t in self.neutral_tables("src", db)
                if self._described("src", db, t) is not None]
        if made:
            raise SystemExit(
                f"{db}: {', '.join(made[:4])} were made by migkit from another"
                " engine's rows, and are followed from that engine, not from"
                " here")
        return self._follower(db).point(
            fence=lambda: int((time.time() - SINCE) * 10 ** 6))

    def copy_point(self, side, db):
        return self.change_point(side, db)

    def log_position(self, side, db):
        from ..changetopic import SINCE
        return {"fence": int((time.time() - SINCE) * 10 ** 6)}

    @staticmethod
    def position_reached(have, want):
        from ..changetopic import Follower
        return Follower.reached(have, want)

    def neutral_changes(self, side, db, token=None, limit=1000):
        """Each row the change stream says changed, in commit order across
        its partitions, as the table holds it when the change is read."""
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

    def _retention_s(self, db, table):
        """Seconds of changes a table's change stream keeps, or None where
        its stream is off."""
        t = self._table("src", db, table)
        admin = self._client("src").table_admin_client
        got = admin.get_table(request={"name": t.name, "view": "FULL"})
        config = getattr(got, "change_stream_config", None)
        period = getattr(config, "retention_period", None) if config else None
        if not period:
            return None
        return period.total_seconds() if hasattr(period, "total_seconds") \
            else float(getattr(period, "seconds", 0))

    def reach_items(self, side):
        items = [{"level": "pass", "scope": "reach",
                  "item": "copied and verified",
                  "detail": "rows read by key ranges and written whole, every"
                            " cell version with its timestamp"}]
        if side == "src":
            from ..changetopic import BigtableChanges
            items.append({"level": "pass", "scope": "reach",
                          "item": "changes followed through its change"
                                  " stream",
                          "detail": f"it loses {BigtableChanges.LOSES}"})
        return items

    def _assess_extra(self):
        return self.reach_items("src")


_EPOCH = datetime.datetime(1970, 1, 1, tzinfo=datetime.timezone.utc)


class BigtableLanes:
    """One table's change stream partitions, each read by the stream's own
    request from where it was - its continuation token, or a start time
    for a partition with none yet - to now. A partition's low watermark is
    its mark; a partition closed by a split or a merge is replaced by the
    ones its close names, each from the token it gives."""
    KIND = "bigtable"

    def __init__(self, engine, db, table):
        self.engine, self.db, self.table = engine, db, table
        self.live = None

    def _name(self):
        return self.engine._table("src", self.db, self.table).name

    def _data(self):
        return self.engine._client("src").table_data_client

    @staticmethod
    def _lane(row_range):
        return (base64.b64encode(bytes(row_range.start_key_closed or b""))
                .decode() + ".." + base64.b64encode(
                    bytes(row_range.end_key_open or b"")).decode())

    @staticmethod
    def _range(lane):
        start, _, end = lane.partition("..")
        return {"start_key_closed": base64.b64decode(start),
                "end_key_open": base64.b64decode(end)}

    def lanes(self):
        if self.live is None:
            self.live = {self._lane(r.partition.row_range) for r in
                         self._data().generate_initial_change_stream_partitions(
                             request={"table_name": self._name()})}
        return sorted(self.live)

    def ends(self):
        from ..changetopic import SINCE
        since = int((time.time() - SINCE) * 10 ** 6)
        self.live = None
        return {lane: {"at": since, "since": since} for lane in self.lanes()}

    def _request(self, lane, pos, end_us):
        req = {"table_name": self._name(),
               "partition": {"row_range": self._range(lane)},
               "end_time": _EPOCH + datetime.timedelta(microseconds=end_us),
               "heartbeat_duration": {"seconds": 1}}
        if pos.get("token"):
            req["continuation_tokens"] = {"tokens": [{
                "partition": {"row_range": self._range(lane)},
                "token": pos["token"]}]}
        else:
            req["start_time"] = _EPOCH + datetime.timedelta(
                microseconds=int(pos.get("since") or pos["at"]))
        return req

    def read(self, at, most, seconds):
        out, nxt, caught = [], {}, True
        end_us = int(time.time() * 10 ** 6)
        stop = time.monotonic() + seconds
        for lane, pos in sorted((at or {}).items()):
            if len(out) >= most or time.monotonic() > stop:
                nxt[lane] = pos
                caught = False
                continue
            pos = dict(pos)
            row, closed = None, False
            for resp in self._data().read_change_stream(
                    request=self._request(lane, pos, end_us)):
                kind = resp._pb.WhichOneof("stream_record")
                if kind == "data_change":
                    dc = resp.data_change
                    row = row or bytes(dc.row_key)
                    if not dc.done:
                        continue
                    commit = _micros(dc.commit_timestamp)
                    pos = {"at": _micros(dc.estimated_low_watermark)
                           or pos["at"], "token": dc.token}
                    out.append((lane, dict(pos), None,
                                {"row_key": base64.b64encode(row).decode(),
                                 "commit_us": commit}, self.table))
                    row = None
                elif kind == "heartbeat":
                    hb = resp.heartbeat
                    pos = {"at": _micros(hb.estimated_low_watermark)
                           or pos["at"],
                           "token": hb.continuation_token.token}
                    out.append((lane, dict(pos), None, {"mark": pos["at"]},
                                self.table))
                elif kind == "close_stream":
                    cs = resp.close_stream
                    tokens = {self._lane(t.partition.row_range): t.token
                              for t in cs.continuation_tokens}
                    new = [self._lane(p.row_range) for p in cs.new_partitions]
                    if new:
                        closed = True
                        for lane2 in new:
                            nxt[lane2] = {"at": pos["at"],
                                          "token": tokens.get(lane2)}
                    elif tokens.get(lane):
                        pos["token"] = tokens[lane]
            if not closed:
                nxt[lane] = pos
        self.live = set(nxt)
        return out, nxt, caught

    def lost(self, at):
        keep = self.engine._retention_s(self.db, self.table)
        if keep is None:
            return (f"{self.table} has no change stream on: its changes are"
                    " kept nowhere a client can read")
        oldest = min((int(p["at"]) for p in (at or {}).values()),
                     default=None)
        if oldest is not None and time.time() - oldest / 10 ** 6 > keep:
            return (f"{self.table}'s change stream keeps {keep / 3600:g}"
                    " hours, and the saved position is older")
        return None

    def room(self, at):
        keep = self.engine._retention_s(self.db, self.table)
        oldest = min((int(p["at"]) for p in (at or {}).values()),
                     default=None)
        if keep is None or oldest is None:
            return None
        return {"seconds": max(0.0, keep - (time.time() - oldest / 10 ** 6))}

    def close(self):
        return None


def _micros(ts):
    """A protobuf timestamp (or the datetime proto-plus makes of one) in
    microseconds since the epoch; 0 where it is unset."""
    if ts is None:
        return 0
    if isinstance(ts, datetime.datetime):
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.timezone.utc)
        return (ts - _EPOCH) // datetime.timedelta(microseconds=1)
    return int(getattr(ts, "seconds", 0)) * 10 ** 6 + int(
        getattr(ts, "nanos", 0)) // 1000


class _Tables:
    """The lanes of every table of an instance, one reader over them."""
    KIND = "bigtable"

    def __init__(self, by_table):
        self.by = by_table

    def _split(self, at):
        out = {t: {} for t in self.by}
        for lane, pos in (at or {}).items():
            t, _, rest = lane.partition("|")
            if t in out:
                out[t][rest] = pos
        return out

    def lanes(self):
        return [f"{t}|{lane}" for t, lanes in self.by.items()
                for lane in lanes.lanes()]

    def ends(self):
        return {f"{t}|{lane}": pos for t, lanes in self.by.items()
                for lane, pos in lanes.ends().items()}

    def read(self, at, most, seconds):
        out, nxt, caught = [], {}, True
        parts = self._split(at)
        for t, lanes in self.by.items():
            got, after, done = lanes.read(parts[t], most - len(out), seconds)
            out += [(f"{t}|{lane}", pos, k, v, topic)
                    for lane, pos, k, v, topic in got]
            nxt.update({f"{t}|{lane}": pos for lane, pos in after.items()})
            caught = caught and done
        return out, nxt, caught

    def lost(self, at):
        parts = self._split(at)
        for t, lanes in self.by.items():
            why = lanes.lost(parts[t])
            if why:
                return why
        return None

    def room(self, at):
        parts = self._split(at)
        got = [lanes.room(parts[t]) for t, lanes in self.by.items()]
        got = [g["seconds"] for g in got if g]
        return {"seconds": min(got)} if got else None

    def close(self):
        return None


def _register():
    """What of the SDK is used, read off this file, and why the rest is
    not (`tooluse`)."""
    from .. import tooluse
    unused = tooluse.UNUSED
    tooluse.register("google-cloud-bigtable", licence="Apache-2.0", reads=(
        ("engines/bigtable.py",),
        {("Table", "read_rows"): "read_rows",
         ("Table", "sample_row_keys"): "sample_row_keys",
         ("Table", "mutate_rows"): "mutate_rows",
         ("Table", "truncate"): "truncate",
         ("Table", "create"): "create",
         ("Table", "list_column_families"): "list_column_families",
         ("DirectRow", "delete"): "delete",
         ("DirectRow", "set_cell"): "set_cell",
         ("RowSet", "add_row_range_from_keys"): "add_row_range_from_keys",
         ("RowSet", "add_row_key"): "add_row_key",
         ("change stream", "read_change_stream"): "read_change_stream",
         ("change stream", "generate_initial_change_stream_partitions"):
             "generate_initial_change_stream_partitions",
         ("change stream", "continuation_tokens"): "continuation_tokens",
         ("change stream", "heartbeat_duration"): "heartbeat_duration"}),
        options={
        ("Table", "mutations_batcher"): (
            unused, "its failures come back after a flush, apart from the"
                    " rows they belong to; each call's own statuses are read"
                    " row by row instead"),
        ("Table", "drop_by_prefix"): (
            unused, "a table is emptied whole, or not at all"),
        ("Table", "read_modify_write_row"): (
            unused, "an append or an increment is the application's; a copy"
                    " writes the cells as they are"),
        ("Table", "execute_query"): (
            unused, "its SQL reads the latest cell of each column, where a"
                    " copy carries every version"),
    })


_register()
