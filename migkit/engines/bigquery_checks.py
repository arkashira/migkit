"""What a BigQuery dataset keeps beside its rows, read and held to the
source's: the deep checks, the catalogue a pair reads, the assessment's
inventory of what a move leaves.

A BigQuery table has no enforced key, no index, no trigger and no server
to ask for its settings; what it has instead is a layout (partitions,
clusters, expiries), constraints that only describe, views, materialized
views and routines, policies on columns and rows, and the access of its
dataset. This module reads each through the client's metadata calls, which
are not billed, or through `INFORMATION_SCHEMA`, and holds the two sides
to one another. A query that reads a table's rows is billed by the bytes
of the columns it reads, whatever its LIMIT, so every such check asks the
dry run first and reads nothing beyond `SCAN_BYTES_MOST` a query; a table
it did not read is named, never counted as clean.

None of the calls below has run against BigQuery itself: each is the
documented one, pinned by a client of this module's tests, and listed in
`capabilities.UNPROVED` until a project has answered it.
"""
import re

from .. import verdict
from .base import Result

#: bytes one query of a check may read, by its dry run; a table above it
#: is named as not read
SCAN_BYTES_MOST = 50 * 1024 ** 3

#: the names `list_tables` gives a table's type
BASE, VIEW, MATVIEW = "TABLE", "VIEW", "MATERIALIZED_VIEW"
EXTERNAL, SNAPSHOT = "EXTERNAL", "SNAPSHOT"

#: the id of the partition holding rows no range takes
UNPARTITIONED = "__UNPARTITIONED__"


class TooBig(Exception):
    """A query's dry run says it would read more than a check may."""

    def __init__(self, nbytes):
        super().__init__(nbytes)
        self.nbytes = nbytes


def _size(n):
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if n < 1024 or unit == "TiB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024


class BigQueryChecks:
    """The BigQuery engine's reading of its datasets (a mixin of
    `warehouse.BigQueryEngine`, which owns the client, the names and the
    quoting)."""

    #: the deep checks that predict what a move will hit, run by `assess`
    #: before it: a layout, a column rule, a collation or a narrower
    #: column the target already has, which the move would then meet
    PREFLIGHT = ("_deep_partitions", "_deep_columns", "_deep_narrowing",
                 "_deep_collation")
    #: days a snapshot of the target (`snapshot_state`) is kept
    SNAPSHOT_DAYS = 7
    #: bytes one query a check runs may read (`SCAN_BYTES_MOST`)
    SCAN_BYTES_MOST = SCAN_BYTES_MOST
    #: tables whose rows one value check reads at most, the rest named
    SCAN_TABLES_MOST = 30

    # ---- what is in a dataset ---------------------------------------------

    def _kept(self):
        """The reads of the deep check running now, or None: a table's
        metadata and a dataset's listing are asked once for the whole
        check and let go at its end, never kept for the next one - a table
        is made or changed between checks."""
        return self.__dict__.get("_reads")

    def _listing(self, side, db):
        """{name: type} of everything the dataset lists (`TABLE`, `VIEW`,
        `MATERIALIZED_VIEW`, `EXTERNAL`, `SNAPSHOT`), the tables migkit
        makes left out. Listing is a metadata call: not billed."""
        kept = self._kept()
        at = ("listing", side, db)
        if kept is not None and at in kept:
            return kept[at]
        out = {}
        for t in self._client(side).list_tables(self.side_database(side, db)):
            if not self._own(t.table_id):
                out[t.table_id] = str(t.table_type or BASE).upper()
        if kept is not None:
            kept[at] = out
        return out

    def _on_target(self, db, table):
        """The name the hop gives a source table on the target."""
        return str(self.hop.target_table_in(db, table))

    def _shared(self, db, kinds=(BASE,)):
        """[(source name, target name)] of the tables of these types the
        hop takes that both sides have, in order; and the names only one
        side has: (pairs, only the source, only the target)."""
        src = {t: k for t, k in self._listing("src", db).items()
               if k in kinds and self.in_scope(db, t)}
        dst = {t: k for t, k in self._listing("dst", db).items()
               if k in kinds}
        wanted = {t: self._on_target(db, t) for t in src}
        pairs = [(s, d) for s, d in sorted(wanted.items()) if d in dst]
        gone = sorted(s for s, d in wanted.items() if d not in dst)
        extra = sorted(set(dst) - set(wanted.values()))
        return pairs, gone, [t for t in extra if self.in_scope(db, t)]

    def _meta(self, side, db, table):
        """The table's metadata (one call, not billed)."""
        kept = self._kept()
        at = ("meta", side, db, table)
        if kept is not None and at in kept:
            return kept[at]
        got = self._client(side).get_table(self._ref(side, db, table))
        if kept is not None:
            kept[at] = got
        return got

    def _scanned(self, side, sql):
        """(rows, bytes) of a query that reads columns, run only where its
        dry run says it reads at most `SCAN_BYTES_MOST`; `TooBig` where
        it would read more. BigQuery bills by bytes read and a LIMIT does
        not lower them, so the size is known before a byte is paid for."""
        from google.cloud import bigquery
        client = self._client(side)
        dry = client.query(sql, job_config=bigquery.QueryJobConfig(
            dry_run=True, use_query_cache=False))
        nbytes = int(dry.total_bytes_processed or 0)
        if nbytes > self.SCAN_BYTES_MOST:
            raise TooBig(nbytes)
        return [list(r.values()) for r in client.query(sql).result()], nbytes

    def _filtered(self, db, table):
        """` where (<the hop's row filter>)` of a table, or ''."""
        pred = self.hop.row_filter(db, table)
        return f" where ({pred})" if pred else ""

    def _normal(self, side, db, sql):
        """A definition's text with this side's project and dataset names
        blanked, the quoting dropped and the spaces folded: two sides in
        two projects write the same view with other names in it."""
        text = str(sql or "").replace("`", "")
        for name in (self._database(side), self._d(side, db)):
            text = re.sub(rf"(?<![\w.]){re.escape(name)}(?![\w])", "@",
                          text)
        text = re.sub(r"(@\s*\.\s*)+", "@.", text)
        return " ".join(text.split())

    # ---- the deep checks ----------------------------------------------------

    def check_deep(self, db):
        """What a copy of a dataset's rows does not carry or keep, held
        to the source's: the layout of its tables, its views, routines
        and materialized views, its keys (declared, never enforced), the
        columns' rules, collation and policies, the indexes beside the
        tables, who may read, and - from the rows themselves, within what
        a query may read (`SCAN_BYTES_MOST`) - the values a copy can leave
        wrong."""
        out = []
        self.__dict__["_reads"] = {}
        try:
            for check in (self._deep_partitions, self._deep_objects,
                          self._deep_matviews, self._deep_keys,
                          self._deep_fk, self._deep_columns,
                          self._deep_narrowing, self._deep_collation,
                          self._deep_rls, self._deep_indexes,
                          self._deep_grants, self._deep_boundary,
                          self._deep_nullempty, self._deep_float,
                          self._deep_timeshift, self._deep_charset,
                          self._deep_render, self._deep_options,
                          self._deep_schema_diff):
                try:
                    got = check(db)
                except Exception as e:  # noqa: BLE001 - said, rest runs
                    word = check.__name__.replace("_deep_", "")
                    got = Result("deep", f"{db} {word}", "error",
                                 f"could not be read: "
                                 f"{(str(e).splitlines() or [''])[-1][:120]}")
                out += got if isinstance(got, list) else [got]
        finally:
            self.__dict__.pop("_reads", None)
        return out

    # ---- partitions ----------------------------------------------------------

    @staticmethod
    def _layout(meta):
        """What a table's metadata says of how its rows are laid out:
        {"m": method, "e": expression, "cluster": the clustering columns,
        "expires": a partition's life in ms, "filter": whether a query must
        name the partition}."""
        tp = getattr(meta, "time_partitioning", None)
        rp = getattr(meta, "range_partitioning", None)
        method = expr = None
        expires = None
        if tp is not None:
            method = f"time ({str(tp.type_ or 'DAY').lower()})"
            expr = tp.field or "ingestion time"
            expires = tp.expiration_ms
        elif rp is not None:
            rng = rp.range_
            method = "range"
            expr = (f"{rp.field} from {rng.start} to {rng.end} by"
                    f" {rng.interval}")
        return {"m": method, "e": expr,
                "cluster": list(getattr(meta, "clustering_fields", None)
                                or []),
                "expires": expires,
                "filter": bool(getattr(meta, "require_partition_filter",
                                       False))}

    def partition_counts(self, side, db):
        """{table: {partition id: rows}} of every table in the dataset, in
        one read of its `INFORMATION_SCHEMA.PARTITIONS` - the rows each
        partition holds, as BigQuery's storage counts them. A table that
        is not partitioned has a row with no partition id: left out."""
        out = {}
        for table, part, rows in self._rows(
                side, db, "select table_name, partition_id, total_rows from"
                          f" {self._catalog(side, db, 'partitions')}"):
            if part is not None:
                out.setdefault(str(table), {})[str(part)] = int(rows or 0)
        return out

    def _deep_partitions(self, db):
        """Every partitioned table's method, column and clustering on both
        sides, and every partition held to its own row count
        (`Engine._partitions_verdict`): the rows of a partition that
        arrived empty, or stranded in `__UNPARTITIONED__` where a range
        was not made, are in a table's total and not in its partitions.

        The counts are those of BigQuery's storage metadata, not a read of
        the rows, so no digest is compared and nothing is billed; a table
        under a row filter is not compared per partition (the metadata
        counts all of its rows) and is named. A partition expiry on the
        target that the source's table does not have deletes the rows a
        copy has just written: it is a finding of its own."""
        pairs, gone, _ = self._shared(db)
        scheme, tables, unread = [], {}, []
        src_counts = dst_counts = None
        for s, d in pairs:
            a, b = self._layout(self._meta("src", db, s)), \
                self._layout(self._meta("dst", db, d))
            if a["m"] != b["m"] or a["e"] != b["e"]:
                scheme.append(
                    f"{s}: partitioned by {a['m'] or 'nothing'}"
                    f" {a['e'] or ''} on the source,"
                    f" {b['m'] or 'nothing'} {b['e'] or ''} on the target"
                    .replace("  ", " "))
            if a["cluster"] != b["cluster"]:
                scheme.append(f"{s}: clustered by"
                              f" {', '.join(a['cluster']) or 'nothing'} on"
                              f" the source, {', '.join(b['cluster']) or 'nothing'}"
                              " on the target")
            if a["expires"] != b["expires"]:
                scheme.append(
                    f"{s}: partitions expire after {a['expires']} ms on the"
                    f" source, {b['expires']} ms on the target"
                    + (" - rows older than that are deleted as they are"
                       " copied" if b["expires"] else ""))
            if a["filter"] != b["filter"]:
                scheme.append(f"{s}: a query must name the partition:"
                              f" {a['filter']} on the source,"
                              f" {b['filter']} on the target")
            if not (a["m"] or b["m"]):
                continue
            if self.hop.row_filter(db, s):
                unread.append(f"{s} (under a row filter)")
                continue
            if src_counts is None:
                src_counts = self.partition_counts("src", db)
                dst_counts = self.partition_counts("dst", db)
            tables[s] = tuple(
                {p: {"rows": n, "digest": None, "bound": None,
                     "catchall": p == UNPARTITIONED}
                 for p, n in got.get(t, {}).items()}
                for got, t in ((src_counts, s), (dst_counts, d)))
        for s in gone:
            if self._layout(self._meta("src", db, s))["m"]:
                scheme.append(f"{s}: missing on target (partitioned on the"
                              " source)")
        if unread:
            said = f"not compared by partition: {'; '.join(unread[:3])}"
            if not tables and not scheme:
                # nothing else was compared: "no partitioned tables" would
                # be untrue
                return Result("deep", f"{db} partitions", "warn", said)
            unread = [said]
        return self._partitions_verdict(db, tables, scheme, UNPARTITIONED,
                                        unread)

    # ---- objects beside the tables -------------------------------------------

    def _routine_text(self, side, db, routine):
        """A routine's kind, language, arguments, return type and body as
        one text, names blanked."""
        full = self._client(side).get_routine(routine.reference)
        args = ", ".join(
            f"{a.name} {getattr(a.data_type, 'type_kind', a.data_type)}"
            for a in (full.arguments or []))
        return self._normal(
            side, db, f"{full.type_} {full.language or 'SQL'} ({args})"
                      f" returns {full.return_type} as {full.body}")

    def _objects(self, side, db, kinds=None):
        """{kind: {name: its definition, names blanked}} of the views,
        materialized views, routines, external tables and snapshots the
        hop takes (`Engine.takes`). Views and routines are read whole (one
        metadata call each); an external table by where its data lies."""
        out = {"view": {}, "materialized view": {}, "routine": {},
               "external table": {}, "snapshot": {}}
        for name, kind in self._listing(side, db).items():
            if kind == VIEW and self.takes(db, "view", name):
                out["view"][name] = self._normal(
                    side, db, self._meta(side, db, name).view_query)
            elif kind == MATVIEW and self.takes(db, "view", name):
                out["materialized view"][name] = self._normal(
                    side, db, self._meta(side, db, name).mview_query)
            elif kind == EXTERNAL and self.in_scope(db, name):
                cfg = self._meta(side, db, name).external_data_configuration
                out["external table"][name] = " ".join(
                    sorted(getattr(cfg, "source_uris", None) or []))
            elif kind == SNAPSHOT and self.in_scope(db, name):
                out["snapshot"][name] = ""
        if kinds is not None and "routine" not in kinds:
            return out
        for r in self._client(side).list_routines(self.side_database(side, db)):
            kind = "procedure" if str(r.type_) == "PROCEDURE" else "function"
            if self.takes(db, kind, r.routine_id):
                out["routine"][r.routine_id] = self._routine_text(
                    side, db, r)
        return out

    def _deep_objects(self, db):
        """The views, materialized views, routines, external tables and
        snapshots the source has: each on the target, and defined as the
        source's. A copy of rows carries none of them."""
        a, b = self._objects("src", db), self._objects("dst", db)
        gone, differ, extra = [], [], []
        for kind in a:
            for name in sorted(a[kind]):
                theirs = b[kind].get(self._on_target(db, name))
                if theirs is None:
                    gone.append(f"{kind} {name}")
                elif theirs != a[kind][name]:
                    differ.append(f"{kind} {name}")
            extra += [f"{kind} {name}" for name in sorted(b[kind])
                      if name not in {self._on_target(db, n)
                                      for n in a[kind]}]
        total = sum(len(v) for v in a.values())
        if gone or differ:
            parts = []
            if gone:
                parts.append(f"{len(gone)} on the source and not the"
                             f" target: {', '.join(gone[:6])}")
            if differ:
                parts.append(f"{len(differ)} defined differently:"
                             f" {', '.join(differ[:6])}")
            return Result("deep", f"{db} objects", "diff", "; ".join(parts),
                          "", "create each on the target from the source's"
                              " definition: a copy of rows carries none")
        return Result("deep", f"{db} objects", "ok",
                      f"{total} views, routines and other objects, all on"
                      " the target as the source defines them"
                      + (f"; {len(extra)} only on the target: "
                         f"{', '.join(extra[:4])}" if extra else ""))

    def _deep_matviews(self, db):
        """Each materialized view the source has: on the target, refreshed
        as the source's is, and refreshed at all - one never refreshed
        answers from nothing until it is."""
        a, b = (self._objects(side, db, ("materialized view",))
                ["materialized view"] for side in ("src", "dst"))
        findings = []
        for name in sorted(a):
            theirs = self._on_target(db, name)
            if theirs not in b:
                findings.append(f"{name}: missing on target")
                continue
            ms, md = self._meta("src", db, name), self._meta("dst", db,
                                                              theirs)
            if getattr(ms, "mview_enable_refresh", None) != \
                    getattr(md, "mview_enable_refresh", None):
                findings.append(
                    f"{name}: refresh is {ms.mview_enable_refresh} on the"
                    f" source, {md.mview_enable_refresh} on the target")
            if getattr(ms, "mview_refresh_interval", None) != \
                    getattr(md, "mview_refresh_interval", None):
                findings.append(f"{name}: refresh interval differs")
            if getattr(md, "mview_last_refresh_time", None) is None:
                findings.append(f"{name}: never refreshed on the target")
        if findings:
            return Result("deep", f"{db} matviews", "diff",
                          f"{len(findings)}: {'; '.join(findings[:6])}", "",
                          "create the view on the target and refresh it"
                          " (or let its interval do) before the application"
                          " reads it")
        return Result("deep", f"{db} matviews", "ok",
                      f"{len(a)} materialized views, each on the target"
                      " refreshed as the source's is" if a else
                      "no materialized views")

    # ---- keys and constraints ------------------------------------------------

    def foreign_keys(self, side, db):
        """[(child, [columns], parent, [parent's columns])] of every
        foreign key the hop's tables declare, read from each table's own
        metadata. BigQuery never enforces one: they describe, and its
        planner may use them."""
        out = []
        pairs = self._listing(side, db)
        for name in sorted(n for n, k in pairs.items() if k == BASE):
            if not self.in_scope(db, name):
                continue
            cons = getattr(self._meta(side, db, name), "table_constraints",
                           None)
            for fk in getattr(cons, "foreign_keys", None) or []:
                out.append((name,
                            [c.referencing_column
                             for c in fk.column_references],
                            fk.referenced_table.table_id,
                            [c.referenced_column
                             for c in fk.column_references]))
        return out

    def _deep_fk(self, db):
        """The foreign keys the source declares, on the target; and the
        target's rows whose declared key points at no row. BigQuery
        refuses neither, so a key that is declared and broken shows only
        here."""
        a = {(self._on_target(db, c), tuple(cols), self._on_target(db, p),
              tuple(pc)) for c, cols, p, pc in self.foreign_keys("src", db)}
        b = {(c, tuple(cols), p, tuple(pc))
             for c, cols, p, pc in self.foreign_keys("dst", db)}
        lines = [f"{c}({', '.join(cols)}) -> {p}({', '.join(pc)}): missing"
                 " on target" for c, cols, p, pc in sorted(a - b)]
        orphans, unread = [], []
        for child, cols, parent, pcols in sorted(b):
            pairs = [f"c.{self._q(x)} = p.{self._q(y)}"
                     for x, y in zip(cols, pcols)]
            nulls = " and ".join(f"c.{self._q(x)} is not null" for x in cols)
            sql = (f"select count(*) from {self._qualified('dst', db, child)}"
                   f" c where {nulls} and not exists (select 1 from"
                   f" {self._qualified('dst', db, parent)} p where"
                   f" {' and '.join(pairs)})")
            try:
                n = int(self._scanned("dst", sql)[0][0][0])
            except TooBig as big:
                unread.append(f"{child} ({_size(big.nbytes)})")
                continue
            if n:
                orphans.append(f"{child}({', '.join(cols)}): {n} rows"
                               f" with no {parent} row")
        lines += orphans
        if lines:
            return Result("deep", f"{db} fk", "diff",
                          f"{len(lines)}: {'; '.join(lines[:6])}", "",
                          "declare the key on the target, or find the rows"
                          " that point at nothing: BigQuery refuses none")
        if unread:
            return Result("deep", f"{db} fk", "warn",
                          f"{len(b)} declared keys match the source's;"
                          f" not read for orphans, as the read would be"
                          f" too large: {'; '.join(unread[:4])}")
        return Result("deep", f"{db} fk", "ok",
                      f"{len(b)} declared foreign keys, all as the source's,"
                      " none with a row pointing at nothing" if b
                      else "no foreign keys declared")

    def _deep_keys(self, db):
        """The primary key of each table on both sides, and - since
        BigQuery refuses a repeated key - a count of the keys the target
        holds twice: the rows a key was meant to forbid. A table with no
        key is named: its rows cannot be matched one by one."""
        pairs, _, _ = self._shared(db)
        lines, keyless, unread, checked = [], [], [], 0
        for s, d in pairs:
            ks, kd = self.neutral_key("src", db, s), \
                self.neutral_key("dst", db, d)
            if ks != kd:
                lines.append(f"{s}: key {ks or 'none'} on the source,"
                             f" {kd or 'none'} on the target")
            if not kd:
                keyless.append(s)
                continue
            cols = ", ".join(self._q(c) for c in kd)
            sql = (f"select count(*) from (select 1 from"
                   f" {self._qualified('dst', db, d)}"
                   f"{self._filtered(db, s)} group by {cols} having"
                   " count(*) > 1)")
            try:
                n = int(self._scanned("dst", sql)[0][0][0])
            except TooBig as big:
                unread.append(f"{d} ({_size(big.nbytes)})")
                continue
            checked += 1
            if n:
                lines.append(f"{d}: {n} key values held by more than one"
                             " row on the target")
        if lines:
            return Result("deep", f"{db} keys", "diff",
                          f"{len(lines)}: {'; '.join(lines[:6])}", "",
                          "BigQuery does not enforce a key: find why the"
                          " rows repeat (a batch loaded twice, a source that"
                          " allowed it) and remove the extra ones")
        note = ""
        if keyless:
            note += (f"; no key on {len(keyless)} tables, rows cannot be"
                     f" matched one by one: {', '.join(keyless[:4])}")
        if unread:
            note += ("; not read for repeated keys, the read would be too"
                     f" large: {', '.join(unread[:4])}")
        return Result("deep", f"{db} keys", "warn" if keyless or unread
                      else "ok",
                      f"{checked} keys match the source's and hold no value"
                      f" twice{note}")

    # ---- the columns -----------------------------------------------------------

    @staticmethod
    def _flat(fields, prefix=""):
        """{column path: field} of a schema, a record's fields under it."""
        out = {}
        for f in fields:
            out[prefix + f.name] = f
            out.update(BigQueryChecks._flat(getattr(f, "fields", ()) or (),
                                            f"{prefix}{f.name}."))
        return out

    def _column_rules(self, side, db, table):
        """{column path: (mode, default expression, has a policy tag,
        collation, rounding mode)} of one table."""
        out = {}
        for path, f in self._flat(self._meta(side, db, table).schema).items():
            tags = getattr(f, "policy_tags", None)
            out[path] = (str(f.mode or "NULLABLE").upper(),
                         getattr(f, "default_value_expression", None),
                         bool(tags and getattr(tags, "names", None)),
                         (f.to_api_repr() or {}).get("collation"),
                         getattr(f, "rounding_mode", None))
        return out

    def neutral_column_rules(self, side, db, table):
        """{column: {"null": takes NULL, "default": its default expression
        or None, "identity": False}} of a table: a REQUIRED column is
        `not null`, a default is the expression BigQuery evaluates at
        each insert, and nothing is numbered by the table."""
        return {path: {"null": rule[0] != "REQUIRED", "default": rule[1],
                       "identity": False}
                for path, rule in self._column_rules(side, db, table).items()
                if "." not in path}

    def _deep_columns(self, db):
        """Each column's rules on both sides: whether it takes NULL, its
        default, its policy tag (the column-level security BigQuery keeps
        beside the column - a column copied without it is readable by
        everyone who reads the table), and its rounding. A column the
        target lacks is the schema check's finding, not this one's."""
        pairs, _, _ = self._shared(db)
        lines = []
        for s, d in pairs:
            a, b = self._column_rules("src", db, s), \
                self._column_rules("dst", db, d)
            for col in sorted(set(a) & set(b)):
                (m1, d1, t1, _, r1), (m2, d2, t2, _, r2) = a[col], b[col]
                if m1 != m2:
                    lines.append(f"{s}.{col}: {m1} on the source, {m2} on"
                                 " the target")
                if (d1 or "").strip() != (d2 or "").strip():
                    lines.append(f"{s}.{col}: default {d1!r} on the source,"
                                 f" {d2!r} on the target")
                if t1 and not t2:
                    lines.append(f"{s}.{col}: a policy tag restricts it on"
                                 " the source and nothing does on the"
                                 " target")
                elif t2 and not t1:
                    lines.append(f"{s}.{col}: a policy tag restricts it on"
                                 " the target and not on the source")
                if r1 != r2:
                    lines.append(f"{s}.{col}: rounding {r1} on the source,"
                                 f" {r2} on the target")
        if lines:
            return Result("deep", f"{db} columns", "diff",
                          f"{len(lines)}: {'; '.join(lines[:6])}", "",
                          "make the target's columns as the source's:"
                          " nullability, default and policy tag are the"
                          " table's, not the rows'")
        return Result("deep", f"{db} columns", "ok",
                      f"{len(pairs)} tables: nullability, defaults, policy"
                      " tags and rounding match the source's")

    def _deep_narrowing(self, db):
        """A target column declared narrower than the source's - a STRING(n)
        or NUMERIC(p, s) with less room - cuts or refuses what the source
        holds (`verdict.narrowing`)."""
        pairs, _, _ = self._shared(db)
        narrow = []
        for s, d in pairs:
            theirs = dict(self.neutral_columns("dst", db, d))
            for col, declared in self.neutral_columns("src", db, s):
                other = theirs.get(col)
                if other is None:
                    continue
                why = verdict.narrowing("bigquery", declared, other)
                if why:
                    narrow.append(f"{s}.{col}: {why}")
        return verdict.narrowing_result(db, narrow)

    def _deep_collation(self, db):
        """How text compares: the dataset's case sensitivity of table
        names and its default collation, each table's, and each text
        column's. A collation that differs changes which rows a key,
        a join and an ORDER BY treat as the same."""
        lines = []
        a = self._client("src").get_dataset(self.side_database("src", db))
        b = self._client("dst").get_dataset(self.side_database("dst", db))
        if bool(getattr(a, "is_case_insensitive", False)) != bool(
                getattr(b, "is_case_insensitive", False)):
            lines.append("table names are case "
                         + ("insensitive" if a.is_case_insensitive
                            else "sensitive") + " on the source, "
                         + ("insensitive" if b.is_case_insensitive
                            else "sensitive") + " on the target")
        da = (a.to_api_repr() or {}).get("defaultCollation")
        db_ = (b.to_api_repr() or {}).get("defaultCollation")
        if da != db_:
            lines.append(f"dataset default collation {da!r} on the source,"
                         f" {db_!r} on the target")
        pairs, _, _ = self._shared(db)
        for s, d in pairs:
            ta = (self._meta("src", db, s).to_api_repr() or {}) \
                .get("defaultCollation")
            tb = (self._meta("dst", db, d).to_api_repr() or {}) \
                .get("defaultCollation")
            if ta != tb:
                lines.append(f"{s}: table collation {ta!r} on the source,"
                             f" {tb!r} on the target")
            ca, cb = self._column_rules("src", db, s), \
                self._column_rules("dst", db, d)
            for col in sorted(set(ca) & set(cb)):
                if ca[col][3] != cb[col][3]:
                    lines.append(f"{s}.{col}: collation {ca[col][3]!r} on"
                                 f" the source, {cb[col][3]!r} on the target")
        if lines:
            return Result("deep", f"{db} collation", "diff",
                          f"{len(lines)}: {'; '.join(lines[:6])}", "",
                          "give the target the source's collation before"
                          " the rows are compared by key or sorted")
        return Result("deep", f"{db} collation", "ok",
                      "case sensitivity and collations match the source's")

    # ---- policies and indexes ------------------------------------------------

    def _row_policies(self, side, db, table):
        """{policy: (filter predicate, grantees)} of one table, read by
        the API's list of its row access policies."""
        got = self._client(side)._connection.api_request(
            method="GET",
            path=f"/projects/{self._database(side)}/datasets/"
                 f"{self._d(side, db)}/tables/{table}/rowAccessPolicies")
        return {p["rowAccessPolicyReference"]["policyId"]:
                (" ".join(str(p.get("filterPredicate", "")).split()),
                 tuple(sorted(p.get("grantees") or [])))
                for p in (got or {}).get("rowAccessPolicies", [])}

    def _deep_rls(self, db):
        """The row access policies of each table: a policy the source has
        and the target lacks leaves every row of the target readable to
        whoever may read the table; one the target alone has hides rows
        from a check that reads as an ordinary user."""
        pairs, _, _ = self._shared(db)
        lines, held = [], 0
        for s, d in pairs:
            a, b = self._row_policies("src", db, s), \
                self._row_policies("dst", db, d)
            held += len(a)
            for name in sorted(set(a) | set(b)):
                if name not in b:
                    lines.append(f"{s}: policy {name} missing on target")
                elif name not in a:
                    lines.append(f"{s}: policy {name} only on the target")
                elif a[name][0] != b[name][0]:
                    lines.append(f"{s}: policy {name} filters differently")
                elif a[name][1] != b[name][1]:
                    lines.append(f"{s}: policy {name} has other grantees")
        if lines:
            return Result("deep", f"{db} rls", "diff",
                          f"{len(lines)}: {'; '.join(lines[:6])}", "",
                          "create the policies on the target before anyone"
                          " reads it: without them every row is visible")
        return Result("deep", f"{db} rls", "ok",
                      f"{held} row access policies, each on the target as"
                      " the source's" if held
                      else "no row access policies on the source")

    def _index_rows(self, side, db):
        """{(table, index, kind): (columns, status)} of the search and
        vector indexes of the dataset."""
        out = {}
        for kind, views, cols in (
                ("search", "search_indexes", "search_index_columns"),
                ("vector", "vector_indexes", "vector_index_columns")):
            on = {}
            for table, index, column in self._rows(
                    side, db, "select table_name, index_name,"
                              " index_column_name from"
                              f" {self._catalog(side, db, cols)}"):
                on.setdefault((str(table), str(index)), []).append(str(column))
            for table, index, status in self._rows(
                    side, db, "select table_name, index_name, index_status"
                              f" from {self._catalog(side, db, views)}"):
                out[(str(table), str(index), kind)] = (
                    tuple(sorted(on.get((str(table), str(index)), []))),
                    str(status))
        return out

    def _deep_indexes(self, db):
        """The search and vector indexes of each table: on the target, over
        the same columns, and built - one still being built, or disabled,
        answers `SEARCH()` from a scan or not at all."""
        a, b = self._index_rows("src", db), self._index_rows("dst", db)
        lines = []
        for (table, index, kind), (cols, _) in sorted(a.items()):
            if not self.in_scope(db, table):
                continue
            mine = b.get((self._on_target(db, table), index, kind))
            if mine is None:
                lines.append(f"{kind} index {index} on {table}: missing on"
                             " target")
            elif mine[0] != cols:
                lines.append(f"{kind} index {index} on {table}: over"
                             f" {', '.join(cols)} on the source,"
                             f" {', '.join(mine[0])} on the target")
            elif mine[1].upper() != "ACTIVE":
                lines.append(f"{kind} index {index} on {table}: {mine[1]}"
                             " on the target")
        if lines:
            return Result("deep", f"{db} indexes", "diff",
                          f"{len(lines)}: {'; '.join(lines[:6])}", "",
                          "create the index on the target (CREATE SEARCH"
                          " INDEX or CREATE VECTOR INDEX) and wait until it"
                          " is ACTIVE")
        return Result("deep", f"{db} indexes", "ok",
                      f"{len(a)} search and vector indexes, each on the"
                      " target over the same columns" if a
                      else "no search or vector indexes")

    # ---- who may read ----------------------------------------------------------

    PUBLIC = ("allUsers", "allAuthenticatedUsers")

    def _access(self, side, db):
        """{(role, member)} of the dataset's access list."""
        got = self._client(side).get_dataset(self.side_database(side, db))
        return {(str(e.role), f"{e.entity_type}:{e.entity_id}")
                for e in got.access_entries or []}

    def _table_roles(self, side, db, table):
        """{(role, member)} of the table's own IAM policy."""
        policy = self._client(side).get_iam_policy(
            self._ref(side, db, table))
        return {(str(b["role"]), str(m)) for b in policy.bindings
                for m in b.get("members", [])}

    def _deep_grants(self, db):
        """Who may read the dataset and each table, on both sides: the
        dataset's access list and a table's own IAM policy. Principals of
        one project are not another's, so a difference is a warning to
        read - except access the target gives to everyone the source does
        not, which is a finding."""
        a, b = self._access("src", db), self._access("dst", db)
        lines, loud = [], []
        for role, who in sorted(a - b):
            lines.append(f"dataset: {who} has {role} on the source only")
        for role, who in sorted(b - a):
            (loud if any(p in who for p in self.PUBLIC) else lines).append(
                f"dataset: {who} has {role} on the target only")
        pairs, _, _ = self._shared(db)
        for s, d in pairs:
            ra, rb = self._table_roles("src", db, s), \
                self._table_roles("dst", db, d)
            for role, who in sorted(ra - rb):
                lines.append(f"{s}: {who} has {role} on the source only")
            for role, who in sorted(rb - ra):
                (loud if any(p in who for p in self.PUBLIC)
                 else lines).append(
                    f"{s}: {who} has {role} on the target only")
        if loud:
            return Result("deep", f"{db} grants", "diff",
                          f"{len(loud)} given to everyone on the target and"
                          f" not on the source: {'; '.join(loud[:4])}", "",
                          "remove the public access from the target: the"
                          " source never gave it")
        if lines:
            return Result("deep", f"{db} grants", "warn",
                          f"{len(lines)} differ (principals are a"
                          f" project's own): {'; '.join(lines[:6])}", "",
                          "grant the target's principals what the"
                          " application needs; nothing here moves access")
        return Result("deep", f"{db} grants", "ok",
                      "the dataset's access and each table's IAM policy"
                      " match the source's")

    # ---- the rows, read within what a query may read ---------------------------

    def _scan_tables(self, db):
        """([(source name, target name, the table's columns)], the names
        past `SCAN_TABLES_MOST`) the value checks read."""
        pairs, _, _ = self._shared(db)
        more = [s for s, _ in pairs[self.SCAN_TABLES_MOST:]]
        return [(s, d, self.neutral_columns("src", db, s))
                for s, d in pairs[:self.SCAN_TABLES_MOST]], more

    def _outcome(self, bad, read, unread, more, ok_text, diff_text, hint):
        """(status, detail, fix hint) of one value check: a difference, else
        complete or - where a table was not read - a warning that says
        which. Never "ok" for a table nobody read."""
        if bad:
            return ("diff", f"{len(bad)} {diff_text}: {'; '.join(bad[:5])}",
                    hint)
        missed = unread + [f"{t} (past the {self.SCAN_TABLES_MOST}"
                           " tables a check reads)" for t in more[:3]]
        if missed:
            return ("warn", f"{read} tables read and match; not read: "
                            f"{'; '.join(missed[:4])}"
                            + (f" and {len(missed) - 4} more"
                               if len(missed) > 4 else ""), "")
        return "ok", ok_text, ""

    def _typed(self, cols, *classes):
        from .. import canon
        return [c for c, t in cols if canon.type_class("bigquery", t)
                in classes]

    def _deep_boundary(self, db):
        """The largest integer key of each table on both sides: a target
        whose largest is past the source's has been written by something
        that is not the copy - BigQuery refuses no duplicate or stray key
        to say so. One column is read a table."""
        scan, more = self._scan_tables(db)
        tables = []
        for s, d, cols in scan:
            key = self.neutral_key("src", db, s)
            if len(key) == 1 and self._typed(
                    [c for c in cols if c[0] == key[0]], "integer"):
                tables.append((s, d, key[0]))
        read, unread, ahead, behind = 0, [], [], []
        for s, d, k in tables:
            def sql(side, t):
                return (f"select max({self._q(k)}) from"
                        f" {self._qualified(side, db, t)}"
                        f"{self._filtered(db, s)}")
            try:
                a = self._scanned("src", sql("src", s))[0][0][0]
                b = self._scanned("dst", sql("dst", d))[0][0][0]
            except TooBig as big:
                unread.append(f"{s} ({_size(big.nbytes)})")
                continue
            read += 1
            if b is not None and (a is None or int(b) > int(a)):
                ahead.append(f"{s}.{k} src_max={a} dst_max={b}")
            elif a is not None and (b is None or int(b) < int(a)):
                behind.append(f"{s}.{k} src_max={a} dst_max={b}")
        if ahead:
            return Result("deep", f"{db} boundary", "diff",
                          f"target max key AHEAD of the source on"
                          f" {len(ahead)}: {'; '.join(ahead[:4])}", "",
                          "something besides the copy wrote to the target:"
                          " find the writer before cutover")
        status, detail, hint = self._outcome(
            [], read, unread, more,
            f"max key checked on {read} tables, none ahead of the source"
            + (f"; {len(behind)} behind (the copy is not done): "
               f"{'; '.join(behind[:3])}" if behind else "")
            if tables else "no table with a single integer key to check",
            "", "")
        return Result("deep", f"{db} boundary", status, detail, "", hint)

    def _deep_nullempty(self, db):
        """NULL and the empty string are two values; a mover that writes
        one for the other leaves every checksum of a row changed in a
        way the application's `IS NULL` and `= ''` branches both feel.
        Per text column, how many of each, on both sides."""
        tables = [(s, d, self._typed(cols, "text"))
                  for s, d, cols in self._scan_tables(db)[0]]
        tables = [(s, d, c) for s, d, c in tables if c]

        def sql(side, t, cols):
            ref = self._qualified(side, db, t if side == "src" else
                                  self._on_target(db, t))
            sel = ", ".join(f"countif({self._q(c)} is null),"
                            f" countif({self._q(c)} = '')" for c in cols)
            return f"select {sel} from {ref}{self._filtered(db, t)}"
        status, detail, hint = self._value_check(
            db, tables, sql, self._split,
            "text columns with a NULL / empty-string split that differs"
            " (a semantic flip)",
            "NULL and the empty string agree on every text column"
            if tables else "no text columns",
            "decide which side is canonical and normalise with NULLIF /"
            " COALESCE")
        return Result("deep", f"{db} nullempty", status, detail, "", hint)

    def _split(self, t, cols, a, b):
        out = []
        for i, c in enumerate(cols):
            x, y = tuple(a[0][2 * i:2 * i + 2]), tuple(b[0][2 * i:2 * i + 2])
            if x != y:
                out.append(f"{t}.{c}: src null/empty={x[0]}|{x[1]}"
                           f" dst={y[0]}|{y[1]}")
        return out

    def _value_check(self, db, tables, sql, compare, diff_text, ok_text,
                     hint):
        """A value check over `tables` [(source, target, columns)]: the
        query `sql(side, table, columns)` on both sides, `compare` finding
        what differs, the tables a dry run refused named."""
        more = self._scan_tables(db)[1]
        bad, unread, read = [], [], 0
        for s, d, cols in tables:
            try:
                a = self._scanned("src", sql("src", s, cols))[0]
                b = self._scanned("dst", sql("dst", s, cols))[0]
            except TooBig as big:
                unread.append(f"{s} ({_size(big.nbytes)})")
                continue
            read += 1
            bad += compare(s, cols, a, b)
        return self._outcome(bad, read, unread, more, ok_text, diff_text,
                             hint)

    def _deep_float(self, db):
        """FLOAT64 is an approximation, so a digest over it is the wrong
        instrument: the count, the sum and the largest magnitude of each
        column are held to one another within a relative 1e-9."""
        tables = [(s, d, self._typed(cols, "float"))
                  for s, d, cols in self._scan_tables(db)[0]]
        tables = [(s, d, c) for s, d, c in tables if c]

        def sql(side, t, cols):
            ref = self._qualified(side, db, t if side == "src" else
                                  self._on_target(db, t))
            sel = ", ".join(f"count({self._q(c)}),"
                            f" coalesce(sum({self._q(c)}), 0),"
                            f" coalesce(max(abs({self._q(c)})), 0)"
                            for c in cols)
            return f"select {sel} from {ref}{self._filtered(db, t)}"

        def compare(t, cols, a, b):
            out = []
            for i, c in enumerate(cols):
                (n1, s1, m1), (n2, s2, m2) = a[0][3 * i:3 * i + 3], \
                    b[0][3 * i:3 * i + 3]
                if int(n1 or 0) != int(n2 or 0):
                    out.append(f"{t}.{c}: non-null count src={n1} dst={n2}")
                    continue
                s1, s2 = float(s1 or 0), float(s2 or 0)
                scale = max(abs(s1), abs(s2), float(m1 or 0), float(m2 or 0),
                            1.0)
                if abs(s1 - s2) > scale * 1e-9:
                    out.append(f"{t}.{c}: sum src={s1!r} dst={s2!r}")
            return out
        status, detail, hint = self._value_check(
            db, tables, sql, compare,
            "FLOAT64 columns drift beyond tolerance",
            "FLOAT64 columns within tolerance" if tables
            else "no FLOAT64 columns",
            "a mover that changed the column's type or rounded it does"
            " this: compare the column definitions first")
        return Result("deep", f"{db} float", status, detail, "", hint)

    def _deep_timeshift(self, db):
        """A copy that read a DATETIME or TIMESTAMP in a zone not UTC leaves
        every value of the column the same hours out: the first 200 rows
        by key, as microseconds since the epoch, on both sides
        (`verdict.uniform_shift`)."""
        shifts, unread, read = [], [], 0
        scan, more = self._scan_tables(db)
        for s, d, cols in scan:
            key = self.neutral_key("src", db, s)
            when = self._typed(cols, "timestamp")
            if len(key) != 1 or not when:
                continue
            for col in when:
                def sql(side, t):
                    ref = self._qualified(side, db, t)
                    value = (f"unix_micros({self._q(col)})"
                             if self._declared(cols, col) == "TIMESTAMP"
                             else f"unix_micros(timestamp({self._q(col)}))")
                    return (f"select {self._q(key[0])}, {value} from {ref}"
                            f" where {self._q(col)} is not null"
                            f"{self._and_filter(db, s)} order by"
                            f" {self._q(key[0])} limit 200")
                try:
                    a = self._scanned("src", sql("src", s))[0]
                    b = self._scanned("dst", sql("dst", d))[0]
                except TooBig as big:
                    unread.append(f"{s} ({_size(big.nbytes)})")
                    break
                read += 1
                line = verdict.uniform_shift(
                    f"{s}.{col}", {str(k): v / 1e6 for k, v in a},
                    {str(k): v / 1e6 for k, v in b})
                if line:
                    shifts.append(line)
        if shifts:
            return verdict.timeshift_result(db, shifts)
        if unread or more:
            status, detail, _ = self._outcome([], read, unread, more, "",
                                              "", "")
            return Result("deep", f"{db} timeshift", status, detail)
        return verdict.timeshift_result(db, [])

    @staticmethod
    def _declared(cols, name):
        return next((str(t).upper().split("(")[0] for c, t in cols
                     if c == name), "")

    def _and_filter(self, db, table):
        pred = self.hop.row_filter(db, table)
        return f" and ({pred})" if pred else ""

    def _deep_charset(self, db):
        """Text that went through a lossy conversion carries the
        replacement character U+FFFD: its count in each text column,
        on both sides. More of them on the target than the source has is
        a transcode that lost characters."""
        tables = [(s, d, self._typed(cols, "text"))
                  for s, d, cols in self._scan_tables(db)[0]]
        tables = [(s, d, c) for s, d, c in tables if c]

        def sql(side, t, cols):
            ref = self._qualified(side, db, t if side == "src" else
                                  self._on_target(db, t))
            sel = ", ".join(
                f"coalesce(sum(length({self._q(c)}) - length(replace("
                f"{self._q(c)}, '\\ufffd', ''))), 0)" for c in cols)
            return f"select {sel} from {ref}{self._filtered(db, t)}"

        def compare(t, cols, a, b):
            return [f"{t}.{c}: {int(y) - int(x)} extra U+FFFD replacement"
                    " characters on the target"
                    for c, x, y in zip(cols, a[0], b[0])
                    if int(y or 0) > int(x or 0)]
        status, detail, hint = self._value_check(
            db, tables, sql, compare,
            "text columns hold replacement characters the source does not",
            "text columns: no replacement-character excess on the target"
            if tables else "no text columns",
            "re-load the affected rows over a UTF-8 path")
        return Result("deep", f"{db} charset", status, detail, "", hint)

    # ---- the options a table and a dataset are kept under -----------------------

    @staticmethod
    def _dataset_options(ds):
        """What a dataset says of how long it keeps what it holds."""
        enc = getattr(ds, "default_encryption_configuration", None)
        return {"default table expiry (ms)": ds.default_table_expiration_ms,
                "default partition expiry (ms)":
                    ds.default_partition_expiration_ms,
                "time travel (hours)": ds.max_time_travel_hours,
                "storage billing": ds.storage_billing_model,
                "labels": dict(ds.labels or {}),
                "key": getattr(enc, "kms_key_name", None)}

    @staticmethod
    def _table_options(meta):
        """What a table says of how long it is kept and how it is read."""
        enc = getattr(meta, "encryption_configuration", None)
        expires = getattr(meta, "expires", None)
        return {"expires": expires.isoformat() if expires else None,
                "labels": dict(meta.labels or {}),
                "description": meta.description,
                "key": getattr(enc, "kms_key_name", None),
                "max staleness": str(getattr(meta, "max_staleness", None)
                                     or "")}

    def _deep_options(self, db):
        """The options of the dataset and of each table, held to the
        source's: an expiry that deletes what a copy has just written (a
        dataset's default table expiry, a table's), the time travel that a
        snapshot or an undo can reach back through, the key that encrypts
        it, the labels cost and ownership are read by."""
        lines = []
        a = self._dataset_options(
            self._client("src").get_dataset(self.side_database("src", db)))
        b = self._dataset_options(
            self._client("dst").get_dataset(self.side_database("dst", db)))
        lines += [f"dataset {k}: {a[k]!r} on the source, {b[k]!r} on the"
                  " target" for k in a if a[k] != b[k]]
        pairs, _, _ = self._shared(db)
        for s_, d in pairs:
            x = self._table_options(self._meta("src", db, s_))
            y = self._table_options(self._meta("dst", db, d))
            lines += [f"{s_} {k}: {x[k]!r} on the source, {y[k]!r} on the"
                      " target" for k in x if x[k] != y[k]]
        if lines:
            return Result("deep", f"{db} options", "diff",
                          f"{len(lines)}: {'; '.join(lines[:6])}", "",
                          "give the target the source's options; an expiry"
                          " it has and the source has not deletes the rows"
                          " of the copy at that time",
                          category="structure.collection-options")
        return Result("deep", f"{db} options", "ok",
                      f"expiry, time travel, key and labels of the dataset"
                      f" and {len(pairs)} tables match the source's",
                      category="structure.collection-options")

    # ---- the whole definition, and the types that read as text ----------------

    def _ddl(self, side, db):
        """{table: the statement that recreates it, names blanked}, as the
        dataset's `INFORMATION_SCHEMA.TABLES` keeps it: columns, partition
        and cluster, expiry, labels, description, key - everything the
        definition holds."""
        return {str(t): self._normal(side, db, ddl) for t, ddl in self._rows(
            side, db, "select table_name, ddl from"
                      f" {self._catalog(side, db, 'tables')} where"
                      " table_type = 'BASE TABLE'")}

    def _deep_schema_diff(self, db):
        """Each table's whole definition against the source's, one text
        against the other: what the narrower checks above do not name
        (a label, a description, an option, a clause BigQuery added)
        shows here, once per table, as a difference of the definition."""
        a, b = self._ddl("src", db), self._ddl("dst", db)
        pairs, _, _ = self._shared(db)
        differ = [s for s, d in pairs if a.get(s) != b.get(d)]
        if differ:
            return Result("schema", f"{db} (structural)", "diff",
                          f"{len(differ)} of {len(pairs)} tables are"
                          " defined differently from the source's:"
                          f" {', '.join(differ[:6])}", "",
                          "compare the two definitions (INFORMATION_SCHEMA"
                          ".TABLES.ddl) and alter the target's")
        return Result("schema", f"{db} (structural)", "ok",
                      f"{len(pairs)} table definitions identical to the"
                      " source's, project and dataset names aside")

    #: the types whose text a driver may render in more than one way
    EXOTIC = ("GEOGRAPHY", "JSON", "INTERVAL", "RANGE", "STRUCT", "ARRAY")

    def _deep_render(self, db):
        """A GEOGRAPHY, JSON, INTERVAL, RANGE, STRUCT or ARRAY value is
        carried as text and parsed again, and a sample of each column -
        five rows by key - is read as JSON text from both sides: the same
        row must read the same."""
        drifts, audited, unread = [], 0, []
        scan, more = self._scan_tables(db)
        for s, d, cols in scan:
            key = self.neutral_key("src", db, s)
            if len(key) != 1:
                continue
            ints = bool(self._typed([c for c in cols if c[0] == key[0]],
                                    "integer"))
            texts = bool(self._typed([c for c in cols if c[0] == key[0]],
                                     "text"))
            for col, declared in cols:
                if not (ints or texts) or str(declared).upper() \
                        .split("<")[0].split("(")[0] not in self.EXOTIC:
                    continue
                q = self._q(col)
                one = (f"select {self._q(key[0])}, to_json_string({q}) from"
                       f" {{ref}} where {q} is not null"
                       f"{self._and_filter(db, s)} order by"
                       f" {self._q(key[0])} limit 5")
                try:
                    a = {str(k): v for k, v in self._scanned(
                        "src", one.format(
                            ref=self._qualified("src", db, s)))[0]}
                    if not a:
                        continue
                    lits = ", ".join(k if ints else self._lit(k) for k in a)
                    b = {str(k): v for k, v in self._scanned(
                        "dst", f"select {self._q(key[0])},"
                               f" to_json_string({q}) from"
                               f" {self._qualified('dst', db, d)} where"
                               f" {self._q(key[0])} in ({lits})")[0]}
                except TooBig as big:
                    unread.append(f"{s} ({_size(big.nbytes)})")
                    break
                audited += 1
                for k, v in a.items():
                    if k in b and b[k] != v:
                        drifts.append(f"{s}.{col}: renders differently (src"
                                      f" {v[:40]!r} dst {b[k][:40]!r})")
                        break
        status, detail, hint = self._outcome(
            drifts, audited, unread, more,
            f"{audited} GEOGRAPHY, JSON, INTERVAL, RANGE, STRUCT and ARRAY"
            " columns sampled, rendering identical" if audited
            else "no such columns",
            "columns render differently on the target",
            "check the column types on the target")
        return Result("deep", f"{db} render", status, detail, "", hint)

    # ---- repairs, tables made as the move goes, the target's state ----------------

    def repair_plan(self, db, kind):
        """The rows the last check found apart, carried across by the pair
        the hop is seen as (`Engine._as_pair`): each repair kept with the
        statement-level record that puts it back (`repairlog`). There are
        no counters to repair: BigQuery numbers nothing."""
        if kind not in ("rows", "all"):
            return []
        return self._as_pair().repair_plan(db, "rows")

    def apply(self, db, action):
        return self._as_pair().apply(db, action)

    def create_missing(self, db, log=None):
        """The tables the target lacks, made the way the pair makes them,
        before any is filled."""
        return self._as_pair().create_missing(db, log)

    def finish_created(self, db, log=None):
        return self._as_pair().finish_created(db, log)

    def capacity(self, side, db):
        """What the project is doing now, for the move's sizing: the jobs
        running or waiting in it. BigQuery gives a project slots and
        concurrent-query limits by its own quota and reservation, which no
        call here reads, so nothing is said of free connections or cpus."""
        opts = self._endpoint(side).options or {}
        region = str(opts.get("location") or "US").lower()
        got = self._rows(
            side, db, "select count(*) from"
                      f" `region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT"
                      " where creation_time > timestamp_sub("
                      "current_timestamp(), interval 1 day) and state !="
                      " 'DONE'")
        return {"running": int(got[0][0])} if got else {}

    def snapshot_state(self, db, state_dir, kind="all"):
        """What the target held before a repair: the time it was taken
        and, of every table in scope, a table snapshot of its own (a copy
        BigQuery keeps by what the table has changed since, expiring in
        `SNAPSHOT_DAYS` days), with the statements that put each back in
        `dst-snapshots.sql`. A counter is nothing here: `kind` "sequences"
        keeps nothing."""
        if kind == "sequences":
            return
        from .warehouse import OWN
        client = self._client("dst")
        now = [list(r.values()) for r in
               client.query("select current_timestamp()").result()][0][0]
        stamp = now.strftime("%Y%m%d%H%M%S")
        lines = [f"-- the target as it was at {now.isoformat()}, in snapshots"
                 f" kept {self.SNAPSHOT_DAYS} days"]
        names = []
        for t in self.neutral_tables("dst", db):
            snap = f"{OWN}snapshot_{stamp}_{self._leaf(t)}"
            client.query(
                f"create snapshot table {self._qualified('dst', db, snap)}"
                f" clone {self._qualified('dst', db, t)} options("
                f"expiration_timestamp = timestamp_add(current_timestamp(),"
                f" interval {int(self.SNAPSHOT_DAYS)} day))").result()
            names.append((t, snap))
            lines.append(
                f"create or replace table {self._qualified('dst', db, t)}"
                f" clone {self._qualified('dst', db, snap)};")
        (state_dir / "dst-snapshots.sql").write_text("\n".join(lines) + "\n")
        (state_dir / "dst-snapshots.txt").write_text("".join(
            f"{t}|{snap}\n" for t, snap in names))

    # ---- moving the tables whole, by BigQuery itself ----------------------------

    def table_facts(self, side, db):
        """{table: {"rows", "bytes", "key"}} of the tables the hop takes,
        from the dataset's own table metadata and the keys it declares: two
        small queries, whatever the number of tables. The rows are
        BigQuery's count of what it has stored, not of what a streaming
        buffer holds."""
        keyed = {str(t) for (t,) in self._rows(
            side, db, "select table_name from"
                      f" {self._catalog(side, db, 'table_constraints')}"
                      " where constraint_type = 'PRIMARY KEY'")}
        out = {}
        for name, rows, size in self._rows(
                side, db, "select table_id, row_count, size_bytes from"
                          f" {self._q(self._d(side, db))}.__TABLES__ where"
                          " type = 1"):
            if not self._own(name) and self.in_scope(db, str(name)):
                out[str(name)] = {"rows": int(rows or 0),
                                  "bytes": int(size or 0),
                                  "key": str(name) in keyed}
        return out

    def native_bulk(self, db, tables, go, log, shape_only=()):
        """Each table copied by BigQuery itself: a copy job from the
        source's table to the target's, which carries the schema, the
        partitions, the clusters, the key and the policies with it and
        reads no row into migkit, and costs no query. A copy job is
        atomic, so a table is empty or whole, never partly there; a table
        the target already holds is therefore known by its rows: the
        source's count - left as it is, for the check to prove - or any
        other, which stops the move rather than replace what is there.
        `shape_only` are tables the table copier fills: made like the
        source's, empty, where the target lacks them."""
        steps = [f"{t}: copied by BigQuery itself, a copy job from the"
                 " source's table - no row passes through migkit"
                 for t in tables]
        steps += [f"{t}: made like the source's, empty; the table copier"
                  " fills it" for t in shape_only]
        if not go or not (tables or shape_only):
            return steps
        from google.api_core import exceptions
        from google.cloud import bigquery
        say = log or (lambda m: None)
        src, dst = self._client("src"), self._client("dst")
        for t in shape_only:
            if not self._exists("dst", db, t):
                dst.query(
                    f"create table if not exists "
                    f"{self._qualified('dst', db, t)} like "
                    + self._q(f"{self._database('src')}."
                              f"{self._d('src', db)}.{t}")).result()
                say(f"{t}: made like the source's")
        jobs = []
        for t in tables:
            have = self._existing("dst", db, t)
            if have is not None and int(have.num_rows or 0):
                theirs = int(src.get_table(self._ref("src", db, t))
                             .num_rows or 0)
                if int(have.num_rows) != theirs:
                    raise SystemExit(
                        f"{t}: the target holds {have.num_rows} rows and "
                        f"the source {theirs}: migkit will not replace a "
                        "table that is already there")
                say(f"{t}: already on the target with the source's "
                    f"{theirs} rows; left as it is, the check proves it")
                continue
            try:
                jobs.append((t, dst.copy_table(
                    self._ref("src", db, t), self._ref("dst", db, t),
                    job_config=bigquery.CopyJobConfig(
                        write_disposition="WRITE_EMPTY"))))
            except exceptions.GoogleAPICallError as e:
                raise SystemExit(f"{t}: the copy job was refused: "
                                 f"{str(e).splitlines()[0][:160]}")
        for t, job in jobs:
            job.result()
            say(f"{t}: copied")
        return steps

    def _existing(self, side, db, table):
        """The table's metadata where it is there, else None."""
        from google.api_core import exceptions
        try:
            return self._client(side).get_table(self._ref(side, db, table))
        except exceptions.NotFound:
            return None

    def _exists(self, side, db, table):
        return self._existing(side, db, table) is not None

    # ---- the assessment -----------------------------------------------------------

    def _handwork(self):
        """What a copy of a dataset's rows leaves: counted and named
        (`migkit.handwork`). The tables are made and filled; a table's
        layout, its views, routines and policies, and the dataset's access
        are the operator's, said here before the move."""
        from .. import handwork
        inv = handwork.Inventory()
        for db in self.databases():
            try:
                listing = self._listing("src", db)
                keyless = [t for t, k in sorted(listing.items())
                           if k == BASE and self.in_scope(db, t)
                           and not self.neutral_key("src", db, t)]
                inv.add("no-row-key", db, "tables", keyless)
                layout = []
                for t, k in sorted(listing.items()):
                    if k == BASE and self.in_scope(db, t):
                        lay = self._layout(self._meta("src", db, t))
                        if lay["m"] or lay["cluster"]:
                            layout.append(
                                f"{t} ({lay['m'] or 'clustered'})")
                inv.add("not-carried", db, "partitioned or clustered tables"
                        " (a table is made plain)", layout)
                objs = self._objects("src", db)
                inv.add("server-side-code", db, "views and routines",
                        [f"{kind} {n}" for kind in ("view", "routine")
                         for n in objs[kind]])
                inv.add("not-carried", db, "materialized views, external"
                        " tables and snapshots",
                        [f"{kind} {n}" for kind in
                         ("materialized view", "external table", "snapshot")
                         for n in objs[kind]])
                pol = [t for t, k in sorted(listing.items())
                       if k == BASE and self.in_scope(db, t)
                       and self._row_policies("src", db, t)]
                inv.add("not-carried", db, "tables with row access"
                        " policies", pol)
            except Exception as e:  # noqa: BLE001 - unknown, never zero
                inv.unknown("not-carried", db,
                            f"the dataset could not be read: "
                            f"{(str(e).splitlines() or [''])[-1][:90]}")
        return inv

    def _version_row(self, sv, dv, parts=1):
        """BigQuery has no release to read: the row says so, rather than
        that nobody could tell."""
        return {"level": "info", "scope": "instance",
                "item": "server version match",
                "detail": "BigQuery is a service its provider runs and"
                          " changes continuously: there is no version of"
                          " the project's own to hold to the other side's"}

    def _source_items(self):
        """The inventory of what the move leaves (`_handwork`) as `assess`
        rows."""
        inv = self._handwork()
        return inv.rows() + inv.summary()

    def _assess_extra(self):
        return super()._assess_extra() + self._source_items()

    # ---- the catalogue a pair reads ------------------------------------------------

    def neutral_views(self, side, db):
        """[(name, the view's query)] of the views the hop takes: the text
        BigQuery keeps, with the dataset's own names left as they are."""
        return [(name, str(self._meta(side, db, name).view_query or ""))
                for name, kind in sorted(self._listing(side, db).items())
                if kind == VIEW and self.takes(db, "view", name)]

    def code_routines(self, side, db):
        """Every function and procedure of the dataset the hop takes, with
        the statement that makes it, as `storedcode.routines` has them."""
        from ..storedcode.routines import Param, Routine
        out = []
        for r in self._client(side).list_routines(self.side_database(side, db)):
            proc = str(r.type_) == "PROCEDURE"
            if not self.takes(db, "procedure" if proc else "function",
                              r.routine_id):
                continue
            full = self._client(side).get_routine(r.reference)
            ddl = self._routine_ddl(side, db, full)
            out.append(Routine(
                kind="procedure" if proc else "function",
                name=full.routine_id,
                params=[Param(a.name, str(getattr(
                    a.data_type, "type_kind", a.data_type)),
                    {"IN": "in", "OUT": "out", "INOUT": "inout"}.get(
                        str(a.mode or "IN").upper(), "in"))
                    for a in full.arguments or []],
                returns=None if proc else str(
                    getattr(full.return_type, "type_kind", full.return_type)
                    or ""),
                returns_set=str(full.type_) == "TABLE_VALUED_FUNCTION",
                body=full.body, definition=ddl,
                language=str(full.language or "SQL").lower(),
                settings={"dataset": self._d(side, db)}))
        return out

    def _routine_ddl(self, side, db, full):
        """The statement that makes a routine, from `INFORMATION_SCHEMA.
        ROUTINES.ddl`; its body alone where the view does not answer."""
        try:
            got = self._rows(
                side, db, "select ddl from"
                          f" {self._catalog(side, db, 'routines')} where"
                          f" routine_name = {self._lit(full.routine_id)}")
            if got and got[0][0]:
                return str(got[0][0])
        except Exception:  # noqa: BLE001 - the body alone, then
            pass
        return full.body

    def stream_writers(self, db):
        """[(what, pausable)] writing into this target dataset besides
        migkit: the accounts whose load, insert, update, delete and merge
        jobs touched it in the last hour, read from the project's job
        history, and migkit's own tail where it runs. A job BigQuery does
        not list (a streaming insert, a Storage Write stream) is not seen
        here, and the report says so."""
        out = super().stream_writers(db)
        opts = self._endpoint("dst").options or {}
        region = str(opts.get("location") or "US").lower()
        mine = self._whoami("dst")
        dataset = self._d("dst", db)
        for user, kind, n in self._rows(
                "dst", db,
                "select user_email, ifnull(statement_type, job_type),"
                " count(*) from"
                f" `region-{region}`.INFORMATION_SCHEMA.JOBS_BY_PROJECT where"
                " creation_time > timestamp_sub(current_timestamp(),"
                " interval 1 hour) and (destination_table.dataset_id ="
                f" {self._lit(dataset)} or exists (select 1 from"
                " unnest(referenced_tables) r where r.dataset_id ="
                f" {self._lit(dataset)})) and error_result is null and"
                " (job_type = 'LOAD' or statement_type in ('INSERT',"
                " 'UPDATE', 'DELETE', 'MERGE', 'TRUNCATE_TABLE')) group by"
                " 1, 2"):
            if user and user != mine:
                out.append((f"{user}: {n} {kind} jobs in the last hour",
                            False))
        return out

    def _whoami(self, side):
        """The account migkit signs in as: the client's service account
        where it has one, else what the project says it is signed in as."""
        creds = getattr(self._client(side), "_credentials", None)
        mine = getattr(creds, "service_account_email", None)
        if mine:
            return mine
        try:
            got = self._rows(side, "", "select session_user()")
            return str(got[0][0]) if got and got[0][0] else None
        except Exception:  # noqa: BLE001 - unknown: every account is named
            return None
