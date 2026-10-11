"""The deep checks a SQL engine's own catalogue answers, one way for the
engines whose catalogue migkit reads with its own SQL: Db2 on each of its
platforms, SAP ASE, Teradata, Snowflake, Redshift and BigQuery (backlog
P0-PARITY.verify).

PostgreSQL and MySQL grew their deep checks inline, one statement at a
time. These engines were left with a pending-state check and a sample of
the source's text: a hop moving Db2 into Db2 was held to nothing about its
foreign keys, triggers, grants, sequences or row permissions. What a check
asks is the same on all of them - a foreign key the source has and the
target lacks, a trigger left disabled, a grant that did not arrive, a
counter behind its rows - and only the catalogue differs. So each engine
answers a fixed set of questions in its own SQL (`CATALOGUE`, below), and
the comparing, the scoping (`Engine.in_scope`, `Engine.takes`) and the
words are here, once.

An engine runs the checks its catalogue answers: its `check_deep` names
each one it runs, so a check whose question its catalogue has no answer to
is not run - never one that reads as clean. A question's SQL is the
engine's: `{schema}` in it is the hop's database on that side, as a
literal in the engine's own quoting, and `{catalog}` the prefix of its
catalogue's views where they are named per schema (BigQuery's dataset).

Every catalogue here was written from its vendor's documentation and is
held by stand-in tests of the SQL each engine asks and the rows it reads
back: no Db2, ASE, Teradata or warehouse runs on this machine.
"""
from .base import Result

#: the questions an engine's `CATALOGUE` answers, and the row each answer
#: is made of - the first value of a row about a table is the table, as
#: `neutral_tables` names it, which is what scopes it
QUESTIONS = {
    "keyless": "table - a table with no primary key and no unique one",
    "fks": "table, name, column, parent table, parent column, enforced"
           " (Y/N) - a row a column pair, in key order",
    "triggers": "table, trigger, enabled (Y/N)",
    "indexes": "table, index, unique (Y/N), column, valid (Y/N, or '' where"
               " the catalogue keeps no index state) - a row a column",
    "checks": "table, constraint, kind, state - '' where the constraint is"
              " enforced and every row was held to it, else the server's"
              " word for why not",
    "partitions": "table, partition, bound, rows (or what counts them)",
    "matviews": "view, state ('' populated and current), rows or ''",
    "rls": "table, policy, enabled (Y/N) - a row permission, a row access"
           " policy or a column mask",
    "generated": "table, column, how it is generated",
    "collation": "table, column, collation - table and column '' for the"
                 " database's own",
    "charset": "table, column, character set - table and column '' for the"
               " database's own",
    "owners": "kind, object, owner",
    "grants": "table, grantee, privilege",
    "seq_grants": "sequence, grantee, privilege",
    "sequences": "sequence, the value it hands out next",
    "identity": "table, column, the value it hands out next",
    "objects": "kind, name, table ('' where it is on none)",
    "options": "table, option, value",
    "spread": "table, how its rows are spread across the system",
    "ids": "table, the system's own identifier of it",
}


def _plain(v):
    """A catalogue value as it is compared: text without the padding a
    fixed-length catalogue column carries, a number as it is."""
    if v is None:
        return ""
    if isinstance(v, bytes):
        return v.decode("utf-8", "replace").strip()
    if isinstance(v, str):
        return v.strip()
    return v


def _yes(v):
    return str(v).strip().upper() in ("Y", "YES", "TRUE", "T", "1",
                                      "ENABLED", "ACTIVE")


class Behind(tuple):
    """(counter, table, column, the column's largest value) - an identity
    column behind its rows, as `sequences_behind` says it everywhere -
    knowing the database it is in (`db`), which the statement that moves
    it on names."""

    def __new__(cls, db, *items):
        got = super().__new__(cls, items)
        got.db = db
        return got


def _listed(items, most=5):
    items = list(items)
    return ", ".join(str(i) for i in items[:most]) + (
        f" and {len(items) - most} more" if len(items) > most else "")


class CatalogChecks:
    """The deep checks of an engine whose catalogue answers `QUESTIONS`
    in its own SQL (`CATALOGUE`)."""

    #: {question: SQL} - this engine's catalogue's answer to each of
    #: `QUESTIONS` it has one for
    CATALOGUE = {}
    #: declared types (their base, small letters) whose values the server
    #: renders in a way of its own - a period, an interval, a document -
    #: and whose text is compared as each side writes it (`_rendering`)
    RENDERED = ()
    #: rows of a table a value check reads at most, each side
    DEEP_ROWS = 500
    #: text columns the NULL-and-empty count reads at most
    DEEP_TEXT_COLUMNS = 30

    # ---- asking the catalogue ---------------------------------------------

    def _schema_of(self, side, db):
        """The hop's database on `side` as this engine's catalogue names
        it: a schema folded to capitals where names fold (`_owner`)."""
        owner = getattr(self, "_owner", None)
        return owner(side, db) if owner is not None else self._d(side, db)

    def _schema_literal(self, side, db):
        lit = getattr(self, "_lit", None)
        name = str(self._schema_of(side, db))
        return lit(name) if lit is not None else \
            "'" + name.replace("'", "''") + "'"

    def _catalog_prefix(self, side, db):
        """Where this engine's catalogue views are, for a question that
        says `{catalog}`: `information_schema` unless the engine keeps
        them per schema (`_catalog`)."""
        named = getattr(self, "_catalog", None)
        if named is None:
            return "information_schema"
        try:
            return named(side, db, "x")[:-2]
        except TypeError:
            return "information_schema"

    def _question(self, side, db, question):
        sql = self.CATALOGUE[question]
        return (sql.replace("{schema}", self._schema_literal(side, db))
                .replace("{catalog}", self._catalog_prefix(side, db)))

    def _named(self, name):
        """A name a catalogue gives, as this engine's listings give it
        (`_said` where names fold to capitals)."""
        said = getattr(self, "_said", None)
        got = _plain(name)
        return said(got) if said is not None and isinstance(got, str) \
            and got else got

    def _asked(self, side, db, question, named=(0,)):
        """The rows of this engine's answer to `question` on `side`, each
        value plain, the values at `named` as its listings name them.
        KeyError where the engine has no answer to it."""
        sql = self._question(side, db, question)
        rows = self._rows(side, db, sql)
        out = []
        for r in rows:
            r = [_plain(v) for v in r]
            for i in named:
                if i < len(r):
                    r[i] = self._named(r[i])
            out.append(r)
        return out

    def _scoped_asked(self, side, db, question, named=(0,)):
        """`_asked`, only the rows of the tables the hop takes; a row
        about the database itself (table '') is kept."""
        rows = self._asked(side, db, question, named)
        return [r for r in rows if r[0] == "" or self.in_scope(db, r[0])]

    def _taken_asked(self, side, db, question, kind_at=0, name_at=1,
                     table_at=None, kind=None):
        """`_asked` of objects beside the tables, only the ones the hop
        carries (`Engine.takes`): by the table each is on where it is on
        one, else by its own name."""
        named = tuple(i for i in (name_at, table_at) if i is not None)
        out = []
        for r in self._asked(side, db, question, named):
            what = kind or str(r[kind_at]).lower()
            on = r[table_at] if table_at is not None and r[table_at] \
                else None
            if what == "table":
                if self.in_scope(db, r[name_at]):
                    out.append(r)
            elif self.takes(db, what, r[name_at], on):
                out.append(r)
        return out

    def _shown(self, side, db, sql):
        """The rows of a statement the server answers with named columns
        (`show ...`), each a mapping of its column's name in small
        letters to its value."""
        conn = self._connect(side, db)
        try:
            cur = conn.cursor()
            self._run(cur, sql)
            names = [str(c[0]).lower() for c in cur.description or ()]
            return [dict(zip(names, (_plain(v) for v in r)))
                    for r in cur.fetchall()]
        finally:
            conn.close()

    @staticmethod
    def _unread(db, word, e):
        """The finding of a check whose catalogue could not be read: an
        error, never a clean answer. `word` ends the scope, so the error
        is the category's."""
        return Result("deep", f"{db} {word}", "error",
                      f"could not read the catalogue:"
                      f" {str(e).splitlines()[0][:100] if str(e) else type(e).__name__}")

    # ---- structure -----------------------------------------------------------

    def _keyless(self, db):
        """Tables with no key the move can verify or repair them by: a
        change to one is applied, and a row compared, by every column."""
        try:
            rows = self._scoped_asked("src", db, "keyless")
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "keys", e)
        names = sorted({r[0] for r in rows})
        if names:
            return Result("deep", f"{db} keys", "diff",
                          f"{len(names)} tables have no primary or unique"
                          f" key: {_listed(names)} - a row of one cannot"
                          " be told from its duplicate, so it is compared"
                          " and repaired by every column", "",
                          "add a primary key, or a unique one, before"
                          " migrating")
        return Result("deep", f"{db} keys", "ok",
                      "every table the hop takes has a primary or unique"
                      " key")

    def _fk_sets(self, side, db):
        """{(child, parent, (columns), (parent's columns)): (name,
        enforced)} of one side's foreign keys on the tables the hop
        takes."""
        got = {}
        for table, name, col, parent, pcol, enforced in self._scoped_asked(
                side, db, "fks", (0, 3)):
            one = got.setdefault((table, name), [parent, [], [], enforced])
            one[1].append(self._named(col))
            one[2].append(self._named(pcol))
        return {(t, p, tuple(c), tuple(pc)): (n, _yes(e))
                for (t, n), (p, c, pc, e) in got.items()}

    def _foreign_keys_found(self, db):
        """The source's foreign keys on the target - by what they join,
        whatever each side named them - enforced there where the source
        enforces them, and the target's rows held to each: a child row
        whose parent is not there is an orphan, which a foreign key the
        server does not enforce (a warehouse's, Db2's NOT ENFORCED, one
        a load left unchecked) lets in silently."""
        try:
            src, dst = self._fk_sets("src", db), self._fk_sets("dst", db)
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "fk", e)
        bad = []
        for k, (name, enforced) in sorted(src.items()):
            if k not in dst:
                bad.append(f"{k[0]}.{name} -> {k[1]}: missing on target")
            elif enforced and not dst[k][1]:
                bad.append(f"{k[0]}.{dst[k][0]}: not enforced on target")
        orphans = []
        for (child, parent, cols, pcols), (name, _) in sorted(dst.items()):
            try:
                n = self._orphans("dst", db, child, cols, parent, pcols)
            except Exception:  # noqa: BLE001 - counted where it can be
                continue
            if n:
                orphans.append(f"{child}.{name}: {n} rows whose {parent}"
                               " row is not there")
        if bad or orphans:
            return Result("deep", f"{db} fk", "diff",
                          "; ".join((bad + orphans)[:6])
                          + (f"; and {len(bad) + len(orphans) - 6} more"
                             if len(bad) + len(orphans) > 6 else ""), "",
                          "create the missing foreign keys on the target,"
                          " and remove or re-copy the orphaned rows before"
                          " cutover")
        return Result("deep", f"{db} fk", "ok",
                      f"{len(src)} foreign keys on the target as on the"
                      " source, and no orphaned row" if src
                      else "no foreign keys on the tables the hop takes")

    def _orphans(self, side, db, child, cols, parent, pcols):
        """How many of `child`'s rows name a `parent` row that is not
        there, counted by the server."""
        c = self._qualified(side, db, child)
        p = self._qualified(side, db, parent)
        on = " and ".join(f"p.{self._q(b)} = c.{self._q(a)}"
                          for a, b in zip(cols, pcols))
        held = " and ".join(f"c.{self._q(a)} is not null" for a in cols)
        got = self._rows(side, db, f"select count(*) from {c} c where"
                                   f" {held} and not exists (select 1 from"
                                   f" {p} p where {on})")
        return int(got[0][0] or 0) if got else 0

    def _triggers_found(self, db):
        """The source's triggers on the target, and enabled there: a
        trigger left disabled after a load does not fire for the
        application either."""
        try:
            src = {(t, n): _yes(e) for t, n, e in self._triggers("src", db)}
            dst = {(t, n): _yes(e) for t, n, e in self._triggers("dst", db)}
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "triggers", e)
        missing = [f"{t}.{n}" for t, n in sorted(src) if (t, n) not in dst]
        if missing:
            return Result("deep", f"{db} triggers", "diff",
                          f"{len(missing)} triggers missing on target:"
                          f" {_listed(missing)}", "",
                          "create them on the target before cutover")
        disabled = [f"{t}.{n}" for (t, n), on in sorted(dst.items())
                    if not on and src.get((t, n), True)]
        if disabled:
            return Result("deep", f"{db} triggers", "diff",
                          f"disabled on target: {_listed(disabled)}", "",
                          "enable them on the target before cutover")
        live = [f"{t}.{n}" for (t, n), on in sorted(dst.items()) if on]
        return Result("deep", f"{db} triggers", "ok",
                      f"{len(src)} triggers on the target as on the source"
                      + (f"; {len(live)} enabled on tables the copy writes"
                         f" ({_listed(live, 4)}): they fire for every row it"
                         " writes - a trigger that stamps or audits rows"
                         " records the move" if live else "")
                      if src or live else "no triggers on the tables the"
                                          " hop takes")

    def _triggers(self, side, db):
        """[(table, trigger, enabled)] of the tables the hop takes, the
        triggers it carries (`takes`)."""
        return [r for r in self._scoped_asked(side, db, "triggers", (0, 1))
                if self.takes(db, "trigger", r[1], r[0])]

    def _index_sets(self, side, db):
        """{(table, unique, (columns)): (name, valid)} of one side."""
        got = {}
        for table, name, unique, col, valid in self._scoped_asked(
                side, db, "indexes", (0,)):
            one = got.setdefault((table, name), [_yes(unique), [], valid])
            one[1].append(self._named(col))
        return {(t, u, tuple(c)): (n, v)
                for (t, n), (u, c, v) in got.items()}

    def _indexes_found(self, db):
        """The source's indexes on the target - by table, columns and
        uniqueness, whatever each side named them - and usable there: an
        index a load left needing a rebuild is one the server will not
        use, and a unique one missing takes duplicates."""
        try:
            src = self._index_sets("src", db)
            dst = self._index_sets("dst", db)
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "indexes", e)
        missing = [f"{'unique ' if k[1] else ''}{k[0]}.{n}"
                   f" ({', '.join(k[2])})"
                   for k, (n, _) in sorted(src.items()) if k not in dst]
        broken = [f"{k[0]}.{n}" for k, (n, v) in sorted(dst.items())
                  if v != "" and not _yes(v)]
        if missing or broken:
            return Result("deep", f"{db} indexes", "diff",
                          "; ".join(
                              ([f"{len(missing)} missing on target:"
                                f" {_listed(missing, 4)}"] if missing
                               else [])
                              + ([f"{len(broken)} not usable on target"
                                  f" (left needing a rebuild):"
                                  f" {_listed(broken, 4)}"] if broken
                                 else [])), "",
                          "create the missing indexes on the target and"
                          " rebuild the ones left unusable")
        unread = all(v == "" for _, v in dst.values())
        return Result("deep", f"{db} indexes", "ok",
                      f"{len(src)} indexes on the target as on the source"
                      + (" (the catalogue keeps no index state to read)"
                         if dst and unread else ""))

    def _constraints_found(self, db):
        """Constraints on the target that are not enforced, or whose rows
        were never held to them, where the source's are: a load that set
        them aside - Db2's check pending, a reference a restore left
        inconsistent, one declared NOT ENFORCED on the way - leaves rows
        nothing checked."""
        try:
            src = {(r[0], r[1]): r[3]
                   for r in self._scoped_asked("src", db, "checks", (0, 1))}
            dst = {(r[0], r[1]): (r[2], r[3])
                   for r in self._scoped_asked("dst", db, "checks", (0, 1))}
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "checks", e)
        bad = [f"{t}.{n} ({kind}): {state}"
               for (t, n), (kind, state) in sorted(dst.items())
               if state and not src.get((t, n))]
        if bad:
            return Result("deep", f"{db} checks", "diff",
                          f"{len(bad)} constraints on the target not held"
                          f" to their rows: {_listed(bad, 4)}", "",
                          "enforce them and check every row on the target"
                          " (Db2: set integrity ... immediate checked)"
                          " before cutover")
        return Result("deep", f"{db} checks", "ok",
                      f"{len(dst)} constraints on the target enforced and"
                      " held to their rows" if dst
                      else "no constraints on the tables the hop takes")

    def _partitions_found(self, db):
        """Each partitioned table's partitions on both sides, with the rows
        each holds (`_partitions_verdict`): a partition can arrive empty
        while the table's count looks plausible, its rows in another."""
        try:
            sides = {side: self._partitions(side, db)
                     for side in ("src", "dst")}
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "partitions", e)
        tables, scheme = {}, []
        for t in sorted(set(sides["src"]) | set(sides["dst"])):
            s, d = sides["src"].get(t), sides["dst"].get(t)
            if s is None or d is None:
                scheme.append(f"{t}: partitioned on the"
                              f" {'source' if d is None else 'target'}"
                              " only")
                continue
            tables[t] = (s, d)
        return self._partitions_verdict(db, tables, scheme,
                                        self.CATCHALL_PARTITION)

    #: what this engine calls the partition rows land in when no other
    #: takes them
    CATCHALL_PARTITION = "last"

    def _partitions(self, side, db):
        """{table: {partition: {"rows", "bound"}}} of one side."""
        out = {}
        for table, name, bound, rows in self._scoped_asked(
                side, db, "partitions"):
            out.setdefault(table, {})[str(name)] = {
                "rows": self._partition_rows(side, db, table, name, rows),
                "bound": str(bound or "")}
        return out

    def _partition_rows(self, side, db, table, partition, said):
        """The rows one partition holds: the catalogue's own count, where
        it keeps one exactly."""
        try:
            return int(said)
        except (TypeError, ValueError):
            return None

    def _matviews_found(self, db):
        """The source's materialized views on the target, populated, and
        holding as many rows as the source's: one created and never
        refreshed answers every query with nothing."""
        try:
            src = {r[0]: r for r in self._taken_asked(
                "src", db, "matviews", name_at=0, kind="view")}
            dst = {r[0]: r for r in self._taken_asked(
                "dst", db, "matviews", name_at=0, kind="view")}
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "matviews", e)
        stale = []
        for name, row in sorted(src.items()):
            there = dst.get(name)
            if there is None:
                stale.append(f"{name}: missing on target")
            elif there[1]:
                stale.append(f"{name}: {there[1]} on target")
            elif len(row) > 2 and len(there) > 2 and row[2] != "" \
                    and there[2] != "" and str(row[2]) != str(there[2]):
                stale.append(f"{name}: {there[2]} rows on target,"
                             f" {row[2]} on source")
        if stale:
            return Result("deep", f"{db} matviews", "diff",
                          "; ".join(stale[:5]), "",
                          "create and refresh them on the target before"
                          " cutover")
        return Result("deep", f"{db} matviews", "ok",
                      f"{len(src)} materialized views on the target,"
                      " populated" if src else "no materialized views")

    def _row_security_found(self, db):
        """The source's row permissions, row access policies and column
        masks on the target, and in force there; and a word where one is
        in force on the source: it applies to migkit's own reads too, so
        the rows and values it copied and compared are what the policy let
        it see."""
        try:
            src = {(t, p): _yes(e)
                   for t, p, e in self._scoped_asked("src", db, "rls")}
            dst = {(t, p): _yes(e)
                   for t, p, e in self._scoped_asked("dst", db, "rls")}
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "rls", e)
        msgs = [f"{t}: {p} missing on target" for (t, p), on in
                sorted(src.items()) if on and (t, p) not in dst]
        msgs += [f"{t}: {p} not in force on target" for (t, p), on in
                 sorted(src.items()) if on and (t, p) in dst
                 and not dst[(t, p)]]
        if msgs:
            return Result("deep", f"{db} rls", "diff", "; ".join(msgs[:5]),
                          "", "create and enable them on the target: until"
                              " then every row and value is open to every"
                              " reader there")
        live = sorted({t for (t, _), on in src.items() if on})
        if live:
            return Result("deep", f"{db} rls", "warn",
                          f"{len(live)} tables have a row or column policy"
                          f" in force on the source ({_listed(live, 4)}):"
                          " migkit's account read them through it, so what"
                          " it copied and compared is what the policy let"
                          " it see", "",
                          "move and check them as an account the policies"
                          " do not filter, or exempt migkit's account")
        return Result("deep", f"{db} rls", "ok",
                      f"{len(src)} row or column policies, the same on both"
                      " sides" if src else "no row or column policies")

    def _generated_found(self, db):
        """Columns the source's server generates, generated the same way
        on the target: one plain there takes whatever a load wrote into
        it, and the application's next write leaves it stale."""
        import re
        try:
            src = {(t, c): g for t, c, g in self._scoped_asked(
                "src", db, "generated", (0, 1))}
            dst = {(t, c): g for t, c, g in self._scoped_asked(
                "dst", db, "generated", (0, 1))}
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "generated", e)

        def norm(text):
            return re.sub(r"\s+", "", str(text)).lower()
        bad = []
        for k, how in sorted(src.items()):
            if k not in dst:
                bad.append(f"{k[0]}.{k[1]}: generated on source, plain or"
                           " missing on target")
            elif norm(how) != norm(dst[k]):
                bad.append(f"{k[0]}.{k[1]}: generated differently on target")
        bad += [f"{k[0]}.{k[1]}: generated on target, plain on source"
                for k in sorted(dst) if k not in src]
        if bad:
            return Result("deep", f"{db} generated", "diff",
                          "; ".join(bad[:6]), "",
                          "make the column generated as on the source, and"
                          " derive it again on the target")
        return Result("deep", f"{db} generated", "ok",
                      f"{len(src)} generated columns match" if src
                      else "no generated columns")

    def _columns_found(self, db):
        """Each table's columns on both sides - names, declared types,
        whether they take NULL and their defaults - for a hop moving an
        engine into itself, where a column that changed on the way is a
        difference, not a conversion."""
        try:
            src = set(self.neutral_tables("src", db))
            dst = set(self.neutral_tables("dst", db))
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "columns", e)
        diffs, compared = [], 0
        for t in sorted(src & dst):
            if not self.in_scope(db, t):
                continue
            try:
                a = self._column_facts("src", db, t)
                b = self._column_facts("dst", db, t)
            except Exception:  # noqa: BLE001 - one table's, said below
                diffs.append(f"{t}: columns not readable")
                continue
            compared += 1
            for n in a:
                if n not in b:
                    diffs.append(f"{t}.{n}: missing on target")
                elif a[n] != b[n]:
                    diffs.append(f"{t}.{n}: {a[n][0]} -> {b[n][0]}"
                                 if a[n][0] != b[n][0] else
                                 f"{t}.{n}: nullable or default differs")
            diffs += [f"{t}.{n}: only on target" for n in b if n not in a]
        if diffs:
            return Result("deep", f"{db} columns", "diff",
                          f"{len(diffs)} column differences:"
                          f" {_listed(diffs, 6)}", "",
                          "align the target's columns with the source's"
                          " before cutover")
        return Result("deep", f"{db} columns", "ok",
                      f"{compared} tables' columns identical on both sides")

    def _column_facts(self, side, db, table):
        """{column: (declared, takes NULL, default)}."""
        rules = self.neutral_column_rules(side, db, table) or {}
        return {n: (str(t).upper(), (rules.get(n) or {}).get("null"),
                    (rules.get(n) or {}).get("default"))
                for n, t in self.neutral_columns(side, db, table)}

    def _structure_found(self, db):
        """The whole schema of the tables the hop takes, compared: columns,
        keys, indexes and foreign keys, in one catalogue each side
        (`catalog.Catalog`, `shape_catalog`) - what differs said table by
        table, written to `structural-diff.txt`."""
        try:
            a = self.shape_catalog("src", db)
            b = self.shape_catalog("dst", db)
        except Exception as e:  # noqa: BLE001 - said
            return Result("schema", f"{db} (structural)", "error",
                          "could not read both sides' catalogues:"
                          f" {str(e).splitlines()[0][:100]}")
        lines = []
        for t in sorted(set(a.tables) | set(b.tables)):
            for what in a.explain(b, t, self.CANON_ENGINE,
                                  added="only on target",
                                  dropped="missing on target"):
                lines.append(f"{t}: {what}")
        path = self.hop.report_dir(db) / "structural-diff.txt"
        if not lines:
            path.unlink(missing_ok=True)
            return Result("schema", f"{db} (structural)", "ok",
                          f"{len(a.tables)} tables identical in structure,"
                          " both sides")
        path.write_text("\n".join(lines) + "\n")
        return Result("schema", f"{db} (structural)", "diff",
                      f"{len(lines)} structural differences:"
                      f" {_listed(lines, 4)}", str(path),
                      "read structural-diff.txt and bring the target's"
                      " schema to the source's before cutover")

    def shape_catalog(self, side, db):
        """The catalogue of the tables the hop takes on `side`, in the one
        shape drift and the structural check read (`catalog.Catalog`):
        columns with their rules, the key, indexes and foreign keys."""
        from ..catalog import Catalog, Column, ForeignKey, Index
        cat = Catalog()
        tables = [t for t in self.neutral_tables(side, db)
                  if self.in_scope(db, t)]
        for t in tables:
            rules = self.neutral_column_rules(side, db, t) or {}
            cat.columns[t] = [
                Column(str(n), str(d), "",
                       (rules.get(n) or {}).get("null", True) is not False,
                       (rules.get(n) or {}).get("default"), None,
                       "identity" if (rules.get(n) or {}).get("identity")
                       else "")
                for n, d in self.neutral_columns(side, db, t)]
            cat.keys[t] = tuple(self.neutral_key(side, db, t))
            cat.tables.append(t)
        if "indexes" in self.CATALOGUE:
            for (t, u, cols), (n, _) in self._index_sets(side, db).items():
                cat.indexes.setdefault(t, []).append(Index("", cols, u))
        if "fks" in self.CATALOGUE:
            for (t, p, cols, pcols), _ in self._fk_sets(side, db).items():
                cat.fks.setdefault(t, []).append(
                    ForeignKey("", cols, p, pcols))
        return cat

    def _objects_found(self, db):
        """Everything beside the tables the hop carries - views, routines,
        triggers, sequences, aliases - on the target as on the source, by
        kind and name (`takes`: a kind the hop's `objects` leaves out is
        not compared)."""
        try:
            src = {(r[0].lower(), r[1]) for r in self._taken_asked(
                "src", db, "objects", 0, 1, 2)}
            dst = {(r[0].lower(), r[1]) for r in self._taken_asked(
                "dst", db, "objects", 0, 1, 2)}
        except Exception as e:  # noqa: BLE001 - said
            return Result("schema", f"{db} objects", "error",
                          "could not read the catalogue:"
                          f" {str(e).splitlines()[0][:100]}")
        missing = sorted(src - dst)
        if missing:
            kinds = {}
            for k, n in missing:
                kinds.setdefault(k, []).append(n)
            return Result("schema", f"{db} objects", "diff",
                          "; ".join(f"{len(v)} {k}s missing on target:"
                                    f" {_listed(v, 4)}"
                                    for k, v in sorted(kinds.items())), "",
                          "create them on the target - converted code is"
                          " proved by migkit check --deep on a pair")
        return Result("schema", f"{db} objects", "ok",
                      f"{len(src)} objects beside the tables on both sides"
                      if src else "no objects beside the tables")

    def _table_options_found(self, db):
        """What each table is beyond its columns, the same on the target:
        the options that change what it keeps - a Teradata SET table
        drops a duplicate row a MULTISET one keeps, a Snowflake TRANSIENT
        table has no fail-safe, a BigQuery table set to expire is deleted,
        a Db2 table loaded NOT LOGGED INITIALLY cannot be recovered."""
        try:
            src = {(t, o): v for t, o, v in self._scoped_asked(
                "src", db, "options")}
            dst = {(t, o): v for t, o, v in self._scoped_asked(
                "dst", db, "options")}
        except Exception as e:  # noqa: BLE001 - said
            return Result("deep", f"{db} table options", "error",
                          "could not read the catalogue:"
                          f" {str(e).splitlines()[0][:100]}",
                          category="structure.collection-options")
        bad = [f"{t} {o}: {src[(t, o)] or 'unset'} -> {dst[(t, o)] or 'unset'}"
               for t, o in sorted(src) if (t, o) in dst
               and str(src[(t, o)]) != str(dst[(t, o)])]
        bad += [f"{t} {o}: {dst[(t, o)]} on target only"
                for t, o in sorted(dst) if (t, o) not in src
                and dst[(t, o)] not in ("", None)]
        if bad:
            return Result("deep", f"{db} table options", "diff",
                          f"{len(bad)} table options differ:"
                          f" {_listed(bad, 5)}", "",
                          "set each table's options on the target as the"
                          " source has them", category=(
                              "structure.collection-options"))
        return Result("deep", f"{db} table options", "ok",
                      f"{len({t for t, _ in src})} tables' options the same"
                      " on both sides",
                      category="structure.collection-options")

    def _spread_found(self, db):
        """How each table's rows are spread across the system - Redshift's
        distribution, Teradata's primary index, Db2's distribution key -
        the same on the target: another spread is another skew, and a key
        joined on that no longer lands on one node."""
        try:
            src = {t: h for t, h in self._scoped_asked("src", db, "spread")}
            dst = {t: h for t, h in self._scoped_asked("dst", db, "spread")}
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "sharding", e)
        bad = [f"{t}: {src.get(t) or 'none'} -> {dst.get(t) or 'none'}"
               for t in sorted(set(src) | set(dst))
               if t in src and t in dst and str(src[t]) != str(dst[t])]
        if bad:
            return Result("deep", f"{db} sharding", "diff",
                          f"{len(bad)} tables spread otherwise on the"
                          f" target: {_listed(bad, 5)}", "",
                          "make them with the source's distribution before"
                          " loading")
        return Result("deep", f"{db} sharding", "ok",
                      f"{len(src)} tables spread the same way on both"
                      " sides")

    # ---- values ------------------------------------------------------------------

    def _collations_found(self, db):
        """The collation of the database and of each text column, the same
        on the target: another collation sorts and compares otherwise, and
        a key that was unique can collapse into duplicates on the way in
        (case or accents folded)."""
        try:
            src = {(t, c): v for t, c, v in self._scoped_asked(
                "src", db, "collation", (0, 1))}
            dst = {(t, c): v for t, c, v in self._scoped_asked(
                "dst", db, "collation", (0, 1))}
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "collation", e)
        bad = [(f"{t}.{c}" if t else "the database") + f": {v} -> {dst[(t, c)]}"
               for (t, c), v in sorted(src.items())
               if (t, c) in dst and str(v) != str(dst[(t, c)])]
        if bad:
            return Result("deep", f"{db} collation", "diff",
                          f"{len(bad)} collations differ: {_listed(bad, 5)}",
                          "", "make the target's collation the source's,"
                              " or prove each unique text key keeps its"
                              " values apart under it, before loading")
        return Result("deep", f"{db} collation", "ok",
                      "collations the same on both sides"
                      + (f" ({len(src)} read)" if src else ""))

    def _charsets_found(self, db):
        """The character set of the database and of each text column, the
        same on the target, and no text the move replaced on the way: a
        load that met bytes it could not read wrote U+FFFD in their place,
        and the target holds more of them than the source."""
        bad = []
        if "charset" in self.CATALOGUE:
            try:
                src = {(t, c): v for t, c, v in self._scoped_asked(
                    "src", db, "charset", (0, 1))}
                dst = {(t, c): v for t, c, v in self._scoped_asked(
                    "dst", db, "charset", (0, 1))}
            except Exception as e:  # noqa: BLE001 - said
                return self._unread(db, "charset", e)
            bad += [(f"{t}.{c}" if t else "the database")
                    + f": {v} -> {dst[(t, c)]}"
                    for (t, c), v in sorted(src.items())
                    if (t, c) in dst and str(v) != str(dst[(t, c)])]
        replaced = []
        for t, col in self._text_columns(db):
            try:
                a = self._count_where("src", db, t, self._has_replacement(
                    self._q(col)))
                b = self._count_where("dst", db, t, self._has_replacement(
                    self._q(col)))
            except Exception:  # noqa: BLE001 - counted where it can be
                continue
            if b > a:
                replaced.append(f"{t}.{col}: {b - a} more values holding"
                                " U+FFFD on target")
        bad += replaced
        if bad:
            return Result("deep", f"{db} charset", "diff",
                          "; ".join(bad[:6]), "",
                          "load the affected rows again through a"
                          " connection that keeps every character")
        return Result("deep", f"{db} charset", "ok",
                      "character sets the same, and no value replaced on"
                      " the way")

    def _has_replacement(self, quoted):
        """The condition a text value holding U+FFFD meets, as this
        engine's SQL writes the character."""
        return f"{quoted} like '%�%'"

    def _count_where(self, side, db, table, where):
        got = self._rows(side, db, "select count(*) from"
                                   f" {self._qualified(side, db, table)}"
                                   f" where {where}")
        return int(got[0][0] or 0) if got else 0

    def _text_columns(self, db, most=None):
        """[(table, column)] of the source's text columns on tables both
        sides hold and the hop takes, at most `DEEP_TEXT_COLUMNS`."""
        from .. import canon
        out = []
        both = set(self.neutral_tables("dst", db))
        for t in self.neutral_tables("src", db):
            if t not in both or not self.in_scope(db, t):
                continue
            for n, typ in self.neutral_columns("src", db, t):
                if canon.type_class(self.CANON_ENGINE, typ) == "text":
                    out.append((t, n))
            if len(out) >= (most or self.DEEP_TEXT_COLUMNS):
                break
        return out[:most or self.DEEP_TEXT_COLUMNS]

    def _null_empty_found(self, db):
        """Each text column's NULLs and empty strings counted on both
        sides: a value that is NULL on one side and '' on the other is
        present in both, so a row comparison that renders both alike says
        nothing - and a column the application tests with IS NULL gives
        other answers."""
        neq = []
        for t, col in self._text_columns(db):
            c = self._q(col)
            sql = (f"select sum(case when {c} is null then 1 else 0 end),"
                   f" sum(case when {c} = '' then 1 else 0 end) from"
                   " {table}")
            try:
                a = self._rows("src", db, sql.replace(
                    "{table}", self._qualified("src", db, t)))
                b = self._rows("dst", db, sql.replace(
                    "{table}", self._qualified("dst", db, t)))
            except Exception:  # noqa: BLE001 - counted where it can be
                continue
            sa = tuple(int(v or 0) for v in a[0]) if a else (0, 0)
            sb = tuple(int(v or 0) for v in b[0]) if b else (0, 0)
            if sa != sb:
                neq.append(f"{t}.{col}: null/empty src={sa[0]}/{sa[1]}"
                           f" dst={sb[0]}/{sb[1]}")
        if neq:
            return Result("deep", f"{db} nullempty", "diff",
                          f"{len(neq)} text columns split NULL and empty"
                          f" otherwise: {_listed(neq, 5)}", "",
                          "decide which side is right for each column and"
                          " carry NULL and '' as the source has them")
        return Result("deep", f"{db} nullempty", "ok",
                      "NULL and empty text split alike on both sides")

    def _keyed_columns(self, db, classes):
        """[(table, key column, column, declared)] of the tables both sides
        hold and the hop takes, keyed by one column, for each column of
        one of `classes` (`canon.type_class`)."""
        from .. import canon
        out = []
        both = set(self.neutral_tables("dst", db))
        for t in self.neutral_tables("src", db):
            if t not in both or not self.in_scope(db, t):
                continue
            key = list(self.neutral_key("src", db, t))
            if len(key) != 1:
                continue
            for n, typ in self.neutral_columns("src", db, t):
                if n != key[0] and canon.type_class(
                        self.CANON_ENGINE, typ) in classes:
                    out.append((t, key[0], n, typ))
        return out

    def _by_key(self, side, db, table, key, col, expr=None):
        """{key: value} of the first `DEEP_ROWS` rows holding a value in
        `col`, by key."""
        k, c = self._q(key), expr or self._q(col)
        sql = self._select(f"{k}, {c}", self._qualified(side, db, table),
                           f" where {self._q(col)} is not null",
                           f" order by {k}", self.DEEP_ROWS)
        return {str(a): b for a, b in self._rows(side, db, sql)}

    def _floats_found(self, db):
        """Each float column's values on both sides, held to a relative
        tolerance (`float_tolerance`): a float carried through text, or
        into a narrower float, comes back another number."""
        tol = float(self.hop.options.get("float_tolerance", 1e-9))
        bad, seen = [], 0
        for t, key, col, _ in self._keyed_columns(db, ("float",))[:10]:
            try:
                a = self._by_key("src", db, t, key, col)
                b = self._by_key("dst", db, t, key, col)
            except Exception:  # noqa: BLE001 - read where it can be
                continue
            seen += 1
            off, worst = 0, 0.0
            for k, v in a.items():
                if k not in b or v is None or b[k] is None:
                    continue
                x, y = float(v), float(b[k])
                d = abs(x - y)
                if d > tol * max(abs(x), abs(y), 1.0):
                    off += 1
                    worst = max(worst, d)
            if off:
                bad.append(f"{t}.{col}: {off} values off by up to {worst:g}")
        if bad:
            return Result("deep", f"{db} float", "diff", "; ".join(bad[:5]),
                          "", "carry floats as their binary value, not as"
                              " text, into a float as wide as the"
                              " source's - or set float_tolerance to"
                              " accept it")
        return Result("deep", f"{db} float", "ok",
                      f"{seen} float columns within tolerance" if seen
                      else "no float columns")

    def _narrowing_found(self, db):
        """Columns the target declares narrower than the source does
        (`verdict.narrowing`): a value that fits the source's is cut,
        rounded or refused on the way."""
        from .. import verdict
        narrow = []
        try:
            both = set(self.neutral_tables("dst", db))
            for t in self.neutral_tables("src", db):
                if t not in both or not self.in_scope(db, t):
                    continue
                dst = dict(self.neutral_columns("dst", db, t))
                for n, typ in self.neutral_columns("src", db, t):
                    if n in dst:
                        why = verdict.narrowing(self.CANON_ENGINE, typ,
                                                dst[n])
                        if why:
                            narrow.append(f"{t}.{n}: {why}")
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "narrowing", e)
        return verdict.narrowing_result(db, narrow)

    def _timeshift_found(self, db):
        """Each timestamp column's instants on both sides, by key: one
        shift on every row, or two an hour apart, is a conversion applied
        to the whole column - a session in a zone other than the source's
        on the way (`verdict.uniform_shift`)."""
        import datetime

        from .. import verdict

        def epoch(v):
            if isinstance(v, datetime.datetime):
                if v.tzinfo is not None:
                    return v.timestamp()
                return (v - datetime.datetime(1970, 1, 1)).total_seconds()
            return None
        shifts = []
        for t, key, col, _ in self._keyed_columns(db, ("timestamp",))[:20]:
            try:
                a = {k: epoch(v) for k, v in self._by_key(
                    "src", db, t, key, col).items()}
                b = {k: epoch(v) for k, v in self._by_key(
                    "dst", db, t, key, col).items()}
            except Exception:  # noqa: BLE001 - read where it can be
                continue
            line = verdict.uniform_shift(
                f"{t}.{col}", {k: v for k, v in a.items() if v is not None},
                {k: v for k, v in b.items() if v is not None})
            if line:
                shifts.append(line)
        return verdict.timeshift_result(db, shifts)

    def _as_text(self, quoted, declared):
        """A value of `declared` as the server writes it as text."""
        return f"cast({quoted} as varchar(4000))"

    def _rendering_found(self, db):
        """The values of the types this engine renders in a way of its own
        (`RENDERED`) as each server writes them, by key: a value that
        compares equal through migkit's renderer can still read otherwise
        to the application if the type or its settings changed."""
        drifts, audited = [], 0
        cols = []
        try:
            both = set(self.neutral_tables("dst", db))
            for t in self.neutral_tables("src", db):
                if t not in both or not self.in_scope(db, t):
                    continue
                key = list(self.neutral_key("src", db, t))
                if len(key) != 1:
                    continue
                for n, typ in self.neutral_columns("src", db, t):
                    base = str(typ).split("(")[0].split("<")[0].strip()\
                        .lower()
                    if base in self.RENDERED:
                        cols.append((t, key[0], n, typ))
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "render", e)
        for t, key, col, typ in cols[:20]:
            expr = self._as_text(self._q(col), typ)
            try:
                a = self._by_key("src", db, t, key, col, expr)
                b = self._by_key("dst", db, t, key, col, expr)
            except Exception:  # noqa: BLE001 - read where it can be
                drifts.append(f"{t}.{col}: not readable as text on one side")
                continue
            audited += 1
            for k, v in a.items():
                if k in b and str(b[k]) != str(v):
                    drifts.append(f"{t}.{col}: renders otherwise (src"
                                  f" {str(v)[:40]!r} dst {str(b[k])[:40]!r})")
                    break
        return Result("deep", f"{db} render", "diff" if drifts else "ok",
                      "; ".join(drifts[:5]) if drifts
                      else (f"{audited} columns of the server's own types"
                            " read the same as text" if audited
                            else "no column of a type the server renders"
                                 " in a way of its own"), "",
                      "check the type's definition and the server's"
                      " settings for it on the target" if drifts else "")

    def _boundary_found(self, db):
        """The target holding keys past the source's largest - rows the
        source never had, or a counter on the target that ran ahead: a
        move that is meant to make the target the source's copy left
        something else there."""
        from .. import canon
        ahead = []
        try:
            both = set(self.neutral_tables("dst", db))
            tables = [t for t in self.neutral_tables("src", db)
                      if t in both and self.in_scope(db, t)]
        except Exception as e:  # noqa: BLE001 - said
            return self._unread(db, "boundary", e)
        checked = 0
        for t in tables:
            try:
                key = list(self.neutral_key("src", db, t))
                if len(key) != 1:
                    continue
                cls = canon.type_class(self.CANON_ENGINE, dict(
                    self.neutral_columns("src", db, t)).get(key[0], ""))
                if cls not in ("integer", "decimal"):
                    continue
                k = self._q(key[0])
                a = self._rows("src", db, f"select max({k}) from"
                                          f" {self._qualified('src', db, t)}")
                b = self._rows("dst", db, f"select max({k}) from"
                                          f" {self._qualified('dst', db, t)}")
            except Exception:  # noqa: BLE001 - read where it can be
                continue
            checked += 1
            hi_a = a[0][0] if a else None
            hi_b = b[0][0] if b else None
            if hi_b is not None and (hi_a is None or hi_b > hi_a):
                ahead.append(f"{t}: target's largest {key[0]} {hi_b},"
                             f" source's {hi_a}")
        if ahead:
            return Result("deep", f"{db} boundary", "diff",
                          f"{len(ahead)} tables hold keys past the"
                          f" source's: {_listed(ahead, 4)}", "",
                          "find what wrote them on the target - an"
                          " application already pointed at it, or a test"
                          " run - before cutover")
        return Result("deep", f"{db} boundary", "ok",
                      f"{checked} tables keyed by a number hold nothing past"
                      " the source's largest key")

    # ---- counters ------------------------------------------------------------

    def _counters(self, side, db):
        """{name: next value} of one side's sequences and identity columns
        the hop takes: a sequence by its name, an identity column as
        `table.column`. A sequence is the hop's where it carries
        sequences (`takes`)."""
        out = {}
        if "sequences" in self.CATALOGUE:
            for name, nxt in self._taken_asked(side, db, "sequences",
                                               name_at=0, kind="sequence"):
                if nxt not in ("", None):
                    out[str(name)] = int(nxt)
        if "identity" in self.CATALOGUE:
            for t, c, nxt in self._scoped_asked(side, db, "identity",
                                                (0, 1)):
                if nxt not in ("", None):
                    out[f"{t}.{c}"] = int(nxt)
        return out

    def sequences_behind(self, db):
        """[(counter, table, column, the column's largest value)] for each
        identity column on the target that would hand out a value its rows
        already hold: the copy and the tail write each row with the
        source's key, and a counter does not move for a value it did not
        hand out. Asked of the target alone."""
        if "identity" not in self.CATALOGUE:
            return []
        out = []
        for t, c, nxt in self._scoped_asked("dst", db, "identity", (0, 1)):
            if nxt in ("", None):
                continue
            got = self._rows("dst", db, f"select max({self._q(c)}) from"
                                        f" {self._qualified('dst', db, t)}")
            top = got[0][0] if got else None
            if top is not None and int(top) >= int(nxt):
                out.append(Behind(db, f"{t}.{c}", t, c, int(top)))
        return out

    def _raise_past(self, behind):
        """The statements that move each identity column past its rows -
        each in the database it was found behind in, so a pair whose
        target this engine is can carry them (`HeteroEngine.repair_plan`)."""
        return [self._restart_sql(getattr(b, "db", None), b[0], b[3] + 1)
                for b in behind]

    def _restart_sql(self, db, name, value):
        """The statement setting a sequence, or an identity column
        (`table.column`), to hand out `value` next, in this engine's SQL."""
        raise NotImplementedError

    def check_autoinc(self, db):
        """usable: would an identity column hand out a value a row on the
        target holds; parity: is every sequence and identity column on the
        target at least where the source's is."""
        try:
            src = self._counters("src", db)
            dst = self._counters("dst", db)
            behind = self.sequences_behind(db)
        except Exception as e:  # noqa: BLE001 - said
            return [Result("autoinc", db, "error", "could not read the"
                           f" counters: {str(e).splitlines()[0][:90]}")]
        res = []
        if behind:
            res.append(Result(
                "autoinc", f"{db} usable", "diff",
                "an identity column would hand out a value a row holds: "
                + "; ".join(f"{t}.{c} largest {top}"
                            for _, t, c, top in behind[:6]), "",
                f"migkit sync {self.hop.name} --db {db} --kind sequences"))
        else:
            res.append(Result("autoinc", f"{db} usable", "ok",
                              "every identity column is past its rows"))
        short = [f"{n} source next {v} target {dst.get(n, 'missing')}"
                 for n, v in sorted(src.items())
                 if dst.get(n) is None or dst[n] < v]
        if short:
            res.append(Result("autoinc", f"{db} parity", "diff",
                              "; ".join(short[:8]), "",
                              f"migkit sync {self.hop.name} --db {db}"
                              " --kind sequences"))
        else:
            res.append(Result("autoinc", f"{db} parity", "ok",
                              f"{len(src)} counters at or past the"
                              " source's"))
        return res

    def _counters_plan(self, db):
        """The repair that sets each counter on the target where the
        source's is, or past its column's rows, with the statements that
        put each back where it was."""
        from .base import RepairAction
        src = self._counters("src", db)
        dst = self._counters("dst", db)
        names = [n for n, v in sorted(src.items())
                 if n in dst and dst[n] < v]
        stmts = [self._restart_sql(db, n, src[n]) for n in names]
        undo = [self._restart_sql(db, n, dst[n]) for n in names]
        for name, _, _, top in self.sequences_behind(db):
            if name not in names:
                stmts.append(self._restart_sql(db, name, top + 1))
                undo.append(self._restart_sql(db, name, dst.get(name,
                                                                top + 1)))
        if not stmts:
            return []
        return [RepairAction(db, "sequences", stmts, undo,
                             f"{len(stmts)} counters moved to where the"
                             " source's next value is, or past their"
                             " column's rows")]

    def _apply_counters(self, db, action):
        """Each statement of a counters repair, on the target."""
        self._target_only("dst", "set counters")
        conn = self._connect("dst", db)
        try:
            cur = conn.cursor()
            for s in action.statements:
                self._run(cur, s.rstrip(";"))
            if hasattr(conn, "commit"):
                conn.commit()
        finally:
            conn.close()

    def repair_plan(self, db, kind):
        """Counters set by this engine's own SQL, with their undo; rows
        carried by the pair every engine repairs through, which records
        each statement with its undo (`repairlog`) and acts only on the
        tables the hop takes."""
        actions = []
        if kind in ("sequences", "all") and (
                "sequences" in self.CATALOGUE
                or "identity" in self.CATALOGUE):
            actions += self._counters_plan(db)
        if kind in ("rows", "all"):
            actions += self._as_pair().repair_plan(db, "rows")
        return actions

    def apply(self, db, action):
        if action.kind == "sequences":
            return self._apply_counters(db, action)
        return self._as_pair().apply(db, action)

    # ---- accounts -------------------------------------------------------------

    def _owners_found(self, db):
        """Who owns each object on the target, against the source: an
        object owned on the target by another account than the one that
        owns it on the source is one the application cannot alter, and
        whose definer's rights another account's are (`ownership`)."""
        from .. import ownership as _own
        try:
            a = {f"{k} {n}": o for k, n, o in self._taken_asked(
                "src", db, "owners")}
            b = {f"{k} {n}": o for k, n, o in self._taken_asked(
                "dst", db, "owners")}
        except Exception as e:  # noqa: BLE001 - said
            return Result("deep", f"{db} ownership", "warn",
                          "cannot read owners:"
                          f" {str(e).splitlines()[0][:90]} - unknown, not"
                          " clean")
        shared = sorted(set(a) & set(b))
        if not shared:
            return Result("deep", f"{db} ownership", "skip",
                          "no objects present on both sides to compare")
        changes = _own.group([(n, a[n], b[n]) for n in shared])
        if not changes:
            return Result("deep", f"{db} ownership", "ok",
                          f"{len(shared)} objects keep their owner")
        note = ("one substitution across every object - most likely the"
                " mover's own account owning what it created"
                if _own.systematic(changes) else
                "objects drifted one by one - read each")
        return Result("deep", f"{db} ownership", "diff",
                      f"{_own.total(changes)} of {len(shared)}:"
                      f" {_own.describe(changes)}", "",
                      f"{note}; give each object to the account that owns"
                      " it on the source")

    def _grants_found(self, db):
        """Each grant on the tables (and sequences) the hop takes, on the
        target as on the source - for the grantees the target has: an
        account missing altogether is the users check's to say. Not
        compared where the hop does not carry accounts."""
        from ..users import NOT_CARRIED, carried, system_principal
        if not carried(self.hop):
            return [Result("deep", f"{db} grants", "skip", NOT_CARRIED),
                    Result("deep", f"{db} seq-grants", "skip", NOT_CARRIED)]
        eng = self.CANON_ENGINE
        out = []
        for question, word, kind in (("grants", "grants", None),
                                     ("seq_grants", "seq-grants",
                                      "sequence")):
            if question not in self.CATALOGUE:
                continue
            try:
                if kind is None:
                    a = self._scoped_asked("src", db, question)
                    b = self._scoped_asked("dst", db, question)
                else:
                    a = self._taken_asked("src", db, question, name_at=0,
                                          kind=kind)
                    b = self._taken_asked("dst", db, question, name_at=0,
                                          kind=kind)
            except Exception as e:  # noqa: BLE001 - said
                out.append(Result("deep", f"{db} {word}", "warn",
                                  "cannot read grants:"
                                  f" {str(e).splitlines()[0][:90]} -"
                                  " unknown, not clean"))
                continue
            a = {(o, g, str(p).upper()) for o, g, p in a
                 if not system_principal(eng, g)}
            b = {(o, g, str(p).upper()) for o, g, p in b
                 if not system_principal(eng, g)}
            there = {g for _, g, _ in b}
            missing = sorted(x for x in a - b if x[1] in there)
            if missing:
                out.append(Result(
                    "deep", f"{db} {word}", "diff",
                    f"{len(missing)} grants missing on target: "
                    + "; ".join(f"{p} on {o} to {g}"
                                for o, g, p in missing[:4]), "",
                    f"migkit users {self.hop.name} create --apply grants"
                    " them"))
            else:
                out.append(Result("deep", f"{db} {word}", "ok",
                                  f"{len(a)} grants on the target as on the"
                                  " source" if a else "no grants to compare"))
        return out

    # ---- the deep check, as each engine runs it -------------------------------

    def _deep(self, db, *checks):
        """The findings of `checks` - each a bound method of this engine
        answering one question of its catalogue - one or several each,
        a check that fails said as its own error."""
        out = []
        for check in checks:
            try:
                got = check(db)
            except Exception as e:  # noqa: BLE001 - said, as the error
                got = Result("deep", f"{db} {check.__name__.strip('_')}",
                             "error", str(e).splitlines()[0][:120]
                             if str(e) else type(e).__name__)
            out += got if isinstance(got, list) else [got] if got else []
        return out

