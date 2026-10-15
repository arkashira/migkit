"""The schema version each side stands on, from the application's own
migration history table (backlog 26.versions).

An application's tool records every migration it has run in a table (or
a collection) of its own. A move carries that table with the rest, so
after a move the two sides agree - until a migration is run on one side
only, which this finds: the tool would then apply it twice or never. A
migration recorded as failed stops the tool where it is recorded, one
older than the latest the other side records is skipped or refused by a
tool that applies in version order, and one recorded with two checksums
fails the tool's validation. A change migkit makes to the target's schema
itself (`Engine.own_schema_changes`) is in no such table: said, with how
each tool records it.

The history is read through the engine (`history_tables`,
`history_columns`, `history_rows`), and what it says is compared here: one
place for every engine, the check and the cutover's gate alike.
"""
import re
from dataclasses import dataclass, field


def _true(v):
    if isinstance(v, (bytes, bytearray)):
        return any(v)
    return str(v).strip().lower() in ("1", "true", "t", "y", "yes")


def _flyway(r):
    return "applied" if "success" not in r or _true(r["success"]) \
        else "failed"


def _liquibase(r):
    kind = str(r.get("exectype") or "").upper()
    return {"FAILED": "failed", "SKIPPED": "not"}.get(kind, "applied")


def _dirty(r):
    return "failed" if _true(r.get("dirty")) else "applied"


def _prisma(r):
    if r.get("rolled_back_at") not in (None, ""):
        return "not"
    return "failed" if r.get("finished_at") in (None, "") else "applied"


def _schemachange(r):
    s = str(r.get("status") or "").strip().upper()
    return "failed" if s and s != "SUCCESS" else "applied"


def _mongock(r):
    s = str(r.get("state") or "").upper()
    if s in ("FAILED", "ROLLBACK_FAILED"):
        return "failed"
    return "not" if s in ("ROLLED_BACK", "IGNORED") else "applied"


@dataclass(frozen=True)
class Tool:
    name: str
    #: how it is named to the operator
    said: str
    #: the table's spellings, lower-cased
    tables: tuple
    #: the columns that name one applied migration
    ids: tuple
    #: read where the table has them: run order, checksum, state
    extra: tuple = ()
    #: columns that must be there, and must not, for a table to be its
    needs: frozenset = frozenset()
    lacks: frozenset = frozenset()
    #: a table of its own that must stand beside it
    beside: tuple = ()
    order: str = ""
    checksum: str = ""
    state: object = None
    #: it records only where it stands, not every migration it ran
    heads: bool = False
    #: the id that sorts as a version, and the one it is grouped by
    version: int = None
    group: int = None
    #: how a change made outside it is recorded; [col] is a column, {t}
    #: the table, quoted as the side quotes them
    record: str = ""
    #: what clears a migration recorded as failed
    unfail: str = "undo what the failed migration did, delete its row,"\
                  " and run it again"

    def of(self, cols):
        need = self.needs or frozenset(c.lower() for c in self.ids)
        return need <= cols and not (self.lacks & cols)


_INSERT = "insert into {t} "
SPECS = (
    Tool("Flyway", "Flyway", ("flyway_schema_history", "schema_version"),
         ("version", "description"), ("installed_rank", "checksum",
                                      "success"),
         order="installed_rank", checksum="checksum", state=_flyway,
         version=0,
         record=_INSERT + "([installed_rank], [version], [description],"
         " [type], [script], [checksum], [installed_by], [execution_time],"
         " [success]) values (<next rank>, '<version>', '<description>',"
         " 'SQL', '<script>', null, current_user, 0, true), then"
         " `flyway repair` to fill its checksum",
         unfail="undo what the failed migration did, then `flyway repair`"
                " removes its row"),
    Tool("Liquibase", "databasechangelog", ("databasechangelog",),
         ("id", "author", "filename"), ("orderexecuted", "md5sum",
                                        "exectype"),
         order="orderexecuted", checksum="md5sum", state=_liquibase,
         record="add a changeset for it to the changelog and run the"
                " changelog's `changelog-sync`, which records it run"),
    Tool("Alembic", "Alembic", ("alembic_version",), ("version_num",),
         heads=True, record="`alembic revision -m '<what>'` for it, then"
                            " `alembic stamp <revision>`"),
    Tool("Django", "Django", ("django_migrations",), ("app", "name"),
         ("id",), order="id", version=1, group=0,
         record="add the migration and run `python manage.py migrate"
                " <app> <migration> --fake`"),
    Tool("golang-migrate", "golang-migrate", ("schema_migrations",),
         ("version",), ("dirty",), needs=frozenset({"version", "dirty"}),
         state=_dirty, heads=True, version=0,
         record="add the migration's files and run `migrate -path <dir>"
                " -database <url> force <version>`",
         unfail="undo what the half-run migration did, then `migrate"
                " force <the last good version>`"),
    Tool("Rails", "Rails", ("schema_migrations",), ("version",),
         lacks=frozenset({"dirty"}), beside=("ar_internal_metadata",),
         version=0, record="add the migration under db/migrate and "
         + _INSERT + "([version]) values ('<its timestamp>')"),
    Tool("dbmate", "dbmate", ("schema_migrations",), ("version",),
         lacks=frozenset({"dirty"}), version=0,
         record="add the migration file and " + _INSERT
         + "([version]) values ('<its version>')"),
    Tool("Entity Framework", "Entity Framework", ("__efmigrationshistory",),
         ("MigrationId",), ("ProductVersion",), version=0,
         record="add the migration and " + _INSERT
         + "([MigrationId], [ProductVersion]) values ('<its id>',"
         " '<the runtime version>')"),
    Tool("schemachange", "schemachange", ("change_history",),
         ("version", "script"), ("installed_on", "checksum", "status"),
         needs=frozenset({"version", "script", "checksum"}),
         order="installed_on", checksum="checksum", state=_schemachange,
         version=0, record="add V<version>__<what>.sql and " + _INSERT
         + "([version], [description], [script], [script_type], [checksum],"
         " [execution_time], [status], [installed_by], [installed_on])"
         " values ('<version>', '<what>', 'V<version>__<what>.sql', 'V',"
         " '<the script's checksum>', 0, 'Success', current_user,"
         " current_timestamp)"),
    Tool("Knex", "Knex", ("knex_migrations",), ("name",), ("id",),
         needs=frozenset({"name", "batch"}), order="id", version=0,
         record="add the migration file and " + _INSERT
         + "([name], [batch], [migration_time]) values ('<file>',"
         " <next batch>, current_timestamp)"),
    Tool("Prisma", "Prisma", ("_prisma_migrations",), ("migration_name",),
         ("started_at", "finished_at", "rolled_back_at", "checksum"),
         order="started_at", checksum="checksum", state=_prisma, version=0,
         record="add the migration folder and run `prisma migrate resolve"
                " --applied <migration_name>`",
         unfail="undo what it did, then `prisma migrate resolve"
                " --rolled-back <migration_name>`"),
    Tool("Sequelize", "Sequelize", ("sequelizemeta",), ("name",),
         version=0, record="add the migration file and " + _INSERT
         + "([name]) values ('<file>')"),
    Tool("TypeORM", "TypeORM", ("migrations",), ("timestamp", "name"),
         ("id",), needs=frozenset({"timestamp", "name"}), order="id",
         version=0, record="add the migration class and " + _INSERT
         + "([timestamp], [name]) values (<its timestamp>,"
         " '<Name><timestamp>')"),
    Tool("migrate-mongo", "migrate-mongo", ("changelog",), ("fileName",),
         ("appliedAt",), needs=frozenset({"filename", "appliedat"}),
         order="appliedAt", version=0,
         record="add the migration file and"
                " `db.{t}.insertOne({fileName: '<file>', appliedAt: new"
                " Date()})`"),
    Tool("Mongock", "Mongock", ("mongockchangelog",), ("changeId", "author"),
         ("state", "timestamp"), order="timestamp", state=_mongock,
         record="add the change unit and `db.{t}.insertOne({changeId:"
                " '<id>', author: '<author>', state: 'EXECUTED', type:"
                " 'EXECUTION', timestamp: new Date()})`"),
)
BY_NAME = {t.name: t for t in SPECS}
#: tool -> (table spellings, columns naming one migration, a condition):
#: the tables an engine that lists them by name asks for
TOOLS = {t.name: (t.tables + t.beside, t.ids, "") for t in SPECS}
NAMES = tuple(sorted({n for t in SPECS for n in t.tables + t.beside}))


def named(identifiers):
    """{lower-cased table name: the identifier} of those among an engine's
    `neutral_tables` a tool may keep its history in; the first of a name
    wins."""
    out = {}
    for ident in identifiers:
        leaf = str(ident).rpartition(".")[2].strip('"`[]').lower()
        if leaf in NAMES:
            out.setdefault(leaf, ident)
    return out


def _key(text):
    """A version's sort key: its runs of digits as numbers."""
    return tuple(int(p) if i % 2 else p
                 for i, p in enumerate(re.split(r"(\d+)", str(text))))


def _text(v):
    if v is None or type(v).__name__ == "_Absent":
        return ""
    return str(v)


@dataclass
class History:
    """One side's record of one tool."""
    tool: Tool
    applied: set = field(default_factory=set)
    failed: set = field(default_factory=set)
    sums: dict = field(default_factory=dict)
    table: str = ""

    @classmethod
    def of(cls, tool, table, rows):
        h = cls(tool, table=table)
        rows = list(rows)
        if tool.order:
            o = tool.order.lower()
            rows.sort(key=lambda r: _key(_text(r.get(o))))
        for r in rows:
            ident = tuple(_text(r.get(c.lower())) for c in tool.ids)
            state = tool.state(r) if tool.state else "applied"
            if state == "applied":
                h.applied.add(ident)
                if tool.checksum and r.get(tool.checksum) not in (None, ""):
                    h.sums[ident] = _text(r[tool.checksum])
            elif state == "failed":
                h.failed.add(ident)
        h.failed -= h.applied
        return h

    def latest(self):
        """{group: the highest version applied}, for a tool whose ids sort."""
        t, out = self.tool, {}
        if t.version is None:
            return out
        for i in self.applied:
            if not i[t.version]:
                continue
            g = i[t.group] if t.group is not None else ""
            if g not in out or _key(i[t.version]) > _key(out[g]):
                out[g] = i[t.version]
        return out


def _as_history(name, x):
    if isinstance(x, History):
        return x
    return History(BY_NAME.get(name) or Tool(name, name, (), ()),
                   applied=set(x))


def applied(history_tables, history_rows, history_columns=None):
    """{tool: History} of one side, for each tool whose table the side has.
    `history_tables()` answers {lower-cased table name: how the engine
    names it}; `history_columns(ref)` [(column, type)] of one;
    `history_rows(ref, columns, where)` its rows, as tuples."""
    have = history_tables()
    out, taken = {}, set()
    for tool in SPECS:
        for spelling in tool.tables:
            ref = have.get(spelling)
            if ref is None or ref in taken:
                continue
            cols = {str(n).lower(): str(n) for n, _ in
                    (history_columns(ref) if history_columns else ())}
            if not cols:
                if tool.needs or tool.lacks or tool.beside:
                    continue
                cols = {c.lower(): c for c in tool.ids}
            if not tool.of(set(cols)) or not all(b in have
                                                  for b in tool.beside):
                continue
            want = [c.lower() for c in tool.ids + tool.extra
                    if c.lower() in cols]
            rows = history_rows(ref, [cols[c] for c in want], "")
            out[tool.name] = History.of(
                tool, ref, [dict(zip(want, r)) for r in rows])
            taken.add(ref)
            break
    return out


def read(engine, db):
    """(source's, target's) `applied`, through the engine."""
    return tuple(applied(
        lambda side=side: engine.history_tables(side, db),
        lambda ref, cols, where, side=side: engine.history_rows(
            side, db, ref, cols, where),
        lambda ref, side=side: engine.history_columns(side, db, ref))
        for side in ("src", "dst"))


def _name(row):
    return "/".join(p for p in row if p)[:80]


def _names(ids, most=3):
    ids = sorted(ids, key=lambda i: tuple(_key(p) for p in i))
    return ", ".join(_name(i) for i in ids[-most:]) + (
        f" and {len(ids) - most} more" if len(ids) > most else "")


@dataclass
class Apart:
    tool: str
    said: str
    status: str
    detail: str
    fix: str = ""
    #: [("gate" | "warn", what, with the next step)] for the cutover
    cutover: list = field(default_factory=list)


def apart(src, dst):
    """[Apart] per tool of the two sides' `applied`."""
    out = []
    for name in sorted(set(src) | set(dst)):
        s = _as_history(name, src[name]) if name in src else None
        t = _as_history(name, dst[name]) if name in dst else None
        tool = (s or t).tool
        said = tool.said
        if s is None or t is None:
            where = "source" if t is None else "target"
            text = (f"{said} keeps its history on the {where} only: the"
                    " other side has no record of the migrations it ran,"
                    " so the tool would run them all again")
            step = (f"carry the {where}'s history table to the other side"
                    " (in the hop's scope) and check again")
            out.append(Apart(name, said, "diff", text, step,
                             [("gate" if where == "source" else "warn",
                               f"{text} - {step}")]))
            continue
        only_s, only_t = s.applied - t.applied, t.applied - s.applied
        if tool.heads and tool.version is not None and only_s and only_t:
            # where it stands, not what it ran: the higher one is ahead
            hs = max(_key(i[tool.version]) for i in only_s)
            ht = max(_key(i[tool.version]) for i in only_t)
            if ht > hs:
                only_s = set()
            else:
                only_t = set()
        said_, gate = [], []
        if only_s:
            ahead = t.latest()
            behind = [i for i in only_s if tool.version is not None
                      and i[tool.version] and ahead.get(
                          i[tool.group] if tool.group is not None else "")
                      and _key(i[tool.version]) < _key(ahead[
                          i[tool.group] if tool.group is not None else ""])]
            text = (f"{len(only_s)} recorded on the source only (e.g."
                    f" {_names(only_s)})")
            if behind:
                text += (f", {len(behind)} of them older than the latest"
                         f" the target records ({_names(behind)}): a tool"
                         " that applies in version order skips or refuses"
                         " them there")
            said_.append(text)
            gate.append(("gate", f"{said}: {text} - let the stream carry"
                         " the history rows and the schema they made, or"
                         " run the application's migrations against the"
                         " target, then check again"))
        if only_t:
            text = (f"{len(only_t)} recorded on the target only (e.g."
                    f" {_names(only_t)})")
            said_.append(text)
            gate.append(("warn", f"{said}: {text} - run on the target and"
                         " not on the source; confirm it was meant before"
                         " the application moves"))
        for h, where in ((s, "source"), (t, "target")):
            if h.failed:
                text = (f"{len(h.failed)} recorded as failed on the {where}"
                        f" ({_names(h.failed)}): the tool stops there")
                said_.append(text)
                gate.append(("gate" if where == "target" else "warn",
                             f"{said}: {text} - {tool.unfail}"))
        sums = sorted(i for i in s.applied & t.applied
                      if s.sums.get(i) and t.sums.get(i)
                      and s.sums[i] != t.sums[i])
        if sums:
            text = (f"{len(sums)} recorded with another checksum on each"
                    f" side ({_names(sums)}): the tool's validation fails")
            said_.append(text)
            gate.append(("gate", f"{said}: {text} - align the scripts with"
                         " the applied ones, then check again"))
        if not said_:
            out.append(Apart(name, said, "ok",
                             f"{said}: the same {len(s.applied)} migrations"
                             " are recorded on both sides"))
            continue
        differs = only_s or only_t or sums
        out.append(Apart(
            name, said, "diff" if differs else "warn",
            f"{said}: " + "; ".join(said_),
            "run the application's migration tool on the side that lacks"
            " them, or compare the history tables" if differs
            else tool.unfail, gate))
    return out


def compare(src, dst):
    """[(status, tool, detail)] from the two sides' `applied`."""
    return [(a.status, a.tool, a.detail) for a in apart(src, dst)]


def _render(tool, table, quote):
    def part(p):
        return p if p[:1] in "\"`[" else quote(p)
    t = ".".join(part(p) for p in str(table).split(".")) if table else "?"
    return re.sub(r"\[(\w+)\]", lambda m: quote(m.group(1)),
                  tool.record).replace("{t}", t)


def unrecorded(dst, changes, quote=lambda n: n):
    """[(tool, said, detail, how)] for each tool whose history the target
    keeps, where migkit changed the target's schema itself (`changes`)."""
    if not changes:
        return []
    out = []
    for name in sorted(dst):
        h = _as_history(name, dst[name])
        out.append((name, h.tool.said,
                    "migkit changed the target's schema itself - "
                    + "; ".join(changes) + f" - and the application's"
                    f" {h.tool.said} history does not know of it: its next"
                    " migration is written against the schema it records",
                    "record it as a migration of the application's: "
                    + _render(h.tool, h.table, quote)))
    return out


def at_cutover(src, dst, changes=(), quote=lambda n: n):
    """[("gate" | "warn", what)] the cutover's preflight holds the
    application's history to."""
    out = [row for a in apart(src, dst) for row in a.cutover]
    out += [("warn", f"{detail}; {how}") for _, _, detail, how in
            unrecorded(dst, changes, quote)]
    return out


def said_in(results):
    """Whether a schema check's answers already hold these findings."""
    return any(r.get("check") == "schema"
               and str(r.get("scope", "")).endswith(" history")
               for r in results)
