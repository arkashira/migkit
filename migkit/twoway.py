"""Two ways at once through migkit's own tails (backlog R3, P0-DEPTH 2).

Two hops of one pair of databases, each with `two_way` in its options, A
to B and B to A: each side's changes carried to the other, what migkit
applied never carried back, and a row both sides changed settled column
by column, by the rule the hop gives each column - so the two sides end
alike, and neither loses what only it changed.

    two_way:
      on_conflict: error            # the rule of a column nothing names
      column: updated_at            # what `newest` compares
      source_rank: 2                # source_priority, and every tie:
      target_rank: 1                #   the side ranked higher wins
      delta: [balance, stock]       # counters, on every table
      delete_vs_update: update      # one side deleted a row, the other
                                    #   changed it
      delete_missing: ignore        # a delete of a row the target has not
                                    #   got
      tables:
        accounts:
          default: newest           # this table's other columns
          column: changed_at
          columns:
            balance: delta
            tags: union
            best: max
            "street, city, zip": {newest: moved_at}    # settled together
          delete_vs_update: delete

How migkit's own writes are told apart: every transaction a two-way tail
applies carries a mark first, and the tail reading that side the other
way leaves the whole transaction out. Which mark is chosen per side from
a ladder (`marks`): a replication origin, a logical message, a tagged
GTID, MariaDB's skip flag, a comment on each statement, or a row of a
table of migkit's own (`migkit_origin`, a row per applying thread, its
count moved) - the fastest the side allows and proves, kept in the tail's
token. Where the servers' own replication can run both ways instead
(`loops_prevented`), none is needed.

What a conflict is. The target's row as it is now is held to what the
change says the row was before it (the full row a binlog keeps, or a
PostgreSQL table with REPLICA IDENTITY FULL); a batch's changes to one row
are held as one - the row before the first, the row after the last. A
column the source changed and the target did not is the source's, one the
target changed and the source did not is the target's, and only a column
both changed, to different values, is a conflict: only it is settled by a
rule. Two sides that changed different columns of one row both keep what
they changed (F7.merge) - the change writes its own columns and leaves the
target's; no row is written whole over the other side's column. The five
kinds, each settled explicitly:

    insert_exists          the source made a row the target made too, with
                           other values: each column that differs settled
                           by its rule, the row taken as made from nothing
    update_origin_differs  both changed the row: each column both changed
                           settled by its rule, the rest merged
    update_missing         the source changed a row the target has not got
    delete_origin_differs  the source deleted a row the target changed
    delete_missing         the source deleted a row the target has not got

A row one side deleted and the other changed is `delete_vs_update`'s, the
same rule for both its kinds so the two ends agree: `update` (the changed
row stays, and is made again whole on the side that deleted it - nothing
is lost), `delete`, `overwrite` (the incoming change), `ignore` (the
target's), `source_priority`, `park` or `error` - by default what the
table's own rule says of rows (`error`, `park`, `overwrite`, `ignore` and
`source_priority` as themselves; the rules that settle values, `update`).
`delete_missing` is `ignore` - the row is gone on both sides, as the
delete wanted, and it is still recorded - unless the hop says `park` or
`error`. Without a before image only `insert_exists`, `update_missing`
and `delete_missing` can be seen, and that is said once.

The rules, for a column or a group of columns settled together:

    error            the tail stops before anything of the batch is applied
                     (what it has always done)
    park             the row set aside on this machine: its changes held,
                     the rest of the tail going on; it goes on again once
                     the target's row is what the source's last change
                     left - an operator decides it on both sides
    overwrite        the incoming change's value (apply_remote)
    ignore           the target's (keep_local)
    source_priority  the side ranked higher
    newest           the side whose `column` is later (last_update_wins)
    max, min         the higher, the lower - of the column itself, or of the
                     column named ({max: version}) for a group
    delta            a counter: the change applied as what it added, where
                     the target's row stands, so both sides' additions are
                     kept (`canon.Added`); a row both made, or a change with
                     no before image, says no addition, and its counter
                     falls to the table's rule
    union            a set, an array or a JSON array: a member either side
                     added is in, one either side removed is out

A column with no rule takes its table's, then the hop's. A group is
settled whole: a column of it changed on both sides makes the group a
conflict, and the side the rule picks gives all of its columns - for the
columns that must stay consistent (an address), and for the ones a
timestamp describes, with the timestamp in the group. A column a rule
compares and names no rule of its own goes with what it decides. A tie
goes to the side ranked higher; where neither is, to the larger of the
two versions as they render - the same answer from both ends.

Converging. Each conflict is settled twice, once at each end: A's change
against B's row on B, and B's against A's on A. Every rule reaches the
same row from both ends - `newest`, `max`, `min`, `delta` and `union` by
what they are, `overwrite` where the hop back says `ignore` and the other
way round, `source_priority` where both hops rank the two sides alike;
the configuration check refuses a pair that would settle one conflict
apart (`apart`). Where each side's changes to a row since the two last
agreed reach the other as one batch or in order, both end alike in any
order and batching (`test_two_way_resolution_converges`). What no before
image sees is a side that changed a value and changed it back between two
exchanges, or a row deleted after a version that won: rules that compare
the values alone can then end apart, the limit every before-image
resolver has. A group whose `newest` column every write moves forward, a
counter, and a `max` over values that only rise converge whatever the
interleaving; `check` finds the rest.

Every conflict is written to `conflicts.jsonl`: the row before, both
versions, each column's rule and what it took, what the row ended as. A
batch that had any adds a line to the hop's record (`audit`) with their
count and the digest of those lines, the counts are on `/metrics`
(`conflicts-counts.json`), and the hop's receivers are told
(`notify.conflicts`); the evidence export carries every one.
"""
import datetime
import hashlib
import json
import threading
from collections import Counter
from dataclasses import dataclass

from . import canon

TABLE = "migkit_origin"
#: the whole-row policies a hop named before rules were per column; each
#: is one of `RULES` now
POLICIES = ("error", "apply_remote", "keep_local", "last_update_wins",
            "source_priority")
#: what settles a column, or a group of them, both sides changed
RULES = ("error", "park", "overwrite", "ignore", "source_priority",
         "newest", "max", "min", "delta", "union")
#: the old names, as the rules they are
ALIASES = {"apply_remote": "overwrite", "keep_local": "ignore",
           "last_update_wins": "newest"}
#: the rules that compare a column
COMPARING = ("newest", "max", "min")
#: what settles a row one side deleted and the other changed
DELETE_VS_UPDATE = ("update", "delete", "overwrite", "ignore",
                    "source_priority", "park", "error")
#: what settles a delete of a row the target has not got
DELETE_MISSING = ("ignore", "park", "error")
#: the kinds of conflict
KINDS = ("insert_exists", "update_origin_differs", "update_missing",
         "delete_origin_differs", "delete_missing")
#: the rule the hop back has to say, where it is not the same one
MIRROR = {"overwrite": "ignore", "ignore": "overwrite"}
#: the classes a counter can be
NUMERIC = ("integer", "decimal", "float")
#: the keys of one table's entry under `tables`
TABLE_KEYS = ("columns", "default", "column", "delete_vs_update",
              "delete_missing")
#: the files a two-way tail keeps its conflicts in, per database: every
#: conflict with its rows (values: sealed where the hop keeps them so),
#: the rows parked (values too), and the counts `/metrics` reads
RECORD = "conflicts.jsonl"
PARKED = "conflicts-parked.json"
COUNTS = "conflicts-counts.json"


def settings(hop):
    got = (hop.options or {}).get("two_way")
    if not got:
        return None
    return got if isinstance(got, dict) else {}


def policy(hop):
    """(policy, column) of a two-way hop, every rule it names checked:
    refused where one is not a rule migkit knows, `newest` compares no
    column, or `source_priority` has no ranks."""
    got = settings(hop) or {}
    rules(hop)
    return str(got.get("on_conflict", "error")), got.get("column")


def _ranks_of(got):
    """(source rank, target rank), or None where the hop ranks neither or
    ranks both alike."""
    try:
        a, b = int(got["source_rank"]), int(got["target_rank"])
    except (KeyError, TypeError, ValueError):
        return None
    return None if a == b else (a, b)


def _ranks(hop):
    return _ranks_of(settings(hop) or {})


def deltas(hop):
    got = (settings(hop) or {}).get("delta") or []
    if isinstance(got, str):
        got = [got]
    return [str(x) for x in got]


# ---- the rules ----------------------------------------------------------------

@dataclass(frozen=True)
class Rule:
    """How a column, or a group of them, is settled where both sides
    changed it: `name` one of `RULES`, `by` the column `newest`, `max` or
    `min` compares (None: the column itself)."""
    name: str
    by: str = None

    def words(self):
        return f"{self.name} by {self.by}" if self.by else self.name


@dataclass(frozen=True)
class TableRules:
    """One table's rules: the groups the hop names ((columns, rule)), the
    rule of every other column, the one a counter or a set falls to where
    a row says no addition (`fallback`), and the two rules of rows."""
    groups: tuple = ()
    default: Rule = Rule("error")
    fallback: Rule = Rule("error")
    delete_vs_update: str = "error"
    delete_missing: str = "ignore"

    def rule_of(self, column):
        """(the columns settled together with `column`, their rule)."""
        for cols, rule in self.groups:
            if column in cols:
                return cols, rule
        return (column,), self.default

    def named(self, column):
        """Whether the hop names `column` itself, not by the default."""
        return any(column in cols for cols, _ in self.groups)

    def counters(self, names, keys, classes):
        """The columns of `names` that are counters: not the key, and a
        number (a column the default makes a counter and is none is
        settled by `fallback`)."""
        return {n for n in names if n not in keys
                and classes.get(n) in NUMERIC
                and self.rule_of(n)[1].name == "delta"}

    def uses(self, name):
        return self.default.name == name or any(
            r.name == name for _, r in self.groups)


def _near(word, words):
    import difflib
    got = difflib.get_close_matches(str(word), list(words), n=1)
    return f" (did you mean {got[0]}?)" if got else ""


def _rule(raw, where, column):
    """A rule as a hop writes one: its name, or {name: the column it
    compares}."""
    by = None
    if isinstance(raw, dict):
        if len(raw) != 1:
            raise SystemExit(f"{where}: one rule - a name, or"
                             " {newest|max|min: the column it compares}")
        (raw, by), = raw.items()
    said = str(raw).strip().lower()
    name = ALIASES.get(said, said)
    if name not in RULES:
        raise SystemExit(f"{where}: {raw} is not one of {', '.join(RULES)}"
                         + _near(said, list(RULES) + list(ALIASES)))
    if by is not None and name not in COMPARING:
        raise SystemExit(f"{where}: {name} compares no column, so it names"
                         " none")
    if name == "newest":
        by = by or column
        if not by:
            if where == "two_way.on_conflict":
                raise SystemExit(
                    "two_way.on_conflict: last_update_wins compares a"
                    " column each row carries its last change's time in -"
                    " name it as two_way.column")
            raise SystemExit(f"{where}: newest compares a column each row"
                             " carries its last change's time in - name it"
                             " as {newest: column}, or the table's or the"
                             " hop's `column`")
    return Rule(name, str(by) if by else None)


def _whole(raw, where, allowed):
    if raw is None:
        return None
    said = str(raw).strip().lower()
    said = ALIASES.get(said, said)
    if said not in allowed:
        raise SystemExit(f"{where}: {raw} is not one of"
                         f" {', '.join(allowed)}" + _near(said, allowed))
    return said


#: what a row one side deleted and the other changed takes by default,
#: from the table's rule: the rules of rows as themselves, and every rule
#: that settles values keeps the changed row - nothing is lost
IMPLIED = {"error": "error", "park": "park", "overwrite": "overwrite",
           "ignore": "ignore", "source_priority": "source_priority"}


class Rules:
    """A two-way hop's rules, read and checked whole (`rules`): `table`
    gives one table's."""

    def __init__(self, got, ranks):
        self.ranks = ranks
        self.column = got.get("column")
        self.raw_default = got.get("on_conflict", "error")
        self.default = _rule(self.raw_default, "two_way.on_conflict",
                             self.column)
        legacy = got.get("delta") or []
        self.legacy = [str(x) for x in ([legacy] if isinstance(legacy, str)
                                        else legacy)]
        self.delete_vs_update = _whole(got.get("delete_vs_update"),
                                       "two_way.delete_vs_update",
                                       DELETE_VS_UPDATE)
        self.delete_missing = _whole(got.get("delete_missing"),
                                     "two_way.delete_missing",
                                     DELETE_MISSING)
        tables = got.get("tables") or {}
        if not isinstance(tables, dict):
            raise SystemExit("two_way.tables: a mapping of each table's"
                             " rules by its name")
        self.tables = {str(t): self._table(str(t), raw or {})
                       for t, raw in tables.items()}
        self.all = self._table(None, {})
        every = [self.all] + list(self.tables.values())
        wanted = [t for t in every if t.uses("source_priority")
                  or t.delete_vs_update == "source_priority"]
        if wanted and ranks is None:
            raise SystemExit("two_way.on_conflict: source_priority keeps"
                             " the row of the side ranked higher - give"
                             " both as two_way.source_rank and"
                             " two_way.target_rank, apart")
        #: whether a batch applied twice would add twice (`exact`)
        self.adds = any(t.uses("delta") for t in every)

    def _table(self, name, raw):
        where = f"two_way.tables.{name}" if name else "two_way"
        if not isinstance(raw, dict):
            raise SystemExit(f"{where}: a mapping of {', '.join(TABLE_KEYS)}")
        for k in raw:
            if k not in TABLE_KEYS:
                raise SystemExit(f"{where}.{k}: not a key of a table's"
                                 f" rules - {', '.join(TABLE_KEYS)}"
                                 + _near(k, TABLE_KEYS))
        column = raw.get("column") or self.column
        if raw.get("default") is not None:
            default = _rule(raw["default"], f"{where}.default", column)
        else:
            default = _rule(self.raw_default, "two_way.on_conflict", column)
        groups, named = [], set()
        explicit = raw.get("columns") or {}
        if not isinstance(explicit, dict):
            raise SystemExit(f"{where}.columns: a mapping of a column (or"
                             " columns settled together, \"a, b\") to its"
                             " rule")
        for key, said in explicit.items():
            cols = tuple(c.strip() for c in str(key).split(",") if c.strip())
            at = f"{where}.columns.{key}"
            rule = _rule(said, at, column)
            if len(cols) > 1 and rule.name in ("delta", "union"):
                raise SystemExit(f"{at}: {rule.name} settles one column -"
                                 " give each its own line")
            if len(cols) > 1 and rule.name in ("max", "min") \
                    and not rule.by:
                raise SystemExit(f"{at}: columns settled together are"
                                 " settled by one column's value - name it"
                                 f" as {{{rule.name}: column}}")
            twice = named & set(cols)
            if not cols or twice:
                raise SystemExit(f"{at}: "
                                 + (f"{', '.join(sorted(twice))} already"
                                    " has a rule" if twice else
                                    "names no column"))
            groups.append((cols, rule))
            named |= set(cols)
        for n in self.legacy:
            if n not in named:
                groups.append(((n,), Rule("delta")))
                named.add(n)
        # a column a rule compares, with no rule of its own, goes with what
        # it decides: both bump it, and it is not a conflict of its own
        for cols, rule in list(groups):
            if rule.by and rule.by not in named:
                groups.append(((rule.by,), Rule(rule.name, rule.by)))
                named.add(rule.by)
        fallback = next((r for r in (default, self.default)
                         if r.name not in ("delta", "union")), Rule("error"))
        dvu = _whole(raw.get("delete_vs_update"),
                     f"{where}.delete_vs_update", DELETE_VS_UPDATE) \
            or self.delete_vs_update or IMPLIED.get(default.name, "update")
        missing = _whole(raw.get("delete_missing"),
                         f"{where}.delete_missing", DELETE_MISSING) \
            or self.delete_missing or "ignore"
        return TableRules(tuple(groups), default, fallback, dvu, missing)

    def table(self, name):
        """The rules of a table, by its name as the change carries it: the
        hop's entry of that name, or of its last part, or the hop's own."""
        name = str(name)
        low = name.lower()
        leaf = low.rpartition(".")[2]
        for t, got in self.tables.items():
            if t.lower() == low:
                return got
        for t, got in self.tables.items():
            if t.lower().rpartition(".")[2] == leaf:
                return got
        return self.all


def rules(hop):
    """The hop's rules (`Rules`), or refused where one is not sound."""
    got = settings(hop) or {}
    return Rules(got, _ranks_of(got))


def refuse_unless_able(src, dst):
    """Both engines must mark what they apply and skip what is marked:
    one that cannot sends every change back where it came from."""
    for eng, role in ((src, "source"), (dst, "target")):
        if not getattr(eng, "READS_ORIGIN_MARK", False) or \
                not callable(getattr(eng, "origin_mark", None)):
            raise SystemExit(
                f"two_way: the {role} ({type(eng).__name__[:-6].lower()})"
                " cannot mark what migkit applies or leave it out when"
                " read, so every change would come back to where it began."
                " Two ways run through migkit between PostgreSQL and MySQL"
                " (or MariaDB) sides")


def thread_origin(hop, numbered=False):
    """The row of `migkit_origin` this applying thread moves: one a
    thread, so lanes applying side by side do not wait on one row - and
    one for the hop where it counts (`exact`), or where a one-way tail
    numbers its batches (`numbered`, `marks.settle_exact`): a numbered
    batch is one transaction, and the row is read back by whichever
    thread, or process, goes on after a stop."""
    if numbered or exact(hop):
        return hop.name
    return f"{hop.name}:{threading.get_ident() % 1024}"


def exact(hop):
    """Whether a batch applied twice would be wrong: counters (`delta`,
    wherever the hop names one) add, so a batch the target committed and
    the tail had not yet saved the position of must not be applied again.
    Such a hop's batch is one transaction, and says in its mark which
    batch it was (`batch_seen`); the tail going on after a stop asks the
    target (`committed_ahead`)."""
    if settings(hop) is None:
        return False
    try:
        return rules(hop).adds
    except SystemExit:
        # refused where the tail starts (`policy`); asked before that, the
        # counters it names plainly
        return bool(deltas(hop))


def batch_seen(engine):
    """The text the mark of the batch being applied carries: its
    position and its number, set by the tail before it applies."""
    return engine.__dict__.get("_batch_seen")


def seen_of(text):
    if not text:
        return None
    try:
        got = json.loads(text)
    except ValueError:
        return None
    return got if isinstance(got, dict) and "batch" in got else None


def committed_ahead(dst, db, batch, rung="table", pending=None):
    """(position, number) of a batch the target committed after the one
    the tail saved last, or None: the target's mark is written in the
    batch's own transaction, so it is there exactly when the batch is.
    The table's mark says where the batch ended too; a replication origin
    or a GTID only says the batch was committed, and where it ended is
    what the tail wrote down before applying it (`pending`)."""
    if rung in (None, "table"):
        got = dst.origin_seen("dst", db)
        if got and int(got.get("batch", -1)) == batch + 1:
            return got["token"], batch + 1
        return None
    if not dst.mark_committed("dst", db, rung, batch + 1):
        return None
    if not pending or int(pending.get("batch", -1)) != batch + 1:
        raise SystemExit(
            f"two_way: the target committed batch {batch + 1} of this tail,"
            " and this machine has no record of where in the source's log"
            " that batch ended - its position was saved elsewhere, or"
            " lost. Applying it again would add its counters twice, and"
            " going on from an older position would too; nothing was"
            " applied. Give this tail back the position file it saved, or"
            " move again with --mode full+cdc")
    return pending["token"], batch + 1


# ---- values ---------------------------------------------------------------------

def _typed(cls, value):
    """A value a change log handed over as text, as the class it is: a
    PostgreSQL slot's timestamp is the string it printed, and a MySQL
    target's the datetime its driver made - rendered apart, they differed
    where nothing had changed (measured)."""
    import datetime
    import decimal
    if not isinstance(value, str):
        return value
    try:
        if cls == "timestamp":
            return datetime.datetime.fromisoformat(value)
        if cls == "date":
            return datetime.date.fromisoformat(value)
        if cls == "time":
            return datetime.time.fromisoformat(value)
        if cls == "decimal":
            return decimal.Decimal(value)
        if cls == "integer":
            return int(value)
        if cls == "float":
            return float(value)
        if cls == "boolean":
            return value.lower() in ("t", "true", "1", "y", "yes", "on")
    except (ValueError, decimal.InvalidOperation):
        return value
    return value


def _text(cls, value):
    if value is None:
        return None
    try:
        return canon.render_value(cls, _typed(cls, value))
    except Exception:  # noqa: BLE001 - compared as it prints
        return str(value)


def _differs(classes, a, b):
    """Whether two rows differ on the columns both have, as the check
    renders them."""
    names = [n for n in a if n in b and n in classes]
    return [n for n in names
            if _text(classes[n], a[n]) != _text(classes[n], b[n])]


def _ident(c):
    return (c["table"], tuple(sorted((k, repr(v))
                                     for k, v in c["key"].items())))


def _number(cls, value):
    if value is None:
        return None
    return _typed(cls, value if isinstance(value, str) else str(value))


def _order(cls, a, b):
    """1 where `a` is the greater, -1 where `b` is, 0 where neither: typed
    first (`_typed`), as rendered where the two do not compare."""
    try:
        x, y = _typed(cls, a), _typed(cls, b)
        return (x > y) - (x < y)
    except TypeError:
        x, y = _text(cls, a), _text(cls, b)
        return (x > y) - (x < y)


# ---- sets ---------------------------------------------------------------------

def _collection(engine, cls, declared):
    """"set" for a column holding a set as comma-separated text (MySQL's
    SET), "" for an array or a JSON column, None for a column that holds
    no members."""
    if str(declared or "").strip().lower().startswith("set"):
        return "set"
    if canon.array_element(engine, declared) is not None or cls == "json":
        return ""
    return None


def _array_literal(text):
    """The members of an array as PostgreSQL prints one ({a,"b c",NULL}),
    one level deep: a nested array is no set of members."""
    body = text[1:-1]
    if not body.strip():
        return []
    out, cur, quoted, esc, was = [], [], False, False, False

    def one():
        got = "".join(cur)
        if not was and got.strip().upper() == "NULL":
            return None
        return got if was else got.strip()
    for ch in body:
        if esc:
            cur.append(ch)
            esc = False
        elif ch == "\\":
            esc = True
        elif ch == '"':
            quoted, was = not quoted, True
        elif ch == "," and not quoted:
            out.append(one())
            cur, was = [], False
        elif ch in "{}" and not quoted:
            raise ValueError("an array of arrays")
        else:
            cur.append(ch)
    out.append(one())
    return out


def _members(value, hint):
    """(members, the form they were written in) of a set, an array or a
    JSON array as a change or a row hands it over; ValueError where the
    value is none of them."""
    if value is None:
        return [], None
    if isinstance(value, (list, tuple, set, frozenset)):
        return list(value), "list"
    if isinstance(value, (bytes, bytearray, memoryview)):
        value = bytes(value).decode("utf-8")
    if isinstance(value, str):
        text = value.strip()
        if hint == "set":
            return [m for m in text.split(",") if m != ""], "set"
        if text.startswith("["):
            got = json.loads(text)
            if isinstance(got, list):
                return got, "json"
        if text.startswith("{") and text.endswith("}"):
            return _array_literal(text), "literal"
    raise ValueError("not a set of members")


def _member_key(m):
    """A member as one text, alike whichever form carried it: an array's
    printed `1` and a list's 1 are one member."""
    if m is None:
        return "\x00"
    if isinstance(m, bool):
        return "t" if m else "f"
    if isinstance(m, (dict, list)):
        return json.dumps(m, sort_keys=True, default=str)
    return str(m)


def _written(members, form):
    if form == "json":
        return json.dumps(members, ensure_ascii=False, default=str)
    if form == "set":
        return ",".join(str(m) for m in members)
    if form == "literal":
        return "{" + ",".join(
            "NULL" if m is None else
            '"' + str(m).replace("\\", "\\\\").replace('"', '\\"') + '"'
            for m in members) + "}"
    return members


def _union(before, remote, local, hint):
    """A set both sides changed, after both: a member either side added is
    in, one either side removed is out, the rest as before - the same from
    either end, its members in the order of their text and in the form the
    target holds them in."""
    b, _ = _members(before, hint)
    r, form_r = _members(remote, hint)
    lo, form_l = _members(local, hint)
    kb, kr, kl = ({_member_key(m): m for m in x} for x in (b, r, lo))
    keep = (kl.keys() & kr.keys()) | (kl.keys() - kb.keys()) \
        | (kr.keys() - kb.keys())
    if remote is None and local is None:
        return None
    every = {**kb, **kr, **kl}
    # a SET is written as its text whatever the driver read it as
    form = "set" if hint == "set" else (form_l or form_r or "list")
    return _written([every[k] for k in sorted(keep)], form)


# ---- settling a row --------------------------------------------------------------

@dataclass
class Settled:
    """What a row's conflict came to. `outcome`: `apply` (the changes as
    they are), `keep` (left out: the target's row stands), `made` (applied,
    the row made whole where it is missing), `merge` (the changes write
    the source's columns and the values settled, never the target's own
    - `drop` the columns they leave, `set` the values they write), `park`
    or `stop`. `decided` is each settled group, for the record."""
    kind: str = None
    outcome: str = "apply"
    decision: str = "apply"
    rule: str = ""
    drop: frozenset = frozenset()
    set: dict = None
    decided: list = None
    result: dict = None
    #: counters settled as values (by `fallback`): written, not added
    plain: frozenset = frozenset()


def _whole_row(kind, word, ranks):
    """(outcome, decision) of a row decided whole by `delete_vs_update` or
    `delete_missing`: which side's state of the row stands."""
    if word == "error":
        return "stop", "stop"
    if word == "park":
        return "park", "parked"
    if kind == "delete_missing":
        # nothing to delete: applied as it is, which writes nothing
        return "apply", "ignored"
    incoming = {"update": kind == "update_missing",
                "delete": kind == "delete_origin_differs",
                "overwrite": True, "ignore": False,
                "source_priority": bool(ranks) and ranks[0] > ranks[1]}[word]
    if not incoming:
        return "keep", "keep_local"
    return ("made", "apply") if kind == "update_missing" else \
        ("apply", "apply")


def settle(table_rules, classes, ranks, made, before, after, here,
           keys=(), hints=None):
    """What a batch's changes to one row come to, held to the target's row
    as it is now (`here`, None where it has none): `made` whether the
    batch's first change of it made it, `before` the row its first change
    says it was (None: no before image), `after` the row its last leaves
    (None: deleted); `keys` the key's columns, never settled, and `hints`
    each set column's form (`_collection`). Pure: the same arguments give
    the same answer at either end, which is what makes the two agree."""
    hints = hints or {}
    if made:
        if here is None:
            return Settled()
        if after is None:
            # the source made it and took it away again in one batch: the
            # target's own row stands, never deleted for a passing one
            return Settled("insert_exists", "keep", "keep_local",
                           table_rules.default.words())
        if not _differs(classes, after, here):
            return Settled()
        return _merge("insert_exists", table_rules, classes, ranks, None,
                      after, here, keys, hints)
    if here is None:
        if after is not None and before is not None \
                and not _differs(classes, after, before):
            # an update that changed nothing (PostgreSQL logs those) of a
            # row the target no longer has: nothing to make it again for
            return Settled(outcome="keep")
        kind = "delete_missing" if after is None else "update_missing"
        word = (table_rules.delete_missing if after is None
                else table_rules.delete_vs_update)
        outcome, decision = _whole_row(kind, word, ranks)
        return Settled(kind, outcome, decision, word)
    if before is None:
        # no before image: a row the target changed too is not seen
        return Settled(outcome="blind")
    if after is not None and not _differs(classes, after, here):
        # already as the change leaves it: applied before a stop and read
        # again, or both sides made the same change
        return Settled()
    if not _differs(classes, before, here):
        return Settled()
    if after is None:
        word = table_rules.delete_vs_update
        outcome, decision = _whole_row("delete_origin_differs", word, ranks)
        return Settled("delete_origin_differs", outcome, decision, word)
    return _merge("update_origin_differs", table_rules, classes, ranks,
                  before, after, here, keys, hints)


def _merge(kind, tr, classes, ranks, before, remote, local, keys, hints):
    """A row both sides changed, or both made (`before` None), settled a
    group of columns at a time: a group only one side changed is that
    side's, a group both changed its rule's."""
    facts = {}
    for c in remote:
        if c in keys or c not in local or c not in classes:
            continue
        cls = classes[c]
        r, lo = _text(cls, remote[c]), _text(cls, local[c])
        if before is None:
            ch_r = ch_l = r != lo
        elif c not in before:
            # the before image does not have it: written, as it always was
            ch_r, ch_l = r != lo, False
        else:
            b = _text(cls, before[c])
            ch_r, ch_l = r != b, lo != b
        facts[c] = (ch_r, ch_l, r == lo)
    took, decided, plain = {}, [], set()
    stop = park = False
    for c in list(facts):
        if c in took:
            continue
        cols, rule = tr.rule_of(c)
        present = [x for x in cols if x in facts]
        ch_r = any(facts[x][0] for x in present)
        ch_l = any(facts[x][1] for x in present)
        same = all(facts[x][2] for x in present)
        if not (ch_r and ch_l and not same):
            for x in present:
                took[x] = ("local", None) if facts[x][1] and \
                    not facts[x][0] else ("remote", None)
            continue
        side, value, used, tie = _group(rule, tr, present, classes, ranks,
                                        before, remote, local, hints)
        stop = stop or side == "stop"
        park = park or side == "park"
        if rule.name == "delta" and used.name != "delta":
            plain.update(present)
        for x in present:
            took[x] = (side, value)
        decided.append({"columns": present, "rule": used.words(),
                        "took": side, "tie": tie})
    if stop or park:
        return Settled(kind, "stop" if stop else "park",
                       "stop" if stop else "parked",
                       tr.default.words(), decided=decided)
    drop, sets, writes = set(), {}, False
    result = dict(local)
    for c, (side, value) in took.items():
        cls = classes[c]
        ch_r, _, same = facts[c]
        if side == "value":
            if _text(cls, value) == _text(cls, local[c]):
                side = "local"
            elif _text(cls, value) == _text(cls, remote[c]):
                side = "remote"
        if side == "remote":
            result[c] = remote[c]
            writes = writes or not same
        elif side == "local":
            if not same:
                drop.add(c)
        elif side == "value":
            result[c] = sets[c] = value
            writes = True
        else:
            # added where the target's row stands
            result[c] = _number(cls, local[c]) + (
                _number(cls, remote[c]) - _number(cls, before[c]))
            writes = writes or ch_r
    counted = decided and all(d["took"] == "added" for d in decided)
    if not drop and not sets:
        outcome, decision = "apply", ("added" if counted else "apply")
    elif not writes:
        outcome, decision = "keep", "keep_local"
    else:
        outcome = "merge"
        decision = "added" if counted and not drop else "merged"
    return Settled(kind, outcome, decision, tr.default.words(),
                   frozenset(drop), sets, decided, result, frozenset(plain))


def _group(rule, tr, cols, classes, ranks, before, remote, local, hints):
    """(side, value, the rule used, whether it was a tie) for one group
    both sides changed: side `remote`, `local`, `value` (a set's merged
    members), `added` (a counter's), `park` or `stop`."""
    name = rule.name
    if name == "error":
        return "stop", None, rule, False
    if name == "park":
        return "park", None, rule, False
    if name == "overwrite":
        return "remote", None, rule, False
    if name == "ignore":
        return "local", None, rule, False
    if name == "source_priority":
        return ("remote" if ranks[0] > ranks[1] else "local"), None, rule, \
            False
    if name == "delta":
        c = cols[0]
        if before is None or classes.get(c) not in NUMERIC or None in (
                before.get(c), remote.get(c), local.get(c)):
            # a row both made, a counter that is no number, or one a side
            # emptied: no addition to keep, so the table's rule says
            return _group(tr.fallback, tr, cols, classes, ranks, before,
                          remote, local, hints)
        return "added", None, rule, False
    if name == "union":
        c = cols[0]
        try:
            if c not in hints:
                raise ValueError("no members")
            value = _union((before or {}).get(c), remote.get(c),
                           local.get(c), hints[c])
        except (ValueError, TypeError):
            return _group(tr.fallback, tr, cols, classes, ranks, before,
                          remote, local, hints)
        return "value", value, rule, False
    by = rule.by or cols[0]
    cls = classes.get(by, "text")
    a, b = remote.get(by), local.get(by)
    if (a is None) != (b is None):
        # a value is later, higher and lower than no value alike
        return ("remote" if b is None else "local"), None, rule, False
    order = 0 if a is None else _order(cls, a, b)
    if name == "min":
        order = -order
    if order:
        return ("remote" if order > 0 else "local"), None, rule, False
    return _tie(cols, classes, ranks, remote, local), None, rule, True


def _tie(cols, classes, ranks, remote, local):
    """The side a tie goes to, the same from both ends: the one ranked
    higher, else the larger of the two versions as they render."""
    if ranks:
        return "remote" if ranks[0] > ranks[1] else "local"
    a = tuple(_text(classes.get(c, "text"), remote.get(c)) or ""
              for c in cols)
    b = tuple(_text(classes.get(c, "text"), local.get(c)) or ""
              for c in cols)
    return "remote" if a >= b else "local"


# ---- a batch --------------------------------------------------------------------

def _moved(c):
    """The key an update moves its row to, or None where it stays: judged
    as the applier judges it (`Engine._collapsed`)."""
    if c.get("op") != "update":
        return None
    key, values = c["key"], c.get("values") or {}
    if any(k not in values for k in key):
        return None
    if not any(values[k] != v for k, v in key.items()):
        return None
    return {k: values[k] for k in key}


def _touched(changes):
    """{ident: (table, key, [(index, role)])} in the order rows were first
    touched: role `op` (the change's own), `gone` (an update moved the row
    away from this key) or `came` (moved to it)."""
    rows, order = {}, []

    def add(table, key, i, role):
        ident = _ident({"table": table, "key": key})
        if ident not in rows:
            rows[ident] = (table, dict(key), [])
            order.append(ident)
        rows[ident][2].append((i, role))
    for i, c in enumerate(changes):
        to = _moved(c)
        if to is None:
            add(c["table"], c["key"], i, "op")
        else:
            add(c["table"], c["key"], i, "gone")
            add(c["table"], to, i, "came")
    return rows, order


def _net(changes, events):
    """(made, before, after) of one row across a batch: whether its first
    change made it, the row that change says it was, and the row the last
    leaves (None: deleted)."""
    first, role = events[0]
    c0 = changes[first]
    made = role == "came" or (role == "op" and c0["op"] == "insert")
    before = None if made else c0.get("before")
    cur = None if made else dict(before or {})
    for i, role in events:
        c = changes[i]
        if role == "gone" or (role == "op" and c["op"] == "delete"):
            cur = None
        elif role == "came" or c["op"] == "insert":
            cur = dict(c.get("values") or {})
        else:
            cur = {**(cur or {}), **(c.get("values") or {})}
    return made, before, cur


def _facts(dst, db, table, tr):
    """(columns as (name, class), {name: class}, {set column: form hint})
    of a target table, its rules held to it: a column the hop names a
    counter has to be a number, one it names a set has to hold members,
    and a column a rule compares has to be there. A column only the
    default makes a counter or a set, and is none, is settled by the
    table's `fallback` instead."""
    got = dst.neutral_columns("dst", db, table)
    engine = getattr(dst, "CANON_ENGINE", "")
    columns = [(n, canon.type_class(engine, t) or "text") for n, t in got]
    classes = dict(columns)
    declared = dict(got)
    hints = {}
    for n, cls in columns:
        rule = tr.rule_of(n)[1]
        if rule.name == "delta" and cls not in NUMERIC and tr.named(n):
            raise SystemExit(
                f"two_way.delta: {table}.{n} is {cls}, not a number - a"
                " counter is added to on both sides, so it has to be one")
        if rule.name == "union":
            form = _collection(engine, cls, declared.get(n))
            if form is None and tr.named(n):
                raise SystemExit(
                    f"two_way: union keeps both sides' members, and"
                    f" {table}.{n} is {declared.get(n)} - a set, an array or"
                    " a JSON array holds members")
            if form is not None:
                hints[n] = form
    for cols, rule in tr.groups:
        if rule.by and rule.by not in classes:
            raise SystemExit(f"two_way: {rule.words()} on {table} compares"
                             f" {rule.by}, which {table} does not have")
    return columns, classes, hints


def _key_text(classes, key, names):
    return tuple(_text(classes.get(k, "text"), key[k]) for k in names)


def resolve(pair, db, changes, log):
    """The changes to apply, each row held to the target's row as it is
    now (`settle`): a column only the target changed left out of what is
    written, one both changed settled by its rule, a counter moved by
    what the source added to it, a row the rules keep left out, a parked
    row's changes held - and a conflict under `error` stops the tail
    before anything of the batch is applied. Every conflict is recorded
    (`_Book`).

    A row whose changes are all deletes migkit made of it leaving the
    hop's row filter (`canon.FILTERED`) is applied as it is: the source
    deleted nothing, so there is nothing of the target's for it to
    conflict with - read as one, a row the target never had was a
    `delete_missing` logged, and under `error` or `park` stopped or
    parked the tail."""
    hop = pair.hop
    rules_ = rules(hop)
    ranks = rules_.ranks
    dst = pair.dst_engine
    rows, order = _touched(changes)
    filtered = {ident for ident in order
                if all(changes[i].get(canon.FILTERED)
                       for i, _ in rows[ident][2])}
    tables = {}
    for ident in order:
        table = rows[ident][0]
        if table not in tables:
            tr = rules_.table(table)
            tables[table] = (tr,) + _facts(dst, db, table, tr)
    book = _Book(pair, db)
    parked = _Parked.of(pair, db)
    heres = {}
    for table, (tr, columns, classes, hints) in tables.items():
        idents = [i for i in order if rows[i][0] == table
                  and i not in filtered]
        if not idents:
            continue
        keynames = sorted(rows[idents[0]][1])
        keys = [tuple(rows[i][1][k] for k in keynames) for i in idents]
        got = dst.neutral_rows_by_key("dst", db, table, columns, keynames,
                                      keys)
        names = [n for n, _ in columns]
        for i in idents:
            row = got.get(_key_text(classes, rows[i][1], keynames))
            heres[i] = dict(zip(names, row)) if row is not None else None
    fates, blind, stops, seen = {}, False, [], set()
    for ident in order:
        if ident in filtered:
            fates[ident] = Settled()
            continue
        table, key, events = rows[ident]
        tr, columns, classes, hints = tables[table]
        keynames = sorted(key)
        at = json.dumps(list(_key_text(classes, key, keynames)))
        seen.add((table, at))
        made, before, after = _net(changes, events)
        here = heres[ident]
        fates[ident] = Settled(outcome="keep")
        if parked.holds(table, at):
            # set aside until decided: held, and let go once the target's
            # row is what the source's last change leaves
            book.add(table, key, parked.kind(table, at), "held", "park",
                     events, changes, before, after, here, None, None)
            parked.hold(table, at, classes, after)
            if parked.level(table, at, classes, here):
                parked.release(table, at)
                book.released += 1
                log(f"two_way: {table} {_shown_key(hop, table, key)} is the"
                    " same on the target as the source's last change left"
                    " it - its changes go on")
            continue
        got = settle(tr, classes, ranks, made, before, after, here,
                     set(keynames), hints)
        if got.outcome == "blind":
            blind = True
            got = Settled()
        fates[ident] = got
        if got.kind is None:
            continue
        book.add(table, key, got.kind, got.decision, got.rule, events,
                 changes, before, after, here, got.decided, got.result)
        if got.outcome == "park":
            parked.park(table, at, key, got.kind, classes, after)
            book.parked += 1
        elif got.outcome == "stop":
            stops.append((table, key, got))
    parked.releasing(dst, db, tables, rules_, book, log, hop, seen)
    parked.save()
    if stops:
        book.close(log)
        table, key, got = stops[0]
        what = ", ".join(d["columns"][0] if len(d["columns"]) == 1
                         else "(" + ", ".join(d["columns"]) + ")"
                         for d in got.decided or [] if d["took"] == "stop")
        if got.kind in ("update_missing", "delete_origin_differs"):
            what = "delete_vs_update"
        elif got.kind == "delete_missing":
            what = "delete_missing"
        why = {"update_missing": "the target has no such row",
               "delete_missing": "the target has no such row",
               "delete_origin_differs": "the target changed the row the"
                                        " source deleted"}.get(
            got.kind, "the target's row changed there too")
        raise SystemExit(
            f"two_way: {got.kind} on {table} {_shown_key(hop, table, key)}:"
            f" {why}, and the rule for {what} is error. Both versions are"
            " in conflicts.jsonl; decide the row on both sides, then run the"
            " tail again")
    if blind and not getattr(pair, "_said_blind", False):
        pair._said_blind = True
        log("two_way: the source keeps no whole previous row for some"
            " changes, so a row the target changed too is found only when"
            " it is missing or made twice - keep full row images on the"
            " source (binlog_row_image FULL, REPLICA IDENTITY FULL)")
    left = sum(1 for g in fates.values() if g.outcome in ("keep", "park"))
    if left:
        log(f"two_way: {left} row(s) left as the target has them - its"
            " version kept, or set aside until decided (conflicts.jsonl)")
    book.close(log)
    return _rewritten(changes, rows, order, fates, tables, log, pair)


def _rewritten(changes, rows, order, fates, tables, log, pair):
    """The batch as each row's fate says, in the batch's order: a row
    left out loses its changes, a row merged has its changes write only
    what it settled, a row made again gets its whole row, a counter goes
    on as what it added - and an update that moved a key, its two ends
    settled apart, becomes the half that goes on."""
    gone, came, act, plain = {}, {}, {}, {}
    for ident in order:
        table, key, events = rows[ident]
        got = fates[ident]
        keeps = got.outcome in ("keep", "park")
        first = True
        for i, role in events:
            c = changes[i]
            if role == "gone":
                gone[i] = not keeps
                continue
            plain[i] = got.plain
            values = c.get("values") or {}
            if got.outcome == "merge":
                # the target's own columns kept where its row stands, not
                # left out: an upsert that leaves a NOT NULL column out is
                # refused before it finds the row (`canon.Kept`)
                values = {n: (canon.Kept(got.result[n]) if n in got.drop
                              else got.set[n] if n in (got.set or {})
                              else v) for n, v in values.items()}
            if role == "came":
                came[i] = (not keeps, values)
                continue
            if keeps or (got.outcome == "merge" and c["op"] == "delete"):
                act[i] = None
                continue
            if got.outcome == "made" and first and c["op"] == "update" \
                    and c.get("before"):
                values = {**c["before"], **values}
            first = False
            act[i] = dict(c, values=values)
    out = []
    for i, c in enumerate(changes):
        if i in gone:
            a, values = came.get(i, (True, c.get("values") or {}))
            if gone[i] and a:
                out.append((i, dict(c, values=values)))
            elif a:
                # the row's old place kept as the target has it: the row
                # arrives at its new key without leaving the old one
                out.append((i, {**{k: v for k, v in c.items()
                                   if k != "before"}, "op": "insert",
                                "key": {k: values.get(k, c["values"][k])
                                        for k in c["key"]},
                                "values": values}))
            elif gone[i]:
                out.append((i, {**c, "op": "delete", "values": {}}))
            continue
        got = act.get(i, c)
        if got is not None:
            out.append((i, got))
    final = []
    for i, c in out:
        tr, _, classes, _ = tables[c["table"]]
        counters = tr.counters(c.get("values") or {}, c["key"], classes) \
            - plain.get(i, frozenset())
        if counters and c["op"] == "update":
            c = _as_added(c, counters, classes, log, pair)
        final.append(c)
    return final


def _as_added(c, counters, classes, log, pair):
    """An update with each counter it moved as what it added (after less
    before), applied where the target's row stands (`canon.Added`). A
    change with no whole previous row cannot say what it added: written
    as it ends, and said once."""
    before = c.get("before")
    values = dict(c.get("values") or {})
    moved = False
    for n in sorted(counters):
        if n not in values or values[n] is None \
                or isinstance(values[n], canon.Added):
            continue
        if before is None or before.get(n) is None:
            if not getattr(pair, "_said_counter_blind", False):
                pair._said_counter_blind = True
                log("two_way.delta: a change without the row as it was"
                    " before cannot say what it added to a counter, so it"
                    " is written as it ends - keep full row images on the"
                    " source")
            continue
        cls = classes.get(n, "decimal")
        to = _number(cls, values[n])
        by = to - _number(cls, before[n])
        values[n] = canon.Added(by, to)
        moved = True
    return dict(c, values=values) if moved else c


def _shown_key(hop, table, key):
    """A row's key as a message may show it: masked where the hop masks
    the table's keys."""
    from . import masking
    if masking.keys(hop, table):
        return {k: masking.token(hop, v) for k, v in key.items()}
    return key


# ---- parked rows --------------------------------------------------------------

class _Parked:
    """The rows a conflict set aside (`park`), kept beside the tail's
    position (`conflicts-parked.json`, sealed where the hop keeps values
    so): {table: {key as rendered: {"key", "kind", "since", "held",
    "last"}}}, `last` the row the source's last change left, as rendered
    (None: deleted). Every change of a parked row is held - the source's
    log moves on past it, and it is recorded - until the target's row is
    `last`: an operator has decided it on both sides, and what the source
    held meanwhile is in the row they decided. A batch read again after a
    stop finds its rows parked as they were, and holds them again."""

    def __init__(self, path):
        self.path = path
        try:
            got = json.loads(path.read_text())
        except (OSError, ValueError):
            got = {}
        self.rows = got.get("rows") if isinstance(got, dict) else None
        self.rows = self.rows if isinstance(self.rows, dict) else {}
        self.changed = False

    @classmethod
    def of(cls, pair, db):
        kept = pair.__dict__.setdefault("_twoway_parked", {})
        if db not in kept:
            kept[db] = cls(pair.hop.report_dir(db) / PARKED)
        return kept[db]

    def holds(self, table, at):
        return at in (self.rows.get(table) or {})

    def kind(self, table, at):
        return self.rows[table][at].get("kind")

    @staticmethod
    def _last(classes, after):
        if after is None:
            return None
        return {n: _text(classes.get(n, "text"), v)
                for n, v in after.items() if n in classes}

    def park(self, table, at, key, kind, classes, after):
        import time
        # the key as it is (a number stays one, to be asked for by it);
        # what JSON has no form for is kept as its text
        self.rows.setdefault(table, {})[at] = {
            "key": dict(key), "kind": kind, "since": time.time(),
            "held": 1, "last": self._last(classes, after)}
        self.changed = True

    def hold(self, table, at, classes, after):
        entry = self.rows[table][at]
        entry["held"] = int(entry.get("held") or 0) + 1
        entry["last"] = self._last(classes, after)
        self.changed = True

    def level(self, table, at, classes, here):
        """Whether the target's row is what the source's last change left:
        both gone, or alike on every column both have."""
        last = self.rows[table][at].get("last")
        if last is None or here is None:
            return last is None and here is None
        return all(_text(classes.get(n, "text"), here[n]) == t
                   for n, t in last.items() if n in here)

    def release(self, table, at):
        self.rows[table].pop(at, None)
        if not self.rows[table]:
            self.rows.pop(table)
        self.changed = True

    def releasing(self, dst, db, tables, rules_, book, log, hop, seen):
        """The parked rows no change of this batch touched (`seen`: those
        it did), released where the target's row is now what the source
        left - read by key, a statement a table."""
        for table in list(self.rows):
            entries = self.rows[table]
            ats = [a for a in entries if (table, a) not in seen]
            if not ats:
                continue
            if table in tables:
                _, columns, classes, _ = tables[table]
            else:
                try:
                    tr = rules_.table(table)
                    columns, classes, _ = _facts(dst, db, table, tr)
                except Exception:  # noqa: BLE001 - asked again next batch
                    continue
            keynames = sorted(entries[ats[0]]["key"])
            keys = [tuple(entries[a]["key"][k] for k in keynames)
                    for a in ats]
            try:
                got = dst.neutral_rows_by_key("dst", db, table, columns,
                                              keynames, keys)
            except Exception:  # noqa: BLE001 - asked again next batch
                continue
            names = [n for n, _ in columns]
            for a in ats:
                row = got.get(tuple(json.loads(a)))
                here = dict(zip(names, row)) if row is not None else None
                if self.level(table, a, classes, here):
                    key = entries[a]["key"]
                    self.release(table, a)
                    book.released += 1
                    log(f"two_way: {table} {_shown_key(hop, table, key)} is"
                        " the same on the target as the source's last"
                        " change left it - its changes go on")

    def counts(self):
        return {t: len(e) for t, e in self.rows.items() if e}

    def save(self):
        import os
        if not self.changed:
            return
        tmp = self.path.with_name(self.path.name + ".tmp")
        tmp.write_text(json.dumps({"rows": self.rows}, default=str))
        os.replace(tmp, self.path)
        self.changed = False


# ---- the record ------------------------------------------------------------------

class _Book:
    """A batch's conflicts, written when the batch is settled: each to
    `conflicts.jsonl` (the row before, both versions, the rule of each
    column and what it took, what the row ended as), their counts to
    `conflicts-counts.json` for `/metrics`, a line with their count and
    the digest of what was written to the hop's record (`audit`), and the
    hop's receivers told (`notify.conflicts`)."""

    def __init__(self, pair, db):
        self.pair, self.db = pair, db
        self.lines = []
        self.kinds = Counter()
        self.counts = Counter()
        self.parked = 0
        self.released = 0

    def add(self, table, key, kind, decision, rule, events, changes,
            before, after, here, decided, result):
        from . import session
        ops = [changes[i]["op"] if role == "op" else f"update ({role})"
               for i, role in events]
        self.lines.append(json.dumps({
            "at": datetime.datetime.now().isoformat(timespec="seconds"),
            "session": session.current(), "hop": self.pair.hop.name,
            "db": self.db, "table": table, "key": key, "kind": kind,
            "op": ops[-1], "ops": ops, "policy": rule,
            "decision": decision, "rules": decided or [],
            "local": here, "remote": after, "remote_before": before,
            "result": result}, default=str))
        self.kinds[kind] += 1
        self.counts[f"{table}\t{kind}\t{decision}"] += 1

    def close(self, log):
        from . import audit
        hop, db = self.pair.hop, self.db
        if not self.lines and not self.released:
            return
        where = hop.report_dir(db)
        text = "".join(line + "\n" for line in self.lines)
        if text:
            with (where / RECORD).open("a") as f:
                f.write(text)
        self._count(where)
        if self.lines:
            audit.append(hop.report_dir() / "changelog.jsonl", {
                "op": "two-way-conflicts", "db": db,
                "rows": len(self.lines), "kinds": dict(self.kinds),
                "parked": self.parked, "released": self.released,
                "record": RECORD,
                "sha256": hashlib.sha256(text.encode()).hexdigest()})
        try:
            from . import notify
            notify.conflicts(hop, db, dict(self.kinds), self.parked, log)
        except Exception:  # noqa: BLE001 - the record stands; told next
            pass
        self.lines, self.kinds, self.counts = [], Counter(), Counter()

    def _count(self, where):
        import os
        path = where / COUNTS
        try:
            got = json.loads(path.read_text())
        except (OSError, ValueError):
            got = {}
        counts = Counter(got.get("counts") or {})
        counts.update(self.counts)
        parked = _Parked.of(self.pair, self.db)
        tmp = path.with_name(path.name + ".tmp")
        tmp.write_text(json.dumps({"counts": dict(counts),
                                   "parked": parked.counts()}))
        os.replace(tmp, path)


# ---- the pair -------------------------------------------------------------------

def _mirrors(a, b, ra, rb):
    """Whether rule `a` on one hop and `b` on the hop back settle one
    conflict to one row: `ra`, `rb` their ranks."""
    if a.name in MIRROR:
        return b.name == MIRROR[a.name]
    if a.name == "source_priority":
        return b.name == "source_priority" and bool(ra) and bool(rb) and \
            (ra[0] > ra[1]) == (rb[1] > rb[0])
    if a.name in ("error", "park"):
        return b.name in ("error", "park")
    return a.name == b.name and str(a.by or "").lower() == \
        str(b.by or "").lower()


def _needed(a, ra):
    if a.name in MIRROR:
        return MIRROR[a.name]
    if a.name == "source_priority":
        return ("source_priority with the ranks turned round"
                f" (source_rank {ra[1]}, target_rank {ra[0]})" if ra
                else "source_priority")
    return a.words()


def apart(one, other):
    """What two hops of one pair - each the other's way back - would settle
    apart, in words; [] where every conflict ends alike on both sides."""
    try:
        a = Rules(one, _ranks_of(one))
        b = Rules(other, _ranks_of(other))
    except SystemExit:
        # each hop's own check says what is wrong with it
        return []
    out = []
    leaves = sorted({t.lower().rpartition(".")[2]
                     for t in list(a.tables) + list(b.tables)})
    ties = False
    for leaf in [None] + leaves:
        ta = a.table(leaf) if leaf else a.all
        tb = b.table(leaf) if leaf else b.all
        where = f"two_way.tables.{leaf}" if leaf else "two_way"
        columns = sorted({c for cols, _ in ta.groups + tb.groups
                          for c in cols})
        for c in [None] + columns:
            ga, xa = ta.rule_of(c) if c else ((), ta.default)
            gb, xb = tb.rule_of(c) if c else ((), tb.default)
            at = f"{where}.columns.{c}" if c else f"{where} default"
            if c and set(ga) != set(gb):
                out.append(f"{at}: settled with {', '.join(ga)} one way"
                           f" and with {', '.join(gb)} the way back - the"
                           " two sides would settle it apart")
                continue
            ties = ties or xa.name in COMPARING
            if not _mirrors(xa, xb, a.ranks, b.ranks):
                out.append(f"{at}: {xa.words()} one way and {xb.words()}"
                           " the way back, so the two sides would settle"
                           " one conflict apart - the way back needs"
                           f" {_needed(xa, a.ranks)}")
        da, db_ = ta.delete_vs_update, tb.delete_vs_update
        if not _mirrors(Rule(da), Rule(db_), a.ranks, b.ranks) \
                and not (da == db_ and da in ("update", "delete")):
            out.append(f"{where}.delete_vs_update: {da} one way and {db_}"
                       " the way back - a row deleted on one side and"
                       " changed on the other would end apart")
    if ties and (a.ranks is None) != (b.ranks is None):
        out.append("two_way.source_rank: one hop ranks the two sides and"
                   " the other does not, so a tie is settled by rank one"
                   " way and by value the way back - rank them on both,"
                   " the same side higher")
    elif ties and a.ranks and b.ranks and \
            (a.ranks[0] > a.ranks[1]) != (b.ranks[1] > b.ranks[0]):
        out.append("two_way.source_rank: the two hops rank different sides"
                   " higher, so a tie goes to each hop's own source - rank"
                   " the same side higher on both")
    return out
