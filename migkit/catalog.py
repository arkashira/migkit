"""What one side's catalogue says about its tables, in one shape every
reader and every comparer meets on (audit 2026-10-01, SC3, SC15).

Drift was a table's column names with a type string each, so a
`varchar(50)` widened to `varchar(100)`, a column made NOT NULL, a
default, an index, a constraint or a collation changed on the source
read as no change at all - and resume, the cutover's gate and the tail's
DDL gate lean on drift. And the catalogue was read a table at a time,
columns twice, keys twice, indexes twice: about 16,000 connections or
programs started for a `check` of 2,000 tables (estimate from the code).

So a side's catalogue is one `Catalog`, read in one pass by each engine:
its tables; for each, its columns as `Column(name, declared, cls,
nullable, default, collation, extra)` in the server's order; its key; its
indexes; its foreign keys; and the rest the server says of it in its own
words (`others`: a check, a trigger). `shape()` is `{table: fingerprint}`,
the hash of everything the server says about the table - never of
migkit's own reading of it (`cls`), so a newer migkit does not see drift
in a table nobody touched. `Shape.changes` says which tables differ, and
`Catalog.explain` what differs in one, in words - the one explanation
drift, the resume, the cutover's gate and a schema check across a hop's
two sides give (`drift`).
"""
import hashlib
import json
from dataclasses import dataclass, field
from typing import NamedTuple


class Column(NamedTuple):
    """One column as the server declares it; `cls` is migkit's class of
    it (`canon`), "" where not read."""
    name: str
    declared: str
    cls: str = ""
    nullable: bool = True
    default: str = None
    collation: str = None
    #: what else the server declares of it, in its words - an identity,
    #: a computed expression, an alias type - "" where nothing
    extra: str = ""


@dataclass(frozen=True)
class Index:
    name: str
    columns: tuple
    unique: bool = False
    #: the server's own definition where it has one (a partial index's
    #: predicate, an expression), "" where the columns say it all
    definition: str = ""


@dataclass(frozen=True)
class ForeignKey:
    name: str
    columns: tuple
    ref_table: str
    ref_columns: tuple
    on_delete: str = ""
    on_update: str = ""


def _canon_index(ix):
    # the name left out: renaming an index changes nothing a row or a
    # read depends on
    return [list(ix.columns), bool(ix.unique), ix.definition or ""]


def _canon_fk(fk):
    return [list(fk.columns), fk.ref_table, list(fk.ref_columns),
            (fk.on_delete or "").lower(), (fk.on_update or "").lower()]


def _canon_column(c):
    # `extra` only where there is one: a table read without it hashes as
    # it did
    return [c.name, str(c.declared or "").strip().lower(), bool(c.nullable),
            None if c.default is None else str(c.default),
            None if c.collation is None else str(c.collation)] + (
        [str(c.extra)] if c.extra else [])


def _said(what, x):
    """An index or a foreign key as `explain` names it: its name where it
    has one, and what it is over."""
    over = f"({', '.join(x.columns)})"
    if what == "foreign key":
        over += f" -> {x.ref_table} ({', '.join(x.ref_columns)})"
        for word, action in (("delete", x.on_delete), ("update", x.on_update)):
            if action and action.lower() not in ("no action", "no_action"):
                over += f" on {word} {action.lower()}"
    else:
        if x.unique:
            what = f"unique {what}"
        if x.definition:
            over += f" {x.definition}"
    return f"{what} {x.name + ' ' if x.name else ''}{over}"


class Shape(dict):
    """`{table: fingerprint}` of one side at one time."""

    def changes(self, other):
        """{"created": [...], "dropped": [...], "altered": [...]} from this
        shape to `other`, each sorted."""
        return {"created": sorted(set(other) - set(self)),
                "dropped": sorted(set(self) - set(other)),
                "altered": sorted(t for t in set(self) & set(other)
                                  if self[t] != other[t])}

    def changed(self, other):
        """Every table whose shape differs: altered, created or dropped."""
        got = self.changes(other)
        return set(got["created"]) | set(got["dropped"]) | set(got["altered"])


@dataclass
class Catalog:
    tables: list = field(default_factory=list)
    #: {table: [Column]} in the server's order
    columns: dict = field(default_factory=dict)
    #: {table: (column, ...)} - the primary key, in key order
    keys: dict = field(default_factory=dict)
    #: {table: [Index]}
    indexes: dict = field(default_factory=dict)
    #: {table: [ForeignKey]}
    fks: dict = field(default_factory=dict)
    #: {table: [text]} - the rest the server says of a table, each part in
    #: its words (a check with its expression, a trigger with a hash of
    #: its body), compared as a set
    others: dict = field(default_factory=dict)

    def __post_init__(self):
        known = list(self.tables)
        for t in list(self.columns) + list(self.keys) + list(
                self.indexes) + list(self.fks) + list(self.others):
            if t not in known:
                known.append(t)
        self.tables = known

    @classmethod
    def from_columns(cls, columns):
        """A catalogue from `{table: [(name, declared), ...]}`, what an
        engine's column catalogue answers today."""
        return cls(columns={t: [Column(str(c[0]), str(c[1])) for c in cols]
                            for t, cols in (columns or {}).items()})

    def _canon(self, table):
        got = {"columns": [_canon_column(c)
                           for c in self.columns.get(table) or []],
               "key": list(self.keys.get(table) or ()),
               "indexes": sorted(_canon_index(i)
                                 for i in self.indexes.get(table) or []),
               "fks": sorted(_canon_fk(f)
                             for f in self.fks.get(table) or [])}
        if self.others.get(table):
            got["others"] = sorted(str(o) for o in self.others[table])
        return got

    def fingerprint(self, table):
        """The hash of what the server says about `table`."""
        text = json.dumps(self._canon(table), sort_keys=True,
                          separators=(",", ":"))
        return hashlib.sha256(text.encode()).hexdigest()

    def shape(self):
        return Shape({t: self.fingerprint(t) for t in self.tables})

    def explain(self, other, table, engine=None, added="added",
                dropped="dropped"):
        """What differs in `table` from this catalogue to `other`, one
        phrase each; [] where nothing does. `engine`, where both are one
        engine's, names a narrower type with why (`verdict.narrowing`);
        `added` and `dropped` are the words for what only `other` has and
        what only this one has - a schema check across a hop's two sides
        says them its own way."""
        if table not in self.tables:
            return [added if added != "added" else "created"] \
                if table in other.tables else []
        if table not in other.tables:
            return [dropped]
        out = []
        was = {c.name: c for c in self.columns.get(table) or []}
        now = {c.name: c for c in other.columns.get(table) or []}
        out += [f"column {n} {added}" for n in now if n not in was]
        out += [f"column {n} {dropped}" for n in was if n not in now]
        for n in [n for n in was if n in now]:
            a, b = _canon_column(was[n]), _canon_column(now[n])
            if a[1] != b[1]:
                from .verdict import narrowing
                why = narrowing(engine, was[n].declared, now[n].declared) \
                    if engine else None
                out.append(f"column {n} {why}" if why else
                           f"column {n} {was[n].declared} ->"
                           f" {now[n].declared}")
            if a[2] != b[2]:
                out.append(f"column {n} "
                           + ("now allows null" if b[2] else "now NOT NULL"))
            if a[3] != b[3]:
                out.append(f"column {n} default {a[3]} -> {b[3]}")
            if a[4] != b[4]:
                out.append(f"column {n} collation {a[4]} -> {b[4]}")
            if (was[n].extra or "") != (now[n].extra or ""):
                out.append(f"column {n} {was[n].extra or 'plain'} ->"
                           f" {now[n].extra or 'plain'}")
        order_was = [n for n in was if n in now]
        order_now = [n for n in now if n in was]
        if order_was != order_now:
            out.append("columns reordered")
        if list(self.keys.get(table) or ()) != list(
                other.keys.get(table) or ()):
            out.append(f"key ({', '.join(self.keys.get(table) or ())}) ->"
                       f" ({', '.join(other.keys.get(table) or ())})")
        for what, mine, theirs, canon in (
                ("index", self.indexes, other.indexes, _canon_index),
                ("foreign key", self.fks, other.fks, _canon_fk)):
            a = {json.dumps(canon(x)): x for x in mine.get(table) or []}
            b = {json.dumps(canon(x)): x for x in theirs.get(table) or []}
            out += [f"{_said(what, b[k])} {added}"
                    for k in sorted(set(b) - set(a))]
            out += [f"{_said(what, a[k])} {dropped}"
                    for k in sorted(set(a) - set(b))]
        mine = set(self.others.get(table) or ())
        theirs = set(other.others.get(table) or ())
        out += [f"{o} {added}" for o in sorted(theirs - mine)]
        out += [f"{o} {dropped}" for o in sorted(mine - theirs)]
        return out

    def as_dict(self):
        """The catalogue as JSON takes it, to keep beside a run."""
        return {"tables": list(self.tables),
                "columns": {t: [list(c) for c in cols]
                            for t, cols in self.columns.items()},
                "keys": {t: list(k) for t, k in self.keys.items()},
                "indexes": {t: [[i.name, list(i.columns), i.unique,
                                 i.definition] for i in ixs]
                            for t, ixs in self.indexes.items()},
                "fks": {t: [[f.name, list(f.columns), f.ref_table,
                             list(f.ref_columns), f.on_delete, f.on_update]
                            for f in fks] for t, fks in self.fks.items()},
                "others": {t: sorted(o) for t, o in self.others.items()}}

    @classmethod
    def from_dict(cls, got):
        got = got or {}
        return cls(
            tables=list(got.get("tables") or []),
            columns={t: [Column(*c) for c in cols]
                     for t, cols in (got.get("columns") or {}).items()},
            keys={t: tuple(k) for t, k in (got.get("keys") or {}).items()},
            indexes={t: [Index(i[0], tuple(i[1]), bool(i[2]), i[3])
                         for i in ixs]
                     for t, ixs in (got.get("indexes") or {}).items()},
            fks={t: [ForeignKey(f[0], tuple(f[1]), f[2], tuple(f[3]), f[4],
                                f[5]) for f in fks]
                 for t, fks in (got.get("fks") or {}).items()},
            others={t: list(o) for t, o in
                    (got.get("others") or {}).items()})
