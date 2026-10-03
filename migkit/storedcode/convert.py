"""Stored code converted one routine at a time, by whichever candidate
proves itself on it (backlog R11).

Per routine the candidates (`rungs`) are tried in their order - the
operator's own file, the query translator, migkit's rules, the two open
converters, a model last - and each version is made in the target's
sandbox, gated and proved against the source's own routine run on the same
fixture (`prove`). The first that proves is chosen; one that proves with a
caveat (a branch never reached) is kept while the rest are tried for one
that proves outright. Callees are converted before their callers, so a
caller is proved over callees already proved.

Where no sandbox is named nothing new is chosen: the query translator's
version is written as it always was (views, functions of one expression)
and `check` proves it; anything else is written as a comment naming why it
is not converted, with the best version a candidate made kept under it,
commented out, for a person to start from. Nothing unproved is applied.

What comes out is sorted into three kinds (the residue): proved (the rung
and its evidence), converted and not proved, not converted (every
candidate's reason). All of it is kept in `code-conversion.json` beside
the hop's reports.
"""
import json
import re
from pathlib import Path

from sqlglot import exp

from .plpgsql import Converted
from .routines import Routine
from .translate import Refused, generate, parse

#: the line a statement a model proposed is marked with (kept as it was)
PROPOSED = "-- proposed by a model, not by the translator: check holds" \
           " it to the source's answers"


def localizer(pair, db):
    """The rewrite that makes a source query's table names the target's:
    the source's database named as a qualifier is dropped, a schema
    holding something the move carries is dropped, and each table goes by
    the name the hop's mapping gave it."""
    here = pair.src_engine._d("src", db) if hasattr(pair.src_engine, "_d") \
        else db
    views = pair.src_engine.neutral_views("src", db)
    carried = set(pair.src_engine.neutral_tables("src", db)) | \
        {n for n, _ in views}

    def local(tree):
        renamed = {}
        for t in tree.find_all(exp.Table):
            # a qualifier naming the source's own database, or a schema
            # holding something this move carries, is not a schema the
            # target has; any other stays, and fails there rather than
            # meaning a table of the same name
            if t.text("catalog") in ("", here):
                t.set("catalog", None)
                if t.text("db") == here or \
                        f"{t.text('db')}.{t.name}" in carried:
                    t.set("db", None)
            new = pair._leaf(pair._rename(t.name, db))
            if new != t.name and not t.alias:
                renamed[t.name] = new
            t.set("this", exp.to_identifier(new))
        for c in tree.find_all(exp.Column):
            if c.args.get("db"):
                c.set("db", None)
            if c.args.get("catalog"):
                c.set("catalog", None)
            if c.table in renamed:
                c.set("table", exp.to_identifier(renamed[c.table]))
        return tree
    return local


def code_dir(hop):
    """Where the operator's own versions are kept: `<name>.sql`, one per
    routine, in the hop option `code_dir` or `convert/<hop>` beside the
    hops file."""
    from .. import config
    named = (hop.options or {}).get("code_dir")
    return Path(named).expanduser() if named else \
        Path(config.CONF).parent / "convert" / hop.name


def by_hand(ctx, r):
    path = ctx["dir"] / f"{r.name}.sql"
    if not path.exists():
        raise Refused("no file of its own is written for it")
    from .programs import statements
    text = path.read_text()
    made = statements(text) if ctx["dst"] == "postgres" else [
        s for s in (x.strip() for x in text.split(";\n")) if s]
    if not made:
        raise Refused(f"{path.name} holds no CREATE statement")
    return Converted(made)


def _there(pair, declared, what):
    from .. import canon
    cls, _ = canon.comparable(pair.src_name, declared)
    if not cls:
        raise Refused(f"{what} is {declared}, which has no counterpart on"
                      f" {pair.dst_name}")
    return cls, canon.ddl_type(pair.dst_name, cls, canon.params(declared))


def translator(pair, ctx, r, fixes=None):
    """The query translator's version of a view, or of a function whose
    body is one expression: the translator alone, as it always was (with
    `fixes`, the rules' rewrites on top - which is the rules' version)."""
    src = pair.src_engine.SQL_DIALECT
    dst = pair.dst_engine.SQL_DIALECT
    local = ctx["local"]
    parting = ctx.get("parting")
    if r.kind == "view":
        tree = parse(r.body, src)
        if parting:
            from .builtins import refusal
            refusal(tree, parting, src)
        tree = local(tree)
        if fixes:
            fixes(tree)
        body = generate(tree, src, dst)
        return Converted([f"create view {r.name} as {body}"])
    one = r.settings.get("expression")
    if r.kind != "function" or one is None:
        raise Refused("its body is statements, not one expression")
    if not all(p.name for p in r.params):
        raise Refused("an argument has no name")
    typed = {p.name: _there(pair, p.type, f"argument {p.name}")
             for p in r.params}
    ret_cls, ret = _there(pair, r.returns, "what it returns")
    if parting:
        from .builtins import refusal
        refusal(parse(one, src), parting, src)

    def exact(node, cls, typ):
        # a decimal's declared scale is where its rounding happens;
        # PostgreSQL ignores it on a function's arguments and result, so
        # it is said in the body instead
        if cls != "decimal" or "(" not in typ:
            return node
        return exp.cast(node, exp.DataType.build(typ, dialect=dst))
    tree = local(parse(one, src))
    if fixes:
        fixes(tree)
    for col in list(tree.find_all(exp.Column)):
        if col.name in typed and not col.table:
            # quoted as the declaration quotes it, so a name in capitals
            # is the same name in both
            col.replace(exact(exp.column(col.name, quoted=True),
                              *typed[col.name]))
    tree = exact(tree, ret_cls, ret)
    body = generate(tree, src, dst)
    return Converted([pair.dst_engine.neutral_function_sql(
        r.name, [(p.name, typed[p.name][1]) for p in r.params], ret, body)])


def emulate(pair, ctx, r):
    """The query translator's version with every function the target's
    emulation of Oracle provides called there, by its schema - so a
    function PostgreSQL lacks, or has with another meaning (`add_months`,
    `months_between`, `instr`, Oracle's `substr`), answers as Oracle's
    does. The target keeps the emulation extension; `leftovers` names it
    and the verdict says a routine depends on it."""
    names = ctx.get("emulated") or set()
    if not names:
        raise Refused("the target's emulation provides no function")

    def fixes(tree):
        # every call by the name Oracle gives it - a function the
        # translator knows (NVL, SUBSTR, ADD_MONTHS) as well as one it
        # does not - called as the emulation's, with its arguments as
        # written
        for node in reversed(list(tree.find_all(exp.Func))):
            name = (node.name if isinstance(node, exp.Anonymous)
                    else node.sql("oracle").split("(")[0]).lower()
            if name not in names:
                continue
            args = node.expressions if isinstance(node, exp.Anonymous) \
                else [v for k in node.arg_types
                      for v in _listed(node.args.get(k))
                      if isinstance(v, exp.Expression)]
            node.replace(exp.Dot(
                this=exp.to_identifier("oracle"),
                expression=exp.Anonymous(this=name, expressions=[
                    a.copy() for a in args])))
    return translator(pair, ctx, r, fixes=fixes)


def _listed(value):
    return value if isinstance(value, list) else [value]


def rules(pair, ctx, r):
    """migkit's own rules: MySQL's and SQL Server's procedural code into
    PL/pgSQL, MySQL's meaning kept in a view or an expression, and
    PostgreSQL's procedural code into MySQL's."""
    src = pair.src_engine.SQL_DIALECT
    dst = pair.dst_engine.SQL_DIALECT
    if (src, dst) == ("postgres", "mysql"):
        from . import pg_mysql
        return pg_mysql.convert(r, ctx["rules"])
    if (src, dst) == ("oracle", "postgres"):
        from . import plsql_pg
        from .learn import apply
        made = plsql_pg.convert(r, ctx["rules"])
        made.statements = [apply(x, ctx.get("learned") or [])
                           for x in made.statements]
        return made
    if dst != "postgres" or src not in ("mysql", "tsql"):
        raise Refused(f"migkit's rules do not convert {src} to {dst}")
    from .learn import apply
    if src == "mysql":
        from . import mysql_pg
        if r.kind == "view" or (r.kind == "function" and
                                r.settings.get("expression") is not None
                                and r.body is None):
            codes = []
            made = translator(pair, ctx, r, fixes=lambda tree: mysql_pg.fix(
                tree, ctx["rules"], 1, lambda c, ln: codes.append(c)))
        else:
            made = mysql_pg.convert(r, ctx["rules"])
    else:
        from . import tsql_pg
        made = tsql_pg.convert(r, ctx["rules"])
    # the rules learned from fixes that proved, on this hop
    made.statements = [apply(x, ctx.get("learned") or [])
                       for x in made.statements]
    return made


def _order(routines):
    """Callees before their callers; triggers and events after both."""
    names = {r.name.lower(): r for r in routines}
    calls = {}
    for r in routines:
        body = r.body or r.settings.get("expression") or ""
        calls[r.name.lower()] = {
            n.lower() for n in re.findall(r"(?i)\b([A-Za-z_]\w*)\s*\(", body)
            if n.lower() in names and n.lower() != r.name.lower()} | {
            n.lower() for n in re.findall(r"(?i)\bcall\s+`?(\w+)", body)
            if n.lower() in names}
    done, out = set(), []
    # functions before the triggers that may call them, triggers before
    # the procedures whose writes fire them - so each is proved over the
    # target's versions of what it sets off
    rank = {"view": 0, "function": 1, "trigger": 2, "procedure": 3,
            "event": 4}
    todo = sorted(routines, key=lambda r: (rank.get(r.kind, 1), r.name))
    while todo:
        ready = [r for r in todo if not (calls[r.name.lower()] - done)] or \
            todo[:1]
        for r in ready:
            out.append(r)
            done.add(r.name.lower())
            todo.remove(r)
    return out


class Conversion:
    """One database's views and routines, converted."""

    def __init__(self, pair, db, propose=True):
        from . import mysql_pg
        from .prove import expressions, routines_of
        self.pair = pair
        self.db = db
        self.propose = propose
        self.src = pair.src_engine.SQL_DIALECT
        self.dst = pair.dst_engine.SQL_DIALECT
        self.local = localizer(pair, db)
        # a view's own statement, for the converters that read one
        views = [Routine(kind="view", name=pair._leaf(n), body=sql,
                         definition=f"CREATE VIEW {pair._leaf(n)} AS {sql}",
                         settings={"source": n})
                 for n, sql in pair.src_engine.neutral_views("src", db)]
        self.views = views
        self.routines = routines_of(pair.src_engine, "src", db)
        expr = expressions(pair.src_engine, "src", db)
        for r in self.routines:
            if r.name in expr:
                r.settings.setdefault("expression", expr[r.name])
        cols, tables, table_columns, keys = {}, {}, {}, {}
        column_types = {}
        try:
            for t in pair.dst_engine.neutral_tables("dst", db):
                leaf = pair._leaf(t)
                tables[leaf.lower()] = leaf
                declared = pair.dst_engine.neutral_columns("dst", db, t)
                names = [c for c, _ in declared]
                table_columns[leaf.lower()] = names
                column_types[leaf.lower()] = {str(c).lower(): str(ty)
                                              for c, ty in declared}
                for c in names:
                    cols.setdefault(c.lower(), c)
            if self.dst == "mysql":
                # which unique key a MySQL `ON DUPLICATE KEY` hits is the
                # target table's to say: its own keys, by its own names
                for t, ks in pair.dst_engine.unique_keys("dst", db).items():
                    keys[pair._leaf(t)] = ks
            else:
                for t, ks in pair.src_engine.unique_keys("src", db).items():
                    keys[pair._leaf(pair._rename(t, db))] = ks
        except Exception:  # noqa: BLE001 - converted without them
            pass
        emulations, emulated = set(), set()
        if pair.dst_engine.CANON_ENGINE == "postgres":
            try:
                from .sandbox import EMULATIONS
                emulations = {x for x in pair.dst_engine._psql(
                    "dst", db, "select extname from pg_extension").split()
                    if x in EMULATIONS}
                if emulations:
                    emulated = set(pair.dst_engine._psql(
                        "dst", db, "select distinct proname from pg_proc p"
                        " join pg_namespace n on n.oid = p.pronamespace"
                        " where n.nspname = 'oracle'").split())
            except Exception:  # noqa: BLE001 - none known
                pass
        from . import builtins
        self.ctx = {"local": self.local, "dst": self.dst,
                    "emulations": emulations, "emulated": emulated,
                    "parting": builtins.load(pair.hop, db)
                    if builtins.turned_on(pair.hop) else {},
                    "dir": code_dir(pair.hop),
                    "rules": mysql_pg.Context(
                        local=self.local, routines=self.routines,
                        columns=cols, keys=keys, tables=tables,
                        table_columns=table_columns)}
        # the target's declared column types, which PostgreSQL's rules
        # into MySQL read for a loop over a query and a column's %TYPE
        self.ctx["rules"].column_types = column_types
        # what PL/SQL's rules read: the functions the target's emulation
        # of Oracle holds, and the source's packages (schemas there)
        self.ctx["rules"].emulated = emulated
        self.ctx["rules"].packages = {
            r.settings["package"].lower() for r in self.routines
            if r.settings.get("package")}
        self.records = []
        #: why the sandbox named for the hop could not be used, if it was
        #: named and could not
        self.why_not = None
        self.used = False
        from .learn import Learned
        self.learned = Learned(pair.hop.report_dir() / "code-rules.json")
        self.ctx["learned"] = self.learned.adopted(f"{self.src}->{self.dst}")
        #: per routine, the rules' version and whether it proved, and the
        #: version chosen - what learning a rule is tried against
        self.rules_tried, self.accepted = {}, {}
        #: what holding the chosen versions to sequences of calls found
        self.sequences = None
        #: per routine, the PL/SQL converter's own estimate, and the
        #: seconds migkit spent converting and proving it (`effort`)
        self.costs, self.seconds = {}, {}

    # ---- without a sandbox: as it always was --------------------------------

    def unproved(self):
        out = []
        views, needs = [], {}
        for v in self.views:
            try:
                made = translator(self.pair, self.ctx, v)
                tree = self.local(parse(v.body, self.src))
                needs[v.name] = {t.name for t in tree.find_all(exp.Table)}
                views.append((v.name, made.statements[0] + ";", ""))
                self._record(v, "converted, not proved", "translator")
            except Refused as e:
                views.append(self._not_converted(v, str(e)))
        placed = set()
        while views:
            ready = [x for x in views
                     if not (needs.get(x[0], set()) & set(needs) - placed
                             - {x[0]})] or views[:1]
            for x in ready:
                out.append(x)
                placed.add(x[0])
                views.remove(x)
        for r in self.routines:
            try:
                made = translator(self.pair, self.ctx, r)
                out.append((r.name, made.sql, ""))
                self._record(r, "converted, not proved", "translator")
            except Refused as e:
                out.append(self._not_converted(r, str(e)))
        return out

    def _not_converted(self, r, why):
        """(name, comment, why): a model's proposal where one is allowed,
        as it always was; else the comment naming it, with the first
        version another candidate made kept under it, commented out."""
        if self.propose:
            from .. import assist
            try:
                definition = self.pair.src_engine.code_definition(
                    "src", self.db, r.kind, r.settings.get("source", r.name))
            except Exception:  # noqa: BLE001 - no definition, no proposal
                definition = None
            made = assist.propose(self.pair.src_name, self.pair.dst_name,
                                  r.kind, r.name, definition,
                                  hop=self.pair.hop)
            if made:
                self._record(r, "converted, not proved", "model")
                return (r.name, f"{made.rstrip().rstrip(';')}; {PROPOSED}",
                        "")
        head = f"-- {r.kind} {r.name} not converted: {why}"
        kept = self._offered(r)
        self._record(r, "not converted", None, reasons=[why])
        if kept:
            rung, made = kept
            why_not = self.why_not or ("no sandbox is named for this hop"
                                       " (proof_sandbox)")
            lines = [head, f"-- {rung} made a version of it, not proved:"
                     f" {why_not}; nothing unproved is applied:"]
            lines += ["-- " + ln for ln in made.sql.splitlines()]
            return (r.name, "\n".join(lines), why)
        return (r.name, head, why)

    def _offered(self, r):
        """The first version a candidate other than the translator and a
        model makes, unproved: (who made it, it) or None."""
        from .plpgsql import unparsed
        from .rungs import candidates
        facts = self.facts(r)
        for c in candidates(self.pair, r, self.ctx):
            if c.name in ("translator", "model") or c.lacks(facts):
                continue
            try:
                made = c.make(r)
            except Exception:  # noqa: BLE001 - refused, or broke
                continue
            # a version the target's own parser refuses is not offered
            if self.dst == "postgres" and unparsed(made.statements):
                continue
            return c.said, made
        return None

    def facts(self, r):
        one = r.settings.get("expression") if r.kind != "view" else r.body
        return {"src": self.src, "dst": self.dst, "kind": r.kind,
                "emulations": self.ctx.get("emulations"),
                "one": one, "file": (self.ctx["dir"] / f"{r.name}.sql")
                if (self.ctx["dir"] / f"{r.name}.sql").exists() else None}

    def _record(self, r, kind, rung, **more):
        self.records.append({"routine": f"{r.kind} {r.name}", "class": kind,
                             "rung": rung, **more})

    # ---- with a sandbox: every candidate proved -----------------------------

    def proved(self, proof):
        """Each routine by the candidate that proves itself on it."""
        from .rungs import Ledger, candidates
        ledger = Ledger(self.pair.hop.report_dir() / "code-rungs.json")
        out = []
        try:
            proof.begin([])
            self._source_views(proof)
            self.ctx["pg_version"] = _pg_version(proof.b)
            # the rules read it for what a target of that version runs
            # (MERGE, OUTPUT's rows before and after)
            self.ctx["rules"].pg_version = self.ctx["pg_version"]
            import time
            for r in self.views + _order(self.routines):
                began = time.monotonic()
                out.append(self._climb(proof, r, candidates(
                    self.pair, r, self.ctx), ledger))
                self.seconds[r.name] = time.monotonic() - began
            out = self._in_sequences(proof, out)
        finally:
            proof.end()
            ledger.save()
            proof.bank.save()
            self.learned.save()
        return out

    def _in_sequences(self, proof, out):
        """The versions chosen, held to the source over sequences of calls
        (`sequences`): one whose call differs in a sequence is not applied
        - its entry becomes the comment naming the sequence, and its
        record says so. Nothing unproved is applied."""
        from . import sequences
        from .prove import HarnessError
        pairs = []
        for r in self.routines:
            if r.name not in self.accepted or r.kind not in (
                    "function", "procedure", "event"):
                continue
            t = proof.made(r)
            if t is not None:
                pairs.append((r, t))
        if not pairs:
            return out
        try:
            found, runs, left = sequences.prove(proof, pairs)
        except HarnessError as e:
            self.sequences = {"not run": str(e)[:160]}
            return out
        self.sequences = {"run": runs, "apart": len(found),
                          "left out": [f"{r.name}: {why}" for r, why in left]}
        apart = {}
        for f in found:
            r = f.calls[f.at][0]
            apart[r.name] = f.said()
        if not apart:
            return out
        for rec in self.records:
            name = rec["routine"].split(" ", 1)[1]
            if name in apart:
                rec["class"] = "not converted"
                rec.setdefault("reasons", []).append(
                    "in a sequence of calls, " + apart[name])
                self.accepted.pop(name, None)
        kinds = {r.name: r.kind for r in self.routines}
        return [(name, f"-- {kinds.get(name, 'routine')} {name} not"
                       " converted: its version answers every single call"
                       " as the source does, but not a sequence of calls: "
                       + apart[name].replace("\n", " "),
                 apart[name]) if name in apart else (name, sql, why)
                for name, sql, why in out]

    def _source_views(self, proof):
        """The source's views made in the source's sandbox, as the source
        defines them, so a view's version can be held to them."""
        a = proof.a
        todo = []
        for v in self.views:
            try:
                made = self.pair.src_engine.code_definition(
                    "src", self.db, "view", v.settings.get("source", v.name))
            except Exception:  # noqa: BLE001 - made from its query instead
                made = None
            # the engine's own statement where it gives one, else the
            # view's query under its name
            made = made or v.definition
            if made:
                todo.append(re.sub(r"(?is)^\s*create\s+.*?\bview\b",
                                   "create view", made, count=1))
        for _ in range(3):
            left = []
            for stmt in todo:
                try:
                    a.create([stmt])
                except Exception:  # noqa: BLE001 - a view on one not made
                    a.conn.rollback()
                    left.append(stmt)
            if not left or len(left) == len(todo):
                break
            todo = left

    def _climb(self, proof, r, cands, ledger):
        from . import model
        from .rungs import ranked
        facts = self.facts(r)
        order, fell = ranked(cands, ledger, facts)
        tried, best = [], None
        reasons = [f"{c.said}: {why}" for c, why in fell]
        for c in order:
            for round_ in range(c.rounds):
                try:
                    if c.name == "model":
                        definition = r.definition or r.body
                        made = model.propose(self.pair.hop, self.src,
                                             self.dst, r, definition, tried)
                    else:
                        made = c.make(r)
                except Refused as e:
                    reasons.append(f"{c.said}: {e}")
                    break
                except Exception as e:  # noqa: BLE001 - a candidate broke
                    reasons.append(f"{c.said}: it failed"
                                   f" ({type(e).__name__}: {str(e)[:80]})")
                    break
                if made.cost and r.name not in self.costs:
                    self.costs[r.name] = made.cost
                from .plpgsql import unparsed
                refused = unparsed(made.statements) \
                    if self.dst == "postgres" else None
                if refused:
                    # the target's own parser says no: nothing to run
                    from .prove import Verdict
                    verdict = Verdict(r.said, "diff", "the target's parser"
                                      f" refuses it: {refused}")
                elif r.kind == "view":
                    verdict, _ = self._prove_view(proof, r, made)
                else:
                    verdict, _ = proof.try_version(r, made.statements)
                ledger.record(c.name, facts, verdict.status == "ok")
                if c.name == "rules":
                    self.rules_tried[r.name] = (r, made,
                                                verdict.status == "ok")
                if verdict.status == "ok":
                    self.accepted[r.name] = made.statements
                    if c.name != "rules" and r.kind != "view":
                        self._learn(proof, r, made, cands, facts)
                    return self._chosen(r, c, made, verdict, reasons,
                                        self._score(proof, r, made))
                if verdict.status == "warn" and best is None:
                    best = (c, made, verdict)
                    break
                reasons.append(f"{c.said}: {verdict.reason}")
                part = self._smallest(proof, c, r, verdict)
                if part:
                    reasons.append(f"{c.said}: the smallest part of the"
                                   f" source that still differs: {part}")
                tried.append((made.sql, verdict.reason + (
                    f"; the part that differs: {part}" if part else ""),
                    verdict.first.get("args") if verdict.first else None))
                if verdict.status == "not proved":
                    break
        if best is not None:
            c, made, verdict = best
            if r.kind == "view":
                self._prove_view(proof, r, made)
            else:
                proof.install(r, made.statements)
            self.accepted[r.name] = made.statements
            return self._chosen(r, c, made, verdict, reasons)
        self._record(r, "not converted", None, reasons=reasons,
                     constructs=self._constructs(r))
        head = f"-- {r.kind} {r.name} not converted: no candidate proved"
        return (r.name, "\n".join([head] + [f"--   {x}" for x in reasons]),
                "; ".join(reasons)[:300] or "no candidate proved")

    def _learn(self, proof, r, accepted, cands, facts):
        """A version other than the rules' proved where the rules' did
        not: each one-function substitution that turns the rules' version
        into one that proves is tried on every routine the rules converted
        with that function, and adopted with zero regressions and at least
        one routine newly proved (see `learn`)."""
        from . import learn
        if r.name not in self.rules_tried:
            # the operator's own file is tried first and was chosen: the
            # rules' version is made and proved now, to learn from
            rung = next((c for c in cands if c.name == "rules"
                         and not c.lacks(facts)), None)
            if rung is None:
                return
            try:
                made = rung.make(r)
            except Exception:  # noqa: BLE001 - nothing to learn from
                return
            verdict, _ = proof.try_version(r, made.statements)
            self.rules_tried[r.name] = (r, made, verdict.status == "ok")
        got = self.rules_tried[r.name]
        if got[2]:
            proof.install(r, accepted.statements)
            return
        _, rules_made, _ = got
        key = f"{self.src}->{self.dst}"
        calls = learn._CALL
        before = {m.group(1).lower() for m in calls.finditer(rules_made.sql)}
        after = {m.group(1).lower() for m in calls.finditer(accepted.sql)}
        pairs = [(o, n) for o in sorted(before - after)
                 for n in sorted(after - before)][:4]
        try:
            for old, new in pairs:
                if self.learned.known(key, old, new):
                    continue
                rule = [{"from": old, "to": new}]
                fixed = [learn.apply(x, rule) for x in rules_made.statements]
                verdict, _ = proof.try_version(r, fixed)
                if verdict.status != "ok":
                    continue
                proved, broke = [r.name], []
                for name, (other, made, ok) in list(self.rules_tried.items()):
                    if name == r.name or not re.search(
                            rf"(?i)\b{re.escape(old)}\s*\(", made.sql):
                        continue
                    v, _ = proof.try_version(other, [
                        learn.apply(x, rule) for x in made.statements])
                    if v.status == "ok" and not ok:
                        proved.append(name)
                    elif v.status != "ok" and ok:
                        broke.append(name)
                    if name in self.accepted:
                        proof.install(other, self.accepted[name])
                if broke:
                    self.learned.propose(key, old, new, broke, r.name)
                else:
                    self.learned.adopt(key, old, new, proved, r.name)
                    self.ctx["learned"] = self.learned.adopted(key)
                break
        finally:
            proof.install(r, accepted.statements)

    #: the candidates whose version of a cut copy is made the same way as
    #: of the whole routine: the operator's file is written for the whole
    #: one, and a model is asked, not run
    CUT_BY = ("rules", "emulate", "plsql-converter", "dialect-converter")

    def _smallest(self, proof, c, r, verdict):
        """The few source statements a failing candidate's version still
        gets wrong on the input that told it apart - what a person or a
        model needs instead of the whole routine."""
        if c.name not in self.CUT_BY or verdict.args is None:
            return None
        from . import reduce
        try:
            got = reduce.smallest(proof, r, verdict.args, c.make)
        except Exception:  # noqa: BLE001 - said without it
            return None
        if not got:
            return None
        lines, _ = got
        return " ".join(lines)[:300]

    def _constructs(self, r):
        if self.src != "mysql" or r.kind == "view":
            return []
        from . import mysql_pg
        try:
            return [c for c, _ in mysql_pg.convert(r, self.ctx["rules"])
                    .constructs]
        except Refused as e:
            return [str(e)]

    #: small changes the chosen version is tried with, to show the proof
    #: that chose it tells them apart (its own mutation score)
    MUTANTS = 6

    def _score(self, proof, r, made):
        """{caught, tried, missed} of the chosen version's mutants - a
        mutant the proof passes is a change it cannot see (or one that
        changes nothing), named with its line."""
        if r.kind not in ("function", "procedure") or self.dst != "postgres":
            return None
        from . import mutants
        try:
            caught, tried, survived = mutants.score(proof, r, made.statements,
                                                    self.MUTANTS)
        except Exception:  # noqa: BLE001 - chosen all the same, said so
            proof.install(r, made.statements)
            return None
        return {"caught": caught, "tried": tried,
                "missed": [f"{what} at line {line}"
                           for what, line in survived]}

    def _chosen(self, r, c, made, verdict, reasons, score=None):
        kind = "proved" if verdict.status == "ok" else "proved with caveats"
        self._record(r, kind, c.name, evidence=verdict.record(),
                     fell=reasons, constructs=[x for x, _ in
                                               made.constructs],
                     **({"mutants": score} if score else {}))
        mark = (f"-- converted by {c.said} and proved: {verdict.reason}"
                + (f" ({verdict.coverage})" if verdict.coverage else ""))
        if made.residue:
            mark += "\n-- left for a person: " + "; ".join(made.residue)
        return (r.name, f"{made.sql}\n{mark}", "" if verdict.status == "ok"
                else verdict.reason)

    def _prove_view(self, proof, r, made):
        """A view's version made in the target's sandbox and held to the
        source's by the fixture's rows it answers."""
        from .prove import Verdict
        b = proof.b
        try:
            with b.conn.cursor() as cur:
                cur.execute(f"drop view if exists {b.q(r.name)}")
            b.conn.commit()
            b.create(made.statements)
        except Exception as e:  # noqa: BLE001 - the candidate's failure
            b.conn.rollback()
            return Verdict(f"view {r.name}", "diff", "the target refuses it:"
                           f" {(str(e).strip().splitlines() or [''])[0][:160]}"
                           ), None
        twin = proof.sandbox.twin
        source = r.settings.get("source", r.name)
        try:
            sc, dc, _ = twin._comparable_columns(
                self.db, twin.src_engine, source, twin.dst_engine, r.name)
            x = twin.src_engine.neutral_digest("src", self.db, source, sc)
            y = twin.dst_engine.neutral_digest("dst", self.db, r.name, dc)
        except Exception as e:  # noqa: BLE001 - said, as not proved
            return Verdict(f"view {r.name}", "not proved", "it could not be"
                           f" read in the sandbox: {str(e)[:100]}"), None
        if x != y:
            return Verdict(f"view {r.name}", "diff", "its rows over the"
                           " fixture differ"), None
        return Verdict(f"view {r.name}", "ok", "its rows over the fixture"
                       " digest the same", calls=1), made

    def converter_costs(self):
        """The PL/SQL converter's own estimate for every routine it reads
        and the climb did not ask it of (a candidate before it proved) -
        where it is built here, and only as its figure (`effort`)."""
        from . import programs
        from .translate import Refused
        if self.dst != "postgres" or \
                self.src not in programs.READS["ora2pg"] or \
                not programs.available("ora2pg"):
            return
        for r in self.routines:
            if r.name in self.costs or r.kind not in programs.ORA2PG_TYPES:
                continue
            try:
                made = programs.ora2pg(r, self.src, self.dst,
                                       self.ctx.get("pg_version"))
            except Refused:
                continue
            if made.cost:
                self.costs[r.name] = made.cost

    def write(self, out_path):
        from . import effort
        try:
            out_path.write_text(json.dumps(
                {"effort": effort.summary(self.records, self.routines,
                                          self.costs, self.seconds,
                                          self.src),
                 "routines": self.records,
                 "sandbox": "used" if self.used else
                 (self.why_not or "not named"),
                 **({"sequences": self.sequences} if self.sequences
                    else {})},
                indent=1, default=str))
        except OSError:
            pass


def _pg_version(runner):
    if runner is None or runner.engine != "postgres":
        return None
    with runner.conn.cursor() as cur:
        cur.execute("show server_version_num")
        got = int(cur.fetchone()[0]) // 10000
    runner.conn.commit()
    return got


def converted(pair, db, propose=True, climb=True):
    """[(name, the target's statement or a comment, why not)] for the
    source's views and routines, in the target's SQL (see the module's
    docstring)."""
    if not (pair.src_engine.SQL_DIALECT and pair.dst_engine.SQL_DIALECT):
        return []
    work = Conversion(pair, db, propose)
    out = None
    if climb:
        from .prove import Proof
        from .sandbox import spec
        if spec(pair.hop) is not None:
            proof = Proof(pair, db)
            proof.open(True)
            work.why_not = proof.why_not
            if proof.sandbox is not None:
                work.used = True
                try:
                    out = work.proved(proof)
                finally:
                    proof.close()
    if out is None:
        out = work.unproved()
    if climb:
        work.converter_costs()
        work.write(pair.hop.report_dir(db) / "code-conversion.json")
    return out


def translatable(pair, db):
    """The names the query translator carries by itself - what `check`
    tells apart from what a model or a person wrote."""
    work = Conversion(pair, db, propose=False)
    names = set()
    for r in work.views + work.routines:
        try:
            translator(pair, work.ctx, r)
            names.add(r.name)
        except Refused:
            pass
    return names
