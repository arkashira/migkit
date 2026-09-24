"""Business-rule checks: the operator's own questions, asked of both sides.

Row-level comparison proves the rows are the same. It does not prove what a
business means by "the same": revenue per day, orders per status, the
balance of every account. Those are questions the operator can write in SQL
and migkit cannot guess, so the hop carries them:

    rules:
      orders by status: select status, count(*) from orders group by status
      revenue: {source: "select sum(amount) from sales",
                target: "select sum(amount) from public.sales"}

Each runs on both sides inside a read-only transaction, so a rule can never
write - the source is not written to, whatever the rule says. The answers
are compared by value, not by text: a sum is the same number whether one
server writes `1.5000` and the other `1.5`, and rows are compared as a set,
because two servers group in whatever order they like.
"""
from decimal import Decimal, InvalidOperation

from .engines.base import Result


def configured(hop):
    """{name: (source sql, target sql)} from the hop's `rules`."""
    out = {}
    for name, rule in ((hop.options or {}).get("rules") or {}).items():
        if isinstance(rule, dict):
            src, dst = rule.get("source"), rule.get("target")
        else:
            src = dst = rule
        if src and dst:
            out[str(name)] = (str(src), str(dst))
    return out


def value(v):
    """One answer, in a form that two engines agree on when the value does:
    numbers as numbers (so `1.5000` and `1.5`, `3` and `3.0` meet), time as
    ISO text, bytes as hex."""
    import datetime
    if v is None:
        return None
    if isinstance(v, bool):
        return Decimal(int(v))
    if isinstance(v, (int, Decimal)) or hasattr(v, "to_decimal"):
        d = v.to_decimal() if hasattr(v, "to_decimal") else Decimal(v)
        return d.normalize() if d else Decimal(0)
    if isinstance(v, float):
        from .canon import _float_text
        try:
            return Decimal(_float_text(v)).normalize()
        except InvalidOperation:
            return repr(v)
    if isinstance(v, (datetime.datetime, datetime.date, datetime.time)):
        return v.isoformat()
    if isinstance(v, (bytes, bytearray, memoryview)):
        return bytes(v).hex().upper()
    return str(v)


def _answer(rows):
    return sorted((tuple(value(v) for v in row) for row in rows),
                  key=lambda r: tuple((x is None, str(x)) for x in r))


def check(engine, db):
    """One Result per rule; `run_rule(side, db, sql)` is the engine's."""
    out = []
    for name, (src_sql, dst_sql) in sorted(configured(engine.hop).items()):
        scope = f"{db} rule {name}"
        try:
            a = _answer(engine.run_rule("src", db, src_sql))
            b = _answer(engine.run_rule("dst", db, dst_sql))
        except Exception as e:
            out.append(Result("rules", scope, "error",
                              f"{str(e).strip().splitlines()[-1][:160]} -"
                              " a rule that did not run is not a rule that"
                              " held"))
            continue
        if a == b:
            out.append(Result("rules", scope, "ok",
                              f"{len(a)} rows, the same on both sides"))
            continue
        only_src = [r for r in a if r not in b]
        only_dst = [r for r in b if r not in a]
        show = "; ".join(
            [f"source {r}" for r in only_src[:3]]
            + [f"target {r}" for r in only_dst[:3]])
        out.append(Result("rules", scope, "diff",
                          f"{len(only_src)} rows only on the source,"
                          f" {len(only_dst)} only on the target: {show}"))
    return out
