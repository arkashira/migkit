"""Two sides of a two-way pair kept in memory, for the tests of how a row
both sides changed is settled (`twoway`).

A side answers what `twoway.resolve` asks a target - its columns, its rows
by key - and applies a batch the way PostgreSQL and MySQL apply one: the
batch collapsed by the base applier's own `_collapsed`, an upsert writing
the columns it carries, a counter moved where the row stands
(`canon.Added`), a value the target keeps left as it stands
(`canon.Kept`), a row made with what it is given and NULL elsewhere. Its
own application's writes are made with `update`, `insert`, `delete` and
`move`, each returning the change its log records - the whole row before
and after, as a binlog with full images or REPLICA IDENTITY FULL keeps.
"""
import copy

from migkit import canon, twoway
from migkit.config import Endpoint, Hop
from migkit.engines.base import Engine

TABLE = "t"
COLUMNS = [("id", "integer"), ("v", "text"), ("w", "text"),
           ("ts", "integer"), ("n", "integer"), ("hi", "integer"),
           ("tags", "integer[]")]


class Side:
    CANON_ENGINE = "postgres"

    def __init__(self, rows=None, columns=COLUMNS):
        self.columns = list(columns)
        self.rows = {}
        for key, row in (rows or {}).items():
            self.rows[key] = self._whole({**copy.deepcopy(row), "id": key})

    def _whole(self, row):
        return {**{n: None for n, _ in self.columns}, **row}

    # ---- what the tail asks the target ----

    def neutral_columns(self, side, db, table):
        return list(self.columns)

    def neutral_rows_by_key(self, side, db, table, columns, key, keys):
        classes = dict(columns)
        out = {}
        for k in keys:
            row = self.rows.get(k[0])
            if row is None:
                continue
            at = tuple(twoway._text(classes[n], v) for n, v in zip(key, k))
            out[at] = tuple(copy.deepcopy(row.get(n)) for n, _ in columns)
        return out

    def apply(self, changes):
        for _, (kind, key, values) in Engine._collapsed(changes):
            at = key["id"]
            if kind == "delete":
                self.rows.pop(at, None)
                continue
            row = self.rows.get(at)
            if row is None:
                self.rows[at] = self._whole({**key, **{
                    n: copy.deepcopy(v.to if isinstance(v, canon.Added)
                                     else v)
                    for n, v in values.items()}})
                continue
            for n, v in values.items():
                if isinstance(v, canon.Kept):
                    continue
                if isinstance(v, canon.Added):
                    row[n] = None if row[n] is None else row[n] + v.by
                else:
                    row[n] = copy.deepcopy(v)

    # ---- the side's own application writing ----

    def update(self, key, **values):
        before = copy.deepcopy(self.rows[key])
        after = {**before, **copy.deepcopy(values)}
        self.rows[key] = copy.deepcopy(after)
        return canon.change("update", TABLE, {"id": key}, after, before)

    def insert(self, key, **values):
        row = self._whole({**copy.deepcopy(values), "id": key})
        self.rows[key] = copy.deepcopy(row)
        return canon.change("insert", TABLE, {"id": key}, row)

    def delete(self, key):
        before = self.rows.pop(key)
        return canon.change("delete", TABLE, {"id": key}, None, before)

    def move(self, key, to):
        before = self.rows.pop(key)
        after = {**copy.deepcopy(before), "id": to}
        self.rows[to] = copy.deepcopy(after)
        return canon.change("update", TABLE, {"id": key}, after, before)


class Pair:
    """What `twoway.resolve` reads off a tail: its hop and its target."""

    def __init__(self, hop, dst):
        self.hop, self.dst_engine = hop, dst


def hop(name, two_way, where, **options):
    """A hetero hop with `two_way` whose reports go under `where`."""
    got = Hop(name=name, engine="hetero",
              source=Endpoint(host="10.0.0.1", port=5432),
              target=Endpoint(host="10.0.0.2", port=3306),
              databases=["d"], options={"two_way": two_way, **options})

    def report_dir(db="", w=where):
        d = w / db if db else w
        d.mkdir(parents=True, exist_ok=True)
        return d
    got.report_dir = report_dir
    return got


def pair(name, two_way, dst, where, **options):
    return Pair(hop(name, two_way, where, **options), dst)


def sync(p, changes, said=None):
    """A batch of the other side's changes settled against `p`'s target
    and applied there: what was applied."""
    got = twoway.resolve(p, "d", copy.deepcopy(changes),
                         (said if said is not None else []).append)
    p.dst_engine.apply(copy.deepcopy(got))
    return got


#: the rules of the hop back that settle every conflict as this hop does
FLIP = {"overwrite": "ignore", "ignore": "overwrite",
        "apply_remote": "keep_local", "keep_local": "apply_remote"}


def mirrored(two_way):
    """The `two_way` of the hop the other way: `overwrite` and `ignore`
    swapped, the ranks turned round, every other rule as it is."""
    def one(rule):
        return FLIP.get(rule, rule) if isinstance(rule, str) else rule
    out = copy.deepcopy(two_way)
    for k in ("on_conflict", "delete_vs_update"):
        if k in out:
            out[k] = one(out[k])
    if "source_rank" in out:
        out["source_rank"], out["target_rank"] = (out["target_rank"],
                                                  out["source_rank"])
    for table in (out.get("tables") or {}).values():
        for k in ("default", "delete_vs_update"):
            if k in table:
                table[k] = one(table[k])
        table["columns"] = {c: one(r) for c, r in
                            (table.get("columns") or {}).items()}
    return out


def records(where, db="d"):
    import json
    path = where / db / twoway.RECORD
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]
