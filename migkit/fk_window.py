from collections import defaultdict, deque
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .setaside import SetAside

PG_FK_SQL = """
select format('%I.%I', cn.nspname, c.relname)
       || chr(31) || k.conname
       || chr(31) || pg_get_constraintdef(k.oid)
       || chr(31) || format('%I.%I', rn.nspname, r.relname)
  from pg_constraint k
  join pg_class c on c.oid = k.conrelid
  join pg_namespace cn on cn.oid = c.relnamespace
  join pg_class r on r.oid = k.confrelid
  join pg_namespace rn on rn.oid = r.relnamespace
 where k.contype = 'f'
 order by 1, 2"""


def hold_or_sequence_dependent_tables(
    tables: Iterable[str],
    dependencies: Iterable[Tuple[str, str]],
) -> Tuple[List[List[str]], List[List[str]]]:
    tbl_set = set(tables)
    in_degree = {t: 0 for t in tbl_set}
    adj = defaultdict(set)

    for child, parent in dependencies:
        if child in tbl_set and parent in tbl_set and child != parent:
            if child not in adj[parent]:
                adj[parent].add(child)
                in_degree[child] += 1

    queue = deque([t for t, deg in in_degree.items() if deg == 0])
    waves: List[List[str]] = []
    visited: Set[str] = set()

    while queue:
        wave: List[str] = []
        for _ in range(len(queue)):
            node = queue.popleft()
            wave.append(node)
            visited.add(node)
            for ch in adj[node]:
                in_degree[ch] -= 1
                if in_degree[ch] == 0:
                    queue.append(ch)
        if wave:
            waves.append(sorted(wave))

    remaining = tbl_set - visited
    cycles: List[List[str]] = []
    if remaining:
        cycles.append(sorted(remaining))
        waves.append(sorted(remaining))

    return waves, cycles


class ForeignKeyWindow:
    def __init__(self, hop, db: str, log: Optional[Any] = None, tables: Optional[Iterable[str]] = None):
        self.hop = hop
        self.db = db
        self.log = log
        self.tables = set(tables) if tables else None
        self.record = SetAside(hop, db, "dropped-fks")
        self.dropped: List[Dict[str, str]] = []

    def __enter__(self):
        eng = getattr(self.hop, "target_engine", None)
        if eng is None and hasattr(self.hop, "engine"):
            from .engines.postgres import PostgresEngine

            eng = PostgresEngine(self.hop)

        try:
            raw = eng._psql("dst", self.db, PG_FK_SQL) if hasattr(eng, "_psql") else ""
        except Exception:
            raw = ""

        fks = []
        for line in (raw or "").splitlines():
            parts = line.split("\x1f")
            if len(parts) >= 4:
                tbl, name, definition, ref_tbl = (parts + [""] * 4)[:4]
                if self.tables and tbl not in self.tables and ref_tbl not in self.tables:
                    continue
                fks.append({"table": tbl, "name": name, "definition": definition, "ref": ref_tbl})

        if not fks:
            return self

        defs_map = {f"{fk['table']}.{fk['name']}": fk for fk in fks}
        self.record.save(defs_map)

        for fk in fks:
            try:
                sch, tbl = fk["table"].split(".", 1) if "." in fk["table"] else ("public", fk["table"])
                sql = f'alter table "{sch}"."{tbl}" drop constraint if exists "{fk["name"]}"'
                if hasattr(eng, "_psql"):
                    eng._psql("dst", self.db, sql)
                self.dropped.append(fk)
            except Exception as e:
                if self.log:
                    self.log(f"could not drop fk {fk['name']}: {e}")

        return self

    def __exit__(self, *exc):
        eng = getattr(self.hop, "target_engine", None)
        if eng is None and hasattr(self.hop, "engine"):
            from .engines.postgres import PostgresEngine

            eng = PostgresEngine(self.hop)

        # Re-apply any dropped or left-behind
        left_defs = []
        try:
            records = self.record.records()
            for _, alive, defs in records:
                if not alive and isinstance(defs, dict):
                    left_defs.extend(defs.values())
        except Exception:
            pass

        all_to_restore = self.dropped + [d for d in left_defs if d not in self.dropped]
        for fk in all_to_restore:
            try:
                sch, tbl = fk["table"].split(".", 1) if "." in fk["table"] else ("public", fk["table"])
                sql = f'alter table "{sch}"."{tbl}" add constraint "{fk["name"]}" {fk["definition"]}'
                if hasattr(eng, "_psql"):
                    eng._psql("dst", self.db, sql)
            except Exception as e:
                if self.log:
                    self.log(f"could not restore fk {fk['name']}: {e}")

        try:
            self.record.unlink()
        except Exception:
            pass
