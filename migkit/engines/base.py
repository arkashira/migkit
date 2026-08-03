from dataclasses import dataclass, field


@dataclass
class Result:
    check: str
    scope: str
    status: str
    detail: str = ""
    report: str = ""
    fix_hint: str = ""


@dataclass
class RepairAction:
    scope: str
    kind: str
    statements: list = field(default_factory=list)
    undo: list = field(default_factory=list)
    note: str = ""


class Engine:
    checks = ("schema", "counts", "autoinc", "data")
    counts_from_data = False

    def __init__(self, hop):
        self.hop = hop

    def databases(self):
        raise NotImplementedError

    def check_schema(self, db):
        raise NotImplementedError

    def check_counts(self, db):
        raise NotImplementedError

    def check_autoinc(self, db):
        return [Result("autoinc", db, "skip", "not applicable for this engine")]

    def check_deep(self, db):
        return [Result("deep", db, "skip", "no deep checks for this engine yet")]

    def check_params(self, db):
        return [Result("params", db, "skip",
                       "no parameter comparison for this engine yet")]

    def _param_result(self, db, src, dst, critical, hint):
        """Dump every server setting from both sides to params.json (same shape
        as objects.json), then flag mismatches. Only behavior-critical settings
        (timezone, encoding, collation, sql_mode, ...) fail the check; the many
        instance-specific ones that always differ on managed databases (memory,
        paths, limits) are counted but stay ok. The full list is on disk."""
        import json
        names = sorted(set(src) | set(dst))
        inv = {n: {"src": src.get(n), "dst": dst.get(n)} for n in names}
        out = self.hop.report_dir(db) / "params.json"
        out.write_text(json.dumps(inv, indent=1, default=str))
        crit_lc = {c.lower() for c in critical}
        diff = [n for n in names if src.get(n) != dst.get(n)]
        crit = [n for n in diff if n.lower() in crit_lc]
        if not diff:
            return [Result("params", f"{db} params", "ok",
                           f"{len(names)} settings, all equal both sides",
                           str(out))]
        if crit:
            shown = "; ".join(f"{n} src={src.get(n)} dst={dst.get(n)}"
                              for n in crit[:12])
            extra = len(diff) - len(crit)
            tail = f" (+{extra} non-critical, see params.json)" if extra else ""
            return [Result("params", f"{db} params", "diff",
                           f"{len(crit)} behavior-critical settings differ: "
                           + shown + tail, str(out), hint)]
        return [Result("params", f"{db} params", "ok",
                       f"{len(diff)} of {len(names)} settings differ but none"
                       " behavior-critical (memory/paths/limits, see"
                       " params.json)", str(out))]

    def _atlas_authoritative(self, res):
        """atlas is schema-aware and the authoritative differ; when it says
        clean, demote the noisier textual opinions (native dump diff, migra,
        liquibase) to informational so the db's verdict follows atlas. The
        precise object inventory still stands. Opt out with
        options.schema_authority != 'atlas'."""
        if self.hop.options.get("schema_authority", "atlas") != "atlas":
            return res
        if not any(r.scope.endswith("(atlas)") and r.status == "ok"
                   for r in res):
            return res
        for r in res:
            textual = (r.scope.endswith(("(migra)", "(liquibase)"))
                       or r.scope == r.scope.split(" ")[0])  # bare "db"
            if (r.check == "schema" and r.status == "diff" and textual
                    and not r.scope.endswith(("(atlas)", "objects"))):
                r.status = "ok"
                r.detail = ("atlas authoritative: clean; textual diff is"
                            f" cosmetic ({r.detail})")[:200]
        return res

    def check_data(self, db, table=None):
        raise NotImplementedError

    def repair_plan(self, db, kind):
        return []

    def setup_target_plan(self, db):
        return []

    def watch_sample(self, db):
        return {}

    def assess(self):
        return [{"level": "warn", "scope": "-",
                 "item": "assess not implemented for this engine yet",
                 "detail": ""}]
