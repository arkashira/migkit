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
