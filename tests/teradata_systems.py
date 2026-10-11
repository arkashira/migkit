"""Two Teradata systems in memory, for the tests that hold one to the other.

No Teradata runs in a container, so each statement migkit sends is answered
here from what a test puts in a system: its tables with their `DBC.ColumnsV`
rows, keys and indexes, objects, triggers, references, partitions, rights and
the numbers its aggregates return. A statement this module does not know is
answered with no rows, so a test says what it reads. The driver is the
fake of `tests/test_teradata_as_a_side_of_a_pair.py`.
"""
import sys
import types

from migkit.config import Endpoint, Hop

SRC, DST = "10.0.0.7", "10.0.0.8"


class DriverError(Exception):
    """An error as the driver raises one."""


DriverError.__module__ = "teradatasql"


def column(name, code, length=0, digits=None, frac=None, chartype=None,
           upper=None, nullable="Y", default=None, identity=None):
    """A `DBC.ColumnsV` row, as `COLUMNS_SQL` selects it."""
    return (name, code, length, digits, frac, chartype, upper, nullable,
            default, identity)


def system(**given):
    """One system's contents; what a test does not give is empty."""
    out = {"tables": [], "columns": {}, "keys": {}, "indexes": {},
           "objects": [], "creators": {}, "triggers": [], "refs": [],
           "ppi": {}, "parts": {}, "pi": {}, "rights": [], "owner": "DBC",
           "space": [], "version": "17.20.03.19", "views": [],
           "sessions": [],
           "stmts": [], "info": {"RELEASE": "17.20", "VERSION": "17.20.03.19"}}
    out.update(given)
    return out


class Systems:
    """The driver's module, answering for two hosts."""

    def __init__(self, src=None, dst=None):
        self.hosts = {SRC: src if src is not None else system(),
                      DST: dst if dst is not None else system()}
        self.said = []
        self.params = []
        self.fail = {}
        #: a test's own answers, asked first: (host, statement, args) -> rows
        self.hook = None

    # ---- the driver --------------------------------------------------

    def module(self):
        mod = types.ModuleType("teradatasql")
        mod.connect = self.connect
        mod.OperationalError = DriverError
        return mod

    def connect(self, **params):
        self.params.append(params)
        return _Conn(self, params["host"])

    def install(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "teradatasql", self.module())
        return self

    def sql(self, host=None):
        return [s for h, s, _ in self.said if host is None or h == host]

    # ---- the answers ---------------------------------------------------

    def answer(self, host, sql, args):
        for needle, error in self.fail.items():
            if needle in sql:
                return error
        s = self.hosts[host]
        low = " ".join(sql.split())
        if self.hook is not None:
            got = self.hook(host, low, args)
            if got is not None:
                return got
        if low.startswith("{fn teradata_nativesql}{fn teradata_database_"
                          "version}"):
            return [(s["version"],)]
        if "from DBC.DBCInfoV" in low:
            return [(k, v) for k, v in s["info"].items()]
        if "from DBC.TablesV" in low:
            return self._tables(s, low, args)
        if "from DBC.ColumnsV" in low:
            return s["columns"].get(args[1], [])
        if "from DBC.IndicesV" in low:
            return self._indices(s, low, args)
        if "from DBC.TriggersV" in low:
            return self._triggers(s, low)
        if "from DBC.All_RI_ChildrenV" in low:
            return [r for r in s["refs"]]
        if "from DBC.PartitioningConstraintsV" in low:
            return [(t, e) for t, e in s["ppi"].items()]
        if low.startswith("select PARTITION, count(*) from"):
            t = low.split('"."')[1].split('"')[0]
            return list(s["parts"].get(t, []))
        if "from DBC.AllRightsV" in low:
            if "AccessRight from" in low and "UserName, TableName" in low:
                return list(s["rights"])
            return [(u, r) for u, _, r in s["rights"]]
        if "from DBC.DatabasesV" in low and "OwnerName" in low:
            return [(s["owner"],)]
        if "from DBC.DiskSpaceV" in low:
            return list(s["space"])
        if "from DBC.SessionInfoV" in low:
            return list(s["sessions"]) if "group by" in low else [(1,)]
        return None

    def _tables(self, s, low, args):
        if "TableKind in ('T', 'O')" in low:
            return [(t,) for t in s["tables"]]
        if "RequestText" in low:
            return list(s["views"])
        if "CreatorName" in low:
            return [(n, c, k) for n, (c, k) in s["creators"].items()]
        if "TableKind in (" in low and "TableKind from" in low:
            return list(s["objects"])
        return [(t,) for t in s["tables"]]

    def _indices(self, s, low, args):
        if "distinct TableName" in low:
            return [(t,) for t in s["ppi"]]
        if "IndexType in ('K', 'P', 'Q')" in low:
            return [("K", "Y", 1, c, i + 1)
                    for i, c in enumerate(s["keys"].get(args[1], []))]
        if "IndexType in ('S', 'U', 'V')" in low:
            return list(s["indexes"].get(args[1], []))
        if "IndexType in ('P', 'Q') order by ColumnPosition" in low:
            return [(c,) for c in s["pi"].get(args[1], [])]
        return []

    def _triggers(self, s, low):
        if "EnabledFlag" in low:
            if low.startswith("select DatabaseName"):
                return [(d, t, on, f) for d, t, on, f in s["triggers"]]
            return [(t, on, f) for _, t, on, f in s["triggers"]]
        return [(t, on) for _, t, on, _ in s["triggers"]]


class _Cursor:
    def __init__(self, conn):
        self.conn, self.rows, self.rowcount = conn, [], -1
        self.description = None

    def execute(self, sql, params=()):
        self.conn.fake.said.append((self.conn.host, sql, tuple(params or ())))
        got = self.conn.fake.answer(self.conn.host, sql, tuple(params or ()))
        if isinstance(got, Exception):
            raise got
        if isinstance(got, int):
            self.rowcount, self.rows = got, []
            return
        self.rows = [list(r) for r in (got or [])]
        self.rowcount = len(self.rows)

    def executemany(self, sql, seq):
        seq = [tuple(r) for r in seq]
        self.conn.fake.said.append((self.conn.host, "MANY " + sql, seq))

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows

    def fetchmany(self, n):
        rows, self.rows = self.rows[:n], self.rows[n:]
        return rows

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None


class _Conn:
    def __init__(self, fake, host):
        self.fake, self.host, self.autocommit = fake, host, True

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.fake.said.append((self.host, "COMMIT", ()))

    def rollback(self):
        self.fake.said.append((self.host, "ROLLBACK", ()))

    def close(self):
        pass


def hop(tmp_path, **given):
    """A Teradata hop between the two systems, its reports in `tmp_path`."""
    h = Hop(name="td", engine="teradata",
            source=Endpoint(host=SRC, user="MIGKIT", password="CHANGE_ME"),
            target=Endpoint(host=DST, user="MIGKIT", password="CHANGE_ME"),
            databases=["shop"], **given)

    def report_dir(db=None):
        d = tmp_path / "reports" / (db or "")
        d.mkdir(parents=True, exist_ok=True)
        return d
    h.report_dir = report_dir
    return h


def engine(tmp_path, **given):
    from migkit.engines.teradata import TeradataEngine
    return TeradataEngine(hop(tmp_path, **given))
