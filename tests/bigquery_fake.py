"""A BigQuery project that answers as the client's documentation says it
does, for the tests of what migkit reads from one: datasets of tables with
their metadata, routines, queries answered by the statement's text, and a
dry run that says how many bytes a query would read.

It is a stand-in for the answers, never for BigQuery: what a real project
says to these calls is not proved by anything that runs here, and a test
that uses it says so (`capabilities.UNPROVED`).
"""
import datetime
from types import SimpleNamespace as NS

from migkit.config import Endpoint, Hop

PROJECTS = {"src": "proj-a", "dst": "proj-b"}


class Field:
    def __init__(self, name, field_type="STRING", mode="NULLABLE", fields=(),
                 default=None, tags=None, rounding=None, collation=None,
                 max_length=None, precision=None, scale=None):
        self.name, self.field_type, self.mode = name, field_type, mode
        self.fields = list(fields)
        self.default_value_expression = default
        self.policy_tags = NS(names=list(tags)) if tags else None
        self.rounding_mode = rounding
        self.max_length, self.precision, self.scale = \
            max_length, precision, scale
        self._collation = collation

    def to_api_repr(self):
        return {"name": self.name, "type": self.field_type,
                **({"collation": self._collation} if self._collation
                   else {})}


class Table:
    """A table's metadata, as `Client.get_table` hands it."""

    def __init__(self, name, schema=(), table_type="TABLE", **kw):
        self.table_id, self.table_type = name, table_type
        self.num_rows = kw.get("rows", 0)
        self.schema = list(schema)
        self.time_partitioning = kw.get("time_partitioning")
        self.range_partitioning = kw.get("range_partitioning")
        self.clustering_fields = kw.get("clustering_fields")
        self.require_partition_filter = kw.get("require_partition_filter")
        self.table_constraints = kw.get("constraints")
        self.view_query = kw.get("view_query")
        self.mview_query = kw.get("mview_query")
        self.mview_enable_refresh = kw.get("mview_enable_refresh", True)
        self.mview_refresh_interval = kw.get("mview_refresh_interval")
        self.mview_last_refresh_time = kw.get(
            "mview_last_refresh_time",
            datetime.datetime(2026, 1, 1, tzinfo=datetime.timezone.utc)
            if table_type == "MATERIALIZED_VIEW" else None)
        self.expires = kw.get("expires")
        self.labels = kw.get("labels") or {}
        self.description = kw.get("description")
        self.encryption_configuration = kw.get("encryption")
        self.max_staleness = kw.get("max_staleness")
        self.external_data_configuration = kw.get("external")
        self._api = kw.get("api") or {}

    def to_api_repr(self):
        return dict(self._api)


def time_partitioning(field=None, type_="DAY", expiration_ms=None):
    return NS(type_=type_, field=field, expiration_ms=expiration_ms)


def range_partitioning(field, start=0, end=1000, interval=10):
    return NS(field=field, range_=NS(start=start, end=end,
                                     interval=interval))


def constraints(primary=(), foreign=()):
    """`foreign`: [(referenced table, [(column, referenced column)])]."""
    return NS(
        primary_key=NS(columns=list(primary)) if primary else None,
        foreign_keys=[NS(name=f"fk{i}", referenced_table=NS(table_id=t),
                         column_references=[
                             NS(referencing_column=a, referenced_column=b)
                             for a, b in cols])
                      for i, (t, cols) in enumerate(foreign)])


class Dataset:
    def __init__(self, **kw):
        self.tables, self.routines, self.rowpolicies = {}, {}, {}
        self.iam = {}
        self.access_entries = kw.get("access", [])
        self.is_case_insensitive = kw.get("insensitive", False)
        self.default_table_expiration_ms = kw.get("table_expiry")
        self.default_partition_expiration_ms = kw.get("partition_expiry")
        self.max_time_travel_hours = kw.get("travel", 168)
        self.storage_billing_model = kw.get("billing")
        self.labels = kw.get("labels") or {}
        self.default_encryption_configuration = None
        self._api = kw.get("api") or {}

    def to_api_repr(self):
        return dict(self._api)

    def add(self, table):
        self.tables[table.table_id] = table
        return table


class Job:
    def __init__(self, rows=None, nbytes=0, affected=0):
        self._rows, self.total_bytes_processed = rows or [], nbytes
        self.num_dml_affected_rows = affected
        self.statement_type = "SELECT"

    def result(self):
        return self._rows


class Row(dict):
    def values(self):
        return list(super().values())


class Client:
    """One project's client. `answers` are (text a statement contains, the
    rows it answers - or a function of the statement -, the bytes its dry
    run reports); the first that matches answers."""

    def __init__(self, project, datasets=None):
        self.project = project
        self.datasets = datasets or {}
        self.answers, self.asked, self.dry = [], [], []
        self._connection = NS(api_request=self._api_request)
        self.snapshots, self.copies, self.refuse = [], [], None

    def dataset(self, name="sales"):
        return self.datasets.setdefault(name, Dataset())

    def on(self, text, rows, nbytes=0):
        self.answers.append((text, rows, nbytes))
        return self

    def _split(self, ref):
        parts = str(ref).split(".")
        return parts[-2], parts[-1]

    def list_tables(self, ref):
        ds = self.datasets[str(ref).split(".")[-1]]
        return [NS(table_id=t.table_id, table_type=t.table_type)
                for t in ds.tables.values()]

    def get_table(self, ref):
        from google.api_core import exceptions
        dsn, name = self._split(ref)
        try:
            return self.datasets[dsn].tables[name]
        except KeyError:
            raise exceptions.NotFound(f"{ref} not found")

    def get_dataset(self, ref):
        return self.datasets[str(ref).split(".")[-1]]

    def list_routines(self, ref):
        ds = self.datasets[str(ref).split(".")[-1]]
        return [NS(routine_id=r.routine_id, type_=r.type_,
                   reference=NS(ref=(ds, r.routine_id)))
                for r in ds.routines.values()]

    def get_routine(self, reference):
        ds, name = reference.ref
        return ds.routines[name]

    def copy_table(self, source, destination, job_config=None):
        """A copy job: recorded, and done when asked for its result."""
        self.copies.append((str(source), str(destination),
                            getattr(job_config, "write_disposition", None)))
        if self.refuse:
            from google.api_core import exceptions
            raise exceptions.Forbidden(self.refuse)
        return Job()

    def get_iam_policy(self, ref):
        dsn, name = self._split(ref)
        return NS(bindings=self.datasets[dsn].iam.get(name, []))

    def _api_request(self, method, path):
        parts = path.strip("/").split("/")
        ds, table = self.datasets[parts[3]], parts[5]
        return {"rowAccessPolicies": [
            {"rowAccessPolicyReference": {"policyId": n},
             "filterPredicate": f, "grantees": g}
            for n, (f, g) in ds.rowpolicies.get(table, {}).items()]}

    def query(self, sql, job_config=None):
        text = " ".join(sql.split())
        dry = bool(getattr(job_config, "dry_run", False))
        (self.dry if dry else self.asked).append(text)
        low = text.lower()
        if low.startswith("create snapshot table"):
            self.snapshots.append(text)
            return Job()
        for needle, rows, nbytes in self.answers:
            if needle.lower() in low:
                got = rows(text) if callable(rows) else rows
                return Job([Row(r) if isinstance(r, dict)
                            else Row({f"f{i}": v for i, v in enumerate(r)})
                            for r in got], nbytes)
        return Job([], 0)


def engine(monkeypatch, src=None, dst=None, name="bigquery",
           report=None, **hop):
    """A BigQuery engine over two fake projects. Every engine of the class
    - a pair makes its own two - reads the project its endpoint names."""
    from migkit.engines import engine_named
    from migkit.engines.warehouse import BigQueryEngine
    clients = {PROJECTS["src"]: src or Client(PROJECTS["src"]),
               PROJECTS["dst"]: dst or Client(PROJECTS["dst"])}
    for c in clients.values():
        c.dataset("sales")
    monkeypatch.setattr(BigQueryEngine, "_client",
                        lambda self, side: clients[self._database(side)])
    monkeypatch.setattr(
        BigQueryEngine, "_rows",
        lambda self, side, db, sql, args=None: [
            list(r.values())
            for r in clients[self._database(side)].query(sql).result()])
    h = Hop(name="bq", engine=name,
            source=Endpoint(options={"database": PROJECTS["src"]}),
            target=Endpoint(options={"database": PROJECTS["dst"]}),
            databases=["sales"], **hop)
    if report is not None:
        h.report_dir = lambda db=None: report
    eng = engine_named(name, h)
    eng.clients = {"src": clients[PROJECTS["src"]],
                   "dst": clients[PROJECTS["dst"]]}
    return eng
