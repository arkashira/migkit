"""Which cloud runs a side, and how that cloud spells a change to it.

Read, never asked (backlog R21): the host a side is reached at names the
service for most managed databases - `*.rds.amazonaws.com`,
`*.postgres.database.azure.com`, `*.sql.tencentcdb.com`,
`*.rds.aliyuncs.com` - and the server says it where the address does not
(a CNAME, a private IP, a proxy in front): RDS and Aurora give their
administrator a role of their own (`rds_superuser`), Aurora answers
`aurora_version()`, Cloud SQL `cloudsqlsuperuser`, Azure
`azure_pg_admin`, AlloyDB `alloydbsuperuser`; an RDS MySQL server runs
from `/rdsdbbin/`, Aurora MySQL has `aurora_version`, Cloud SQL MySQL
`cloudsql_*` variables. Only what a service says of itself names it - a
server that says nothing recognisable is "run by its owner", which is
also what the scripts below are written for.

What it is for is the second half: a setting a DBA must change is
changed differently on each - a parameter group on RDS (a cluster
parameter group on Aurora), a database flag on Cloud SQL, a server
parameter on Azure, the console's parameter settings on Tencent and
Alibaba, `ALTER SYSTEM` or the server's configuration file on a server
run by its owner - and `migkit.dba` writes each in its own words.
"""
import re
from dataclasses import dataclass, field

#: (pattern on the host, cloud, service), the first match wins; the order
#: puts a proxy and a reader before the instance names they also match
HOSTS = [
    (r"\.proxy-[a-z0-9]+\.[a-z0-9-]+\.rds\.amazonaws\.com(\.cn)?$",
     "aws", "rds-proxy"),
    (r"\.cluster-ro-[a-z0-9]+\.[a-z0-9-]+\.rds\.amazonaws\.com(\.cn)?$",
     "aws", "aurora-reader"),
    (r"\.cluster-(custom-)?[a-z0-9]+\.[a-z0-9-]+\.rds\.amazonaws\.com"
     r"(\.cn)?$", "aws", "aurora"),
    (r"\.[a-z0-9]+\.[a-z0-9-]+\.rds\.amazonaws\.com(\.cn)?$", "aws", "rds"),
    (r"\.docdb(-elastic)?\.amazonaws\.com$", "aws", "documentdb"),
    (r"\.cache\.amazonaws\.com(\.cn)?$", "aws", "elasticache"),
    (r"\.memorydb\.[a-z0-9-]+\.amazonaws\.com$", "aws", "memorydb"),
    (r"\.kafka(-serverless)?\.[a-z0-9-]+\.amazonaws\.com$", "aws", "msk"),
    (r"\.redshift(-serverless)?\.amazonaws\.com$", "aws", "redshift"),
    (r"\.(es|aoss)\.amazonaws\.com$", "aws", "opensearch"),
    (r"(^|\.)dynamodb\.[a-z0-9-]+\.amazonaws\.com$", "aws", "dynamodb"),
    (r"\.sql\.goog\.?$", "gcp", "cloud-sql"),
    (r"\.(postgres|mysql)\.database\.azure\.com$", "azure", "flexible"),
    (r"\.database\.windows\.net$", "azure", "sql-database"),
    (r"\.(mongo|mongocluster)\.cosmos\.azure\.com$", "azure", "cosmos"),
    (r"\.redis\.cache\.windows\.net$", "azure", "cache"),
    (r"\.servicebus\.windows\.net$", "azure", "event-hubs"),
    (r"\.tencentcdb\.com$", "tencent", "tencentdb"),
    (r"\.polardb\.rds\.aliyuncs\.com$", "alibaba", "polardb"),
    (r"\.aliyuncs\.com$", "alibaba", "apsaradb"),
]

#: what each is called where an operator reads it
LABELS = {
    ("aws", "rds"): "Amazon RDS", ("aws", "aurora"): "Amazon Aurora",
    ("aws", "aurora-reader"): "Amazon Aurora (its reader endpoint)",
    ("aws", "rds-proxy"): "Amazon RDS Proxy",
    ("aws", "documentdb"): "Amazon DocumentDB",
    ("aws", "elasticache"): "Amazon ElastiCache",
    ("aws", "memorydb"): "Amazon MemoryDB", ("aws", "msk"): "Amazon MSK",
    ("aws", "redshift"): "Amazon Redshift",
    ("aws", "opensearch"): "Amazon OpenSearch Service",
    ("aws", "dynamodb"): "Amazon DynamoDB",
    ("gcp", "cloud-sql"): "Google Cloud SQL", ("gcp", "alloydb"): "AlloyDB",
    ("azure", "flexible"): "Azure Database (flexible server)",
    ("azure", "sql-database"): "Azure SQL Database",
    ("azure", "cosmos"): "Azure Cosmos DB",
    ("azure", "cache"): "Azure Cache for Redis",
    ("azure", "event-hubs"): "Azure Event Hubs",
    ("tencent", "tencentdb"): "TencentDB",
    ("alibaba", "polardb"): "Alibaba PolarDB",
    ("alibaba", "apsaradb"): "Alibaba ApsaraDB",
}


@dataclass
class Where:
    """Who runs a side. `cloud` "" is its owner - a server nobody's
    service names."""
    cloud: str = ""
    service: str = ""
    evidence: list = field(default_factory=list)
    #: the region, where the address carries one (AWS's names do)
    region: str = ""

    @property
    def label(self):
        if not self.cloud:
            return "run by its owner (no managed service names it)"
        return LABELS.get((self.cloud, self.service),
                          f"{self.cloud} {self.service}".strip())

    def as_dict(self):
        return {"cloud": self.cloud, "service": self.service,
                "label": self.label, "evidence": list(self.evidence),
                "region": self.region}


#: an AWS region inside a service's address: `db.abc.eu-west-1.rds...`
AWS_REGION = re.compile(r"\.([a-z]{2}(?:-gov|-iso[a-z]?)?-[a-z]+-\d)\.")


def from_host(host):
    """`Where` from the name a side is reached at, or an empty one."""
    name = str(host or "").strip().lower().rstrip(".")
    for pattern, cloud, service in HOSTS:
        if re.search(pattern, name):
            m = AWS_REGION.search(name) if cloud == "aws" else None
            return Where(cloud, service, [f"the address ({name})"],
                         m.group(1) if m else "")
    return Where()


#: PostgreSQL: roles and settings only a service creates
PG_SIGNS = {
    "rds_superuser": ("aws", "rds"), "cloudsqlsuperuser": ("gcp", "cloud-sql"),
    "azure_pg_admin": ("azure", "flexible"),
    "alloydbsuperuser": ("gcp", "alloydb"),
}
PG_PROBE = ("select coalesce((select string_agg(rolname, ',') from pg_roles"
            " where rolname in ('rds_superuser', 'cloudsqlsuperuser',"
            " 'azure_pg_admin', 'alloydbsuperuser')), '') || chr(31) ||"
            " exists(select 1 from pg_proc where proname ="
            " 'aurora_version')::text")


def _pg(eng, side):
    roles, _, aurora = eng._psql(side, "postgres", PG_PROBE).strip(
        ).partition("\x1f")
    roles = [r for r in roles.split(",") if r]
    aurora = aurora.strip() == "true"
    if aurora:
        return Where("aws", "aurora", ["the server (aurora_version())"])
    for role in roles:
        cloud, service = PG_SIGNS[role]
        return Where(cloud, service, [f"the server (role {role})"])
    return None


def _mysql(eng, side):
    rows = dict(eng._q(side, "show global variables where variable_name in"
                             " ('aurora_version', 'basedir') or"
                             " variable_name like 'cloudsql%'"))
    if rows.get("aurora_version"):
        return Where("aws", "aurora", ["the server (aurora_version "
                                       f"{rows['aurora_version']})"])
    if str(rows.get("basedir", "")).startswith("/rdsdbbin/"):
        return Where("aws", "rds", ["the server (it runs from /rdsdbbin/)"])
    named = sorted(k for k in rows if k.startswith("cloudsql"))
    if named:
        return Where("gcp", "cloud-sql", [f"the server ({named[0]})"])
    return None


#: engine family -> how its server is asked
PROBES = {"postgres": _pg, "mysql": _mysql}


def of(eng, side, ep):
    """`Where` for one side: the server's word where it says one, the
    address's otherwise; where both say, both are the evidence. A probe
    that cannot run (a user who may not read the catalogue) leaves the
    address's answer."""
    by_host = from_host(ep.host)
    family = getattr(eng, "CANON_ENGINE", "") or ""
    probe = PROBES.get(family)
    by_server = None
    if probe is not None:
        try:
            by_server = probe(eng, side)
        except Exception:  # noqa: BLE001 - the address still says
            by_server = None
    if by_server is None:
        return by_host
    if by_host.cloud == by_server.cloud:
        # the address can be more exact (a reader endpoint), the server
        # surer; keep the address's service where it is the same cloud's
        service = (by_host.service if by_host.service in (
            "aurora-reader", "rds-proxy") else by_server.service)
        return Where(by_server.cloud, service,
                     by_host.evidence + by_server.evidence, by_host.region)
    return by_server


def managed(where):
    """Whether a DBA changes this side's settings through a cloud's
    console or API rather than on the server."""
    return bool(where and where.cloud)
