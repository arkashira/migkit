"""Which cloud runs a side, and how that cloud spells a change to it.

Read, never asked (backlog R21): the host a side is reached at names the
service for most managed databases - `*.rds.amazonaws.com`,
`*.postgres.database.azure.com`, `*.sql.tencentcdb.com`,
`*.rds.aliyuncs.com` - and the server says it where the address does not
(a CNAME, a private IP, a proxy in front): RDS and Aurora give their
administrator a role of their own (`rds_superuser`), Aurora answers
`aurora_version()`, Cloud SQL `cloudsqlsuperuser`, Azure
`azure_pg_admin`, AlloyDB `alloydbsuperuser` - and, read before a role,
which a copy restored elsewhere keeps, the settings each defines for
itself (`rds.*`, `cloudsql.*`, `azure.*`, `alloydb.*`); an RDS MySQL server runs
from `/rdsdbbin/`, Aurora MySQL has `aurora_version`, Cloud SQL MySQL
`cloudsql_*` variables. Only what a service says of itself names it - a
server that says nothing recognisable is "run by its owner", which is
also what the scripts below are written for. The managed-service
catalogue (`managed`) adds the addresses and the signs of every other
service it lists - a vendor's role, a variable only one service has -
after these, so what was named here before is named the same.

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
    (r"\.dsql(-[a-z0-9]+)?\.[a-z0-9-]+\.on\.aws$", "aws", "dsql"),
    (r"\.documents\.azure\.com$", "azure", "cosmos-nosql"),
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
    ("aws", "dsql"): "Amazon Aurora DSQL",
    ("azure", "cosmos-nosql"): "Azure Cosmos DB for NoSQL",
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
    #: other clouds the server's own signs name as well (a role two
    #: services create): the address decides between them
    alike: tuple = ()

    @property
    def label(self):
        if not self.cloud:
            return "run by its owner (no managed service names it)"
        if (self.cloud, self.service) in LABELS:
            return LABELS[(self.cloud, self.service)]
        from . import managed
        try:
            got = [s.label for s in managed.services().values()
                   if (s.provider, s.where) == (self.cloud, self.service)]
        except Exception:  # noqa: BLE001 - the catalogue unread
            got = []
        # several engines under one word: the provider's name for them all
        return (got[0] if len(got) == 1 else
                f"{managed.providers().get(self.cloud, {}).get('label', self.cloud)}"
                f" {self.service}" if got else
                f"{self.cloud} {self.service}".strip())

    def as_dict(self):
        return {"cloud": self.cloud, "service": self.service,
                "label": self.label, "evidence": list(self.evidence),
                "region": self.region}


#: an AWS region inside a service's address: `db.abc.eu-west-1.rds...`
AWS_REGION = re.compile(r"\.([a-z]{2}(?:-gov|-iso[a-z]?)?-[a-z]+-\d)\.")


#: a provider's pattern that takes every address of its own: its other
#: services' names, from the catalogue, are asked before it
CATCH_ALL = (r"\.tencentcdb\.com$", r"\.aliyuncs\.com$")


def _hosts():
    """`HOSTS`, with the managed-service catalogue's address names for the
    services these do not name (`managed.host_patterns`) - a provider's
    before its catch-all, the rest after."""
    from . import managed
    try:
        more = managed.host_patterns()
    except Exception:  # noqa: BLE001 - the catalogue unread: these stand
        more = []
    known = {p for p, _, _ in HOSTS}
    more = [h for h in more if h[0] not in known]
    out, placed = [], set()
    for entry in HOSTS:
        if entry[0] in CATCH_ALL:
            for h in more:
                if h[1] == entry[1] and h[0] not in placed:
                    out.append(h)
                    placed.add(h[0])
        out.append(entry)
    return out + [h for h in more if h[0] not in placed]


def from_host(host):
    """`Where` from the name a side is reached at, or an empty one."""
    name = str(host or "").strip().lower().rstrip(".")
    for pattern, cloud, service in _hosts():
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
#: PostgreSQL: the settings a service defines for itself, by their
#: prefix - a server's own settings, which a copy restored elsewhere does
#: not carry, where its roles (`PG_SIGNS`) come along with every dump of
#: the roles (audit 2026-10-01, K18)
PG_SETTINGS = {"rds": ("aws", "rds"), "cloudsql": ("gcp", "cloud-sql"),
               "azure": ("azure", "flexible"),
               "alloydb": ("gcp", "alloydb")}
PG_PROBE = ("select coalesce((select string_agg(rolname, ',') from pg_roles"
            " where rolname in ('rds_superuser', 'cloudsqlsuperuser',"
            " 'azure_pg_admin', 'alloydbsuperuser')), '') || chr(31) ||"
            " exists(select 1 from pg_proc where proname ="
            " 'aurora_version')::text || chr(31) || coalesce((select"
            " string_agg(distinct split_part(name, '.', 1), ',') from"
            " pg_settings where name ~ '^(rds|cloudsql|azure|alloydb)\\.'),"
            " '')")


def _signs(kind):
    """The managed-service catalogue's signs of one kind
    (`managed.signs`); none where the catalogue cannot be read."""
    from . import managed
    try:
        return managed.signs(kind)
    except Exception:  # noqa: BLE001 - the catalogue unread: ours stand
        return {}


def _listed(names):
    return ", ".join("'" + str(n).replace("'", "''") + "'" for n in names)


def _pg(eng, side):
    signs = {**_signs("pg_roles"), **PG_SIGNS}
    probe = PG_PROBE.replace(_listed(PG_SIGNS), _listed(sorted(signs)))
    roles, aurora, settings = (eng._psql(side, "postgres", probe).strip(
        ).split("\x1f") + ["", ""])[:3]
    roles = [r for r in roles.split(",") if r]
    aurora = aurora.strip() == "true"
    if aurora:
        return Where("aws", "aurora", ["the server (aurora_version())"])
    # a service's own settings first: its roles survive a dump of the
    # roles, so a server restored from one read as the service and its
    # page copy was refused
    own = sorted(s for s in settings.strip().split(",") if s in PG_SETTINGS)
    if own:
        cloud, service = PG_SETTINGS[own[0]]
        return Where(cloud, service, [f"the server (its {own[0]}.*"
                                      " settings)"],
                     alike=tuple(dict.fromkeys(
                         PG_SETTINGS[s][0] for s in own[1:]
                         if PG_SETTINGS[s][0] != cloud)))
    # the clouds' own roles first, as `PG_SIGNS` has them; where the roles
    # name more than one cloud, each is kept for the address to decide
    named = [(r, signs[r]) for r in sorted(roles, key=lambda r: (
        r not in PG_SIGNS, r)) if r in signs]
    if named:
        role, (cloud, service) = named[0]
        return Where(cloud, service, [f"the server (role {role})"],
                     alike=tuple(dict.fromkeys(
                         c for _, (c, _) in named if c != cloud)))
    return None


def _mysql(eng, side):
    variables = _signs("my_variables")
    comments = _signs("my_version_comment")
    asked = _listed(sorted({"aurora_version", "basedir", "version_comment",
                            *variables}))
    rows = dict(eng._q(side, "show global variables where variable_name in"
                             f" ({asked}) or variable_name like"
                             " 'cloudsql%'"))
    if rows.get("aurora_version"):
        return Where("aws", "aurora", ["the server (aurora_version "
                                       f"{rows['aurora_version']})"])
    if str(rows.get("basedir", "")).startswith("/rdsdbbin/"):
        return Where("aws", "rds", ["the server (it runs from /rdsdbbin/)"])
    named = sorted(k for k in rows if k.startswith("cloudsql"))
    if named:
        return Where("gcp", "cloud-sql", [f"the server ({named[0]})"])
    for name, (cloud, service) in sorted(variables.items()):
        if rows.get(name) not in (None, ""):
            return Where(cloud, service, [f"the server ({name})"])
    said = str(rows.get("version_comment") or "").lower()
    for text, (cloud, service) in sorted(comments.items()):
        if text.lower() in said:
            return Where(cloud, service, ["the server (its version says"
                                          f" {text})"])
    return None


def _mongo(eng, side):
    """A service's own field in `buildInfo`, where the catalogue names
    one; nothing is named by a field's absence (`variants`)."""
    fields = _signs("mongo_build_info")
    if not fields:
        return None
    got = eng._client(side).admin.command("buildInfo")
    for name, (cloud, service) in sorted(fields.items()):
        if name in got:
            return Where(cloud, service, [f"the server (buildInfo {name})"])
    return None


#: engine family -> how its server is asked
PROBES = {"postgres": _pg, "mysql": _mysql, "mongodb": _mongo}


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
    if by_host.cloud and by_host.cloud in by_server.alike:
        # the server's roles name two clouds, and the address one of them
        return Where(by_host.cloud, by_host.service,
                     by_host.evidence + by_server.evidence, by_host.region)
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
