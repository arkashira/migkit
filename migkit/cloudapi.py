"""What a managed database's provider says and does about it, asked
through that provider's own API (backlog P0-SCENARIOS): the facts of the
instance - its class, CPU and memory, storage, version, maintenance window,
character set, the parameter group it runs with and whether other
instances share it, and the parameters asked for with whether they may be
changed and whether a change waits for a restart - a restore point made and
waited for until the provider says it is ready, the restore points the
provider keeps, a copy of the instance made from one, and parameters set
and waited for until they are applied.

One layer with an adapter per provider, so every module that needs a
provider (`managed`, `cutover`, `cloudseed`) asks the one thing the one
way, and a real run no longer scripts a snapshot by hand before a cutover.
What a product's API does not offer is said as such (`NotOffered`), never
imitated; what it offers is in each adapter's `PRODUCTS`, read without a
call (`offered`).

Credentials are found where the provider's own CLI finds them, in its
order - the environment, the CLI's profile files, the SDK's own file, the
instance's role - and `credentials()` says where in words; a role the hop
names is assumed over what was found. A chain that finds nothing says every
place it looked.

Every wait is `until`: asked every `POLL` seconds, for at most `WAIT`
(`MIGKIT_CLOUD_POLL` and `MIGKIT_CLOUD_WAIT` override both), and a failed
answer from the provider ends it at once.

**What is proved here and what only on a real account.** Every call is
driven against a stand-in that answers in the recorded shape of the
provider's API - built with the SDK's own models where it has them, so a
wrong field name fails - and every chain of credentials against a home
directory and an environment made for the test. Not provable here: the
permissions an account grants, the time a provider takes, and the values a
real instance holds.
"""
import datetime
import json
import os
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

#: seconds between two looks at something the provider is making, and the
#: most a make is waited for
POLL, WAIT = 20, 6 * 3600


def until(ask, what, log, poll=None, most=None):
    """Ask until `ask()` answers something other than None; the answer."""
    poll = poll if poll is not None else float(
        os.environ.get("MIGKIT_CLOUD_POLL", POLL))
    most = most or float(os.environ.get("MIGKIT_CLOUD_WAIT", WAIT))
    end, said = time.time() + most, 0.0
    while True:
        got = ask()
        if got is not None:
            return got
        if time.time() > end:
            raise SystemExit(f"{what} was not ready within {most:,.0f}s")
        if time.time() - said > 300:
            log(f"waiting for {what}")
            said = time.time()
        time.sleep(poll)


def _ready(ask, what, log, done, failed):
    """`until` the status `ask()` answers with its object, `(status, got)`,
    is one of `done` - the object; one of `failed` ends the wait at once."""
    done = {str(s).lower() for s in done}
    failed = {str(s).lower() for s in failed}

    def once():
        status, got = ask()
        status = str(status).lower()
        if status in failed:
            raise SystemExit(f"{what} is {status}")
        return (got if got is not None else status) if status in done \
            else None
    return until(once, what, log)


class NotOffered(SystemExit):
    """The provider's API offers no such call for this product."""


@dataclass
class Param:
    value: object = None
    #: whether the provider lets it be changed (None: it does not say)
    modifiable: object = None
    #: whether a change takes effect only after a restart
    restart: object = None


@dataclass
class Instance:
    provider: str
    product: str
    id: str
    engine: str = ""
    version: str = ""
    klass: str = ""
    cpu: object = None
    memory_gb: object = None
    storage_gb: object = None
    #: where the API states it (a spec's connection limit), else None
    max_connections: object = None
    charset: str = ""
    time_zone: str = ""
    #: the maintenance window, in words
    maintenance: str = ""
    host: str = ""
    port: object = None
    status: str = ""
    #: the parameter group or template it runs with, and whether another
    #: instance runs with it too (None: unknown)
    group: str = ""
    group_shared: object = None
    #: name -> Param, only the names asked for
    parameters: dict = field(default_factory=dict)


@dataclass
class RestorePoint:
    id: str
    #: "snapshot" | "backup" | "point in time"
    kind: str
    status: str = ""
    at: str = ""
    ready: bool = False


#: what each ability is, in the words a refusal says it
ABILITY = {"restore_point": "restore point made on request",
           "clone": "copy made from a restore point",
           "parameters": "change of parameters"}

DAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
        "Sunday")


def _now():
    return datetime.datetime.now(datetime.timezone.utc).replace(
        microsecond=0)


def _stamp(when):
    """An ISO time in UTC, as every restore point carries it; "" for none."""
    if when in (None, ""):
        return ""
    if isinstance(when, (int, float)):
        when = datetime.datetime.fromtimestamp(
            when / 1000 if when > 1e11 else when, datetime.timezone.utc)
    if isinstance(when, str):
        when = _time(when)
    if when.tzinfo is None:
        when = when.replace(tzinfo=datetime.timezone.utc)
    return when.astimezone(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


def _time(text, zone=datetime.timezone.utc):
    """A time a provider wrote - ISO with or without its zone, or
    "YYYY-MM-DD HH:MM:SS" in `zone` - as an aware datetime."""
    if isinstance(text, datetime.datetime):
        got = text
    else:
        text = str(text).strip().replace("Z", "+00:00")
        got = datetime.datetime.fromisoformat(text.replace(" ", "T", 1))
    return got if got.tzinfo else got.replace(tzinfo=zone)


def _point(at):
    """What a copy is made from: a RestorePoint, None for the latest, or a
    time - given as a RestorePoint of the kind "point in time"."""
    if at is None or isinstance(at, RestorePoint):
        return at
    when = _stamp(at)
    return RestorePoint(id=when, kind="point in time", at=when, ready=True)


def _gb(value, per=1):
    return None if value in (None, "") else round(float(value) / per, 1)


def _num(value):
    if value in (None, ""):
        return None
    try:
        got = float(value)
    except (TypeError, ValueError):
        return None
    return int(got) if got.is_integer() else got


def _host(url):
    """The host of an endpoint written as a URL, or the text itself."""
    from urllib.parse import urlparse
    got = urlparse(str(url or ""))
    return got.hostname or str(url or "")


class Adapter:
    """One provider's API for one product's instance (`conf["id"]`)."""
    provider = ""
    #: the provider as an operator reads it
    WHO = ""
    #: product -> {"restore_point": "snapshot" | "backup" | "point in time"
    #: | "", "clone": bool, "parameters": bool}
    PRODUCTS = {}
    #: {(product, ability): why the API has none}, said with the refusal
    WHY = {}

    def __init__(self, conf):
        self.conf = dict(conf or {})
        self.product = str(self.conf.get("product") or "")
        if self.product not in self.PRODUCTS:
            raise SystemExit(
                f"{self.WHO}'s API is asked here for"
                f" {', '.join(sorted(self.PRODUCTS))};"
                f" {self.product or 'no product'} is not one of them")
        self.id = str(self.conf.get("id") or "")
        self.region = str(self.conf.get("region") or "")
        self.role = str(self.conf.get("role") or "")
        self._found = None

    def credentials(self):
        """(the SDK's credentials, where they were found - in words),
        resolved the way the provider's own CLI resolves them."""
        if self._found is None:
            self._found = self._credentials()
        return self._found

    def _offered(self, ability):
        got = self.PRODUCTS[self.product][ability]
        if not got:
            why = self.WHY.get((self.product, ability))
            raise NotOffered(f"{self.WHO}'s API offers no"
                             f" {ABILITY[ability]} for {self.product}"
                             + (f": {why}" if why else ""))
        return got

    def instance(self, names=()):
        raise NotImplementedError

    def find(self, host, port=None):
        raise NotImplementedError

    def restore_point(self, name, log):
        """A restore point made and waited for until the provider says it
        is ready. A product whose nature is a point in time has one at any
        moment: now is the point, and nothing is asked of the provider."""
        kind = self._offered("restore_point")
        if kind == "point in time":
            now = _stamp(_now())
            return RestorePoint(id=now, kind=kind, at=now, ready=True)
        return self._make(name, log)

    def restore_points(self):
        raise NotImplementedError

    def clone(self, name, at, log):
        """A new instance `name` made from `at` - a RestorePoint, a time,
        or None for the latest - and waited for: {"host", "port", "id",
        "kind"}."""
        self._offered("clone")
        return self._clone(name, _point(at), log)

    def set_parameters(self, values, log):
        """Each of `values` set, and waited for until the provider says it
        is applied: {name: its value before}. Only ever behind an
        operator's yes."""
        self._offered("parameters")
        return self._set(dict(values), log) if values else {}

    def _latest(self, point):
        """The restore point a copy from "the latest" is made from, for a
        product that copies only from its restore points."""
        if point is not None:
            return point
        ready = [p for p in self.restore_points() if p.ready]
        if not ready:
            raise SystemExit(f"{self.id} has no restore point ready to copy"
                             " from: make one first")
        return max(ready, key=lambda p: p.at or "")

    def _no_time(self, point):
        """A product that copies only from its own restore points, asked
        for a time."""
        if point is not None and point.kind == "point in time":
            raise NotOffered(f"{self.WHO}'s API copies {self.product} only"
                             " from a restore point it keeps, not from a"
                             " point in time")
        return point


# --- AWS ---------------------------------------------------------------------

#: where the AWS CLI's chain found its credentials, in words
AWS_WHERE = {
    "env": "the environment (AWS_ACCESS_KEY_ID)",
    "shared-credentials-file": "the CLI's credentials file",
    "config-file": "the CLI's config file",
    "sso": "the CLI's single sign-on login",
    "login": "the CLI's console login",
    "assume-role": "a role the CLI's profile assumes",
    "assume-role-with-web-identity": "a web identity token",
    "custom-process": "a process the CLI's profile names",
    "container-role": "the container's role",
    "iam-role": "the instance's role",
    "explicit": "keys given to the client",
}


def _aws_code(e):
    return getattr(e, "response", {}).get("Error", {}).get("Code", "") \
        or type(e).__name__


def _pages(call, key, **kw):
    """Every item of a listing that pages by Marker or NextToken."""
    out = []
    while True:
        got = call(**kw)
        out += got.get(key) or []
        token = got.get("Marker") or got.get("NextToken")
        if not token:
            return out
        kw["Marker" if got.get("Marker") else "NextToken"] = token


class Aws(Adapter):
    """RDS and Aurora, DocumentDB, ElastiCache, MemoryDB, Redshift,
    DynamoDB, OpenSearch and MSK through their API. A client comes from
    migkit's one AWS client (`config._aws_client`: the CLI's chain, the
    hop's role assumed); a profile the hop names, through its session."""
    provider, WHO = "aws", "AWS"
    PRODUCTS = {
        "rds": {"restore_point": "snapshot", "clone": True,
                "parameters": True},
        "docdb": {"restore_point": "snapshot", "clone": True,
                  "parameters": True},
        "elasticache": {"restore_point": "snapshot", "clone": True,
                        "parameters": True},
        "memorydb": {"restore_point": "snapshot", "clone": True,
                     "parameters": True},
        "redshift": {"restore_point": "snapshot", "clone": True,
                     "parameters": True},
        "dynamodb": {"restore_point": "backup", "clone": True,
                     "parameters": False},
        "opensearch": {"restore_point": "", "clone": False,
                       "parameters": False},
        "msk": {"restore_point": "", "clone": False, "parameters": False},
    }
    WHY = {
        ("opensearch", "restore_point"):
            "a domain's snapshots are taken through the domain's own"
            " endpoint, into a repository registered there",
        ("opensearch", "clone"):
            "a domain is restored from its snapshots through its own"
            " endpoint",
        ("opensearch", "parameters"):
            "a domain's settings are its advanced options, changed with"
            " the whole domain configuration",
        ("msk", "restore_point"):
            "a cluster keeps no backup; its topics are copied",
        ("msk", "clone"): "a cluster keeps no backup to copy it from",
        ("msk", "parameters"):
            "a broker's settings are a configuration revision applied to"
            " the whole cluster",
        ("dynamodb", "parameters"): "a table has no server parameters",
    }
    SERVICE = {"rds": "rds", "docdb": "docdb", "elasticache": "elasticache",
               "memorydb": "memorydb", "redshift": "redshift",
               "dynamodb": "dynamodb", "opensearch": "opensearch",
               "msk": "kafka"}
    #: the waits' words for a status that will not become ready
    FAILED = ("failed", "incompatible-restore", "incompatible-parameters",
              "deleting", "deleted", "inaccessible-encryption-credentials")

    def _session(self):
        import boto3
        profile = self.conf.get("profile") or None
        got = boto3.Session(profile_name=profile)
        if profile and profile not in got.available_profiles:
            raise SystemExit(f"the AWS CLI has no profile {profile}")
        self.region = self.region or got.region_name or ""
        return got

    def _credentials(self):
        session = self._session()
        got = session.get_credentials()
        named = self.conf.get("profile") or os.environ.get("AWS_PROFILE") \
            or "default"
        if got is None:
            raise SystemExit(
                "no AWS credentials: looked in the environment"
                " (AWS_ACCESS_KEY_ID), the CLI's credentials and config"
                f" files (profile {named}), its single sign-on login, a web"
                " identity token, the container's role and the instance's"
                " role")
        where = AWS_WHERE.get(got.method, f"the CLI's chain ({got.method})")
        if got.method in ("shared-credentials-file", "config-file", "sso",
                          "login", "assume-role", "custom-process"):
            where += f", profile {named}"
        if not self.role:
            return got, where
        from botocore.credentials import Credentials
        made = session.client("sts", region_name=self.region or None) \
            .assume_role(RoleArn=self.role, RoleSessionName="migkit")[
            "Credentials"]
        return (Credentials(made["AccessKeyId"], made["SecretAccessKey"],
                            made["SessionToken"], method="assume-role"),
                f"the role {self.role}, assumed over {where}")

    def client(self, service=None):
        service = service or self.SERVICE[self.product]
        if not self.conf.get("profile"):
            from .config import _aws_client
            return _aws_client(service, self.region or None,
                               self.role or None)
        creds = self.credentials()[0].get_frozen_credentials()
        return self._session().client(
            service, region_name=self.region or None,
            aws_access_key_id=creds.access_key,
            aws_secret_access_key=creds.secret_key,
            aws_session_token=creds.token)

    # what the product is --------------------------------------------------

    def _rds(self):
        """("cluster" | "instance", its description), as `cloudseed`
        reads a source: a cluster first, then an instance."""
        rds = self.client()
        try:
            return "cluster", rds.describe_db_clusters(
                DBClusterIdentifier=self.id)["DBClusters"][0]
        except Exception as e:  # noqa: BLE001 - an instance, then
            if _aws_code(e) not in ("DBClusterNotFoundFault",
                                    "DBClusterNotFound"):
                raise
        return "instance", rds.describe_db_instances(
            DBInstanceIdentifier=self.id)["DBInstances"][0]

    def _writer(self, cluster):
        for m in cluster.get("DBClusterMembers") or ():
            if m.get("IsClusterWriter"):
                return self.client().describe_db_instances(
                    DBInstanceIdentifier=m["DBInstanceIdentifier"])[
                    "DBInstances"][0]
        return {}

    def _describe(self):
        """{"kind", "it": the product's description, "writer": a cluster's
        writer, "groups": [(kind, name)] its parameters are read from - a
        cluster's own group before its writer's - "members": an
        ElastiCache group's clusters}."""
        c, p = self.client(), self.product
        if p == "rds":
            kind, it = self._rds()
            if kind == "instance":
                return {"kind": kind, "it": it, "writer": it,
                        "groups": [("db", g["DBParameterGroupName"]) for g
                                   in it.get("DBParameterGroups") or ()]}
            writer = self._writer(it)
            return {"kind": kind, "it": it, "writer": writer,
                    "groups": [("cluster", it["DBClusterParameterGroup"])]
                    + [("db", g["DBParameterGroupName"]) for g
                       in writer.get("DBParameterGroups") or ()]}
        if p == "docdb":
            it = c.describe_db_clusters(DBClusterIdentifier=self.id)[
                "DBClusters"][0]
            return {"kind": "cluster", "it": it, "writer": self._writer(it),
                    "groups": [("cluster", it["DBClusterParameterGroup"])]}
        if p == "elasticache":
            it = c.describe_replication_groups(ReplicationGroupId=self.id)[
                "ReplicationGroups"][0]
            members = [c.describe_cache_clusters(CacheClusterId=m)[
                "CacheClusters"][0] for m in it.get("MemberClusters") or ()]
            return {"kind": "group", "it": it, "members": members,
                    "writer": members[0] if members else {},
                    "groups": [("cache", members[0]["CacheParameterGroup"][
                        "CacheParameterGroupName"])] if members else []}
        if p == "memorydb":
            it = c.describe_clusters(ClusterName=self.id)["Clusters"][0]
            return {"kind": "cluster", "it": it,
                    "groups": [("memorydb", it["ParameterGroupName"])]}
        if p == "redshift":
            it = c.describe_clusters(ClusterIdentifier=self.id)[
                "Clusters"][0]
            return {"kind": "cluster", "it": it,
                    "groups": [("redshift", g["ParameterGroupName"]) for g
                               in it.get("ClusterParameterGroups") or ()]}
        if p == "dynamodb":
            return {"kind": "table", "groups": [], "it": c.describe_table(
                TableName=self.id)["Table"]}
        if p == "opensearch":
            return {"kind": "domain", "groups": [], "it": c.describe_domain(
                DomainName=self.id)["DomainStatus"]}
        return {"kind": "cluster", "groups": [], "it": c.describe_cluster_v2(
            ClusterArn=self.id)["ClusterInfo"]}

    def _sizes(self, klass):
        """(vCPUs, memory in GB) of a class, as EC2 describes the type it
        runs on; (None, None) where EC2 sells no such type."""
        kind = re.sub(r"^(db|cache|kafka)\.|\.(search|elasticsearch)$", "",
                      str(klass or ""))
        if not kind:
            return None, None
        try:
            t = self.client("ec2").describe_instance_types(
                InstanceTypes=[kind])["InstanceTypes"][0]
        except Exception:  # noqa: BLE001 - a class of the product's own
            return None, None
        return t["VCpuInfo"]["DefaultVCpus"], _gb(
            t["MemoryInfo"]["SizeInMiB"], 1024)

    def instance(self, names=()):
        d = self._describe()
        it, w, p = d["it"], d.get("writer") or {}, self.product
        got = Instance(self.provider, p, self.id)
        if p in ("rds", "docdb"):
            got.engine, got.version = it.get("Engine", ""), it.get(
                "EngineVersion", "")
            got.klass = w.get("DBInstanceClass", "")
            got.maintenance = it.get("PreferredMaintenanceWindow", "")
            got.charset = it.get("CharacterSetName", "") or w.get(
                "CharacterSetName", "")
            got.time_zone = w.get("Timezone", "")
            if d["kind"] == "cluster":
                got.host, got.port = it.get("Endpoint", ""), it.get("Port")
                got.status = it.get("Status", "")
            else:
                got.host = (it.get("Endpoint") or {}).get("Address", "")
                got.port = (it.get("Endpoint") or {}).get("Port")
                got.status = it.get("DBInstanceStatus", "")
            if not str(got.engine).startswith(("aurora", "docdb")):
                got.storage_gb = _num(it.get("AllocatedStorage"))
        elif p == "elasticache":
            got.klass = it.get("CacheNodeType", "")
            got.engine, got.version = w.get("Engine", ""), w.get(
                "EngineVersion", "")
            got.maintenance = w.get("PreferredMaintenanceWindow", "")
            got.status = it.get("Status", "")
            ep = it.get("ConfigurationEndpoint") or next(
                (g.get("PrimaryEndpoint") for g in it.get("NodeGroups") or ()
                 if g.get("PrimaryEndpoint")), {})
            got.host, got.port = ep.get("Address", ""), ep.get("Port")
        elif p == "memorydb":
            got.klass, got.engine = it.get("NodeType", ""), it.get(
                "Engine", "redis")
            got.version = it.get("EngineVersion", "")
            got.maintenance = it.get("MaintenanceWindow", "")
            got.status = it.get("Status", "")
            ep = it.get("ClusterEndpoint") or {}
            got.host, got.port = ep.get("Address", ""), ep.get("Port")
        elif p == "redshift":
            got.klass, got.engine = it.get("NodeType", ""), "redshift"
            got.version = it.get("ClusterVersion", "")
            got.maintenance = it.get("PreferredMaintenanceWindow", "")
            got.status = it.get("ClusterStatus", "")
            got.storage_gb = _gb(it.get("TotalStorageCapacityInMegaBytes"),
                                 1024)
            ep = it.get("Endpoint") or {}
            got.host, got.port = ep.get("Address", ""), ep.get("Port")
        elif p == "dynamodb":
            got.engine, got.status = "dynamodb", it.get("TableStatus", "")
            got.storage_gb = _gb(it.get("TableSizeBytes"), 1 << 30)
        elif p == "opensearch":
            got.engine, got.version = "opensearch", it.get(
                "EngineVersion", "")
            got.klass = (it.get("ClusterConfig") or {}).get(
                "InstanceType", "")
            got.storage_gb = _num((it.get("EBSOptions") or {}).get(
                "VolumeSize"))
            got.host = it.get("Endpoint") or (it.get("Endpoints") or {}).get(
                "vpc", "")
            got.port = 443 if got.host else None
            got.status = "processing" if it.get("Processing") else "active"
        else:
            broker = (it.get("Provisioned") or {}).get(
                "BrokerNodeGroupInfo") or {}
            got.engine, got.klass = "kafka", broker.get("InstanceType", "")
            got.version = ((it.get("Provisioned") or {}).get(
                "CurrentBrokerSoftwareInfo") or {}).get("KafkaVersion", "")
            got.storage_gb = _num(((broker.get("StorageInfo") or {}).get(
                "EbsStorageInfo") or {}).get("VolumeSize"))
            got.status = it.get("State", "")
        got.cpu, got.memory_gb = self._sizes(got.klass)
        if d["groups"]:
            got.group = d["groups"][0][1]
            got.group_shared = any(self._shared(k, g) for k, g in
                                   d["groups"])
        rows = self._params(d, names)
        got.parameters = {n: Param(r["value"], r["modifiable"], r["restart"])
                          for n, r in rows.items()}
        return got

    def _shared(self, kind, group):
        """Whether an instance or cluster other than this one runs with the
        group - a change to it changes them too."""
        c = self.client()
        if kind == "db":
            mine = {self.id}
            if self.product == "rds":
                it = self._rds()[1]
                mine |= {m["DBInstanceIdentifier"] for m in
                         it.get("DBClusterMembers") or ()}
            return any(i["DBInstanceIdentifier"] not in mine and group in
                       [g["DBParameterGroupName"] for g in
                        i.get("DBParameterGroups") or ()]
                       for i in _pages(c.describe_db_instances,
                                       "DBInstances"))
        if kind == "cluster":
            return any(x["DBClusterIdentifier"] != self.id
                       and x.get("DBClusterParameterGroup") == group
                       for x in _pages(c.describe_db_clusters, "DBClusters"))
        if kind == "cache":
            return any(x.get("ReplicationGroupId") != self.id
                       and (x.get("CacheParameterGroup") or {}).get(
                           "CacheParameterGroupName") == group
                       for x in _pages(c.describe_cache_clusters,
                                       "CacheClusters"))
        if kind == "memorydb":
            return any(x["Name"] != self.id
                       and x.get("ParameterGroupName") == group
                       for x in _pages(c.describe_clusters, "Clusters"))
        return any(x["ClusterIdentifier"] != self.id and group in
                   [g["ParameterGroupName"] for g in
                    x.get("ClusterParameterGroups") or ()]
                   for x in _pages(c.describe_clusters, "Clusters"))

    def _group_rows(self, kind, group):
        """[(name, value, modifiable, restart)] of a parameter group."""
        c = self.client()
        if kind == "db":
            got = _pages(c.describe_db_parameters, "Parameters",
                         DBParameterGroupName=group)
        elif kind == "cluster":
            got = _pages(c.describe_db_cluster_parameters, "Parameters",
                         DBClusterParameterGroupName=group)
        elif kind == "cache":
            got = _pages(c.describe_cache_parameters, "Parameters",
                         CacheParameterGroupName=group)
        elif kind == "memorydb":
            got = _pages(c.describe_parameters, "Parameters",
                         ParameterGroupName=group)
        else:
            got = _pages(c.describe_cluster_parameters, "Parameters",
                         ParameterGroupName=group)
        out = []
        for r in got:
            apply = str(r.get("ApplyType") or r.get("ChangeType") or "")
            out.append((r.get("ParameterName") or r.get("Name"),
                        r.get("ParameterValue", r.get("Value")),
                        r.get("IsModifiable"),
                        True if apply in ("static", "requires-reboot")
                        else False if apply in ("dynamic", "immediate")
                        else None))
        return out

    def _params(self, d, names):
        """{name: {"value", "modifiable", "restart", "kind", "group"}} of the
        names asked, each from the first of the groups that has it."""
        want, out = set(names), {}
        for kind, group in d["groups"]:
            if not want - set(out):
                break
            for name, value, modifiable, restart in self._group_rows(
                    kind, group):
                if name in want and name not in out:
                    out[name] = {"value": value, "modifiable": modifiable,
                                 "restart": restart, "kind": kind,
                                 "group": group}
        return out

    def find(self, host, port=None):
        c, p, host = self.client(), self.product, str(host)

        def at(address, their_port):
            return address == host and (port is None or their_port is None
                                        or int(their_port) == int(port))
        if p in ("rds", "docdb"):
            kw = ({"Filters": [{"Name": "engine", "Values": ["docdb"]}]}
                  if p == "docdb" else {})
            for x in _pages(c.describe_db_clusters, "DBClusters", **kw):
                if at(x.get("Endpoint"), x.get("Port")) or at(
                        x.get("ReaderEndpoint"), x.get("Port")):
                    return x["DBClusterIdentifier"]
            for x in _pages(c.describe_db_instances, "DBInstances", **kw):
                ep = x.get("Endpoint") or {}
                if at(ep.get("Address"), ep.get("Port")):
                    return x.get("DBClusterIdentifier") or \
                        x["DBInstanceIdentifier"]
        elif p == "elasticache":
            for x in _pages(c.describe_replication_groups,
                            "ReplicationGroups"):
                eps = [x.get("ConfigurationEndpoint") or {}] + [
                    e for g in x.get("NodeGroups") or () for e in
                    (g.get("PrimaryEndpoint") or {},
                     g.get("ReaderEndpoint") or {})]
                if any(at(e.get("Address"), e.get("Port")) for e in eps):
                    return x["ReplicationGroupId"]
        elif p == "memorydb":
            for x in _pages(c.describe_clusters, "Clusters"):
                ep = x.get("ClusterEndpoint") or {}
                if at(ep.get("Address"), ep.get("Port")):
                    return x["Name"]
        elif p == "redshift":
            for x in _pages(c.describe_clusters, "Clusters"):
                ep = x.get("Endpoint") or {}
                if at(ep.get("Address"), ep.get("Port")):
                    return x["ClusterIdentifier"]
        elif p == "opensearch":
            names = [d["DomainName"] for d in
                     c.list_domain_names().get("DomainNames") or ()]
            for i in range(0, len(names), 5):
                for x in c.describe_domains(DomainNames=names[i:i + 5])[
                        "DomainStatusList"]:
                    eps = [x.get("Endpoint")] + list(
                        (x.get("Endpoints") or {}).values())
                    if host in eps:
                        return x["DomainName"]
        return None

    # restore points ---------------------------------------------------------

    def _make(self, name, log):
        c, p = self.client(), self.product
        what = f"the {self.PRODUCTS[p]['restore_point']} {name}"
        if p == "rds" and self._rds()[0] == "instance":
            c.create_db_snapshot(DBSnapshotIdentifier=name,
                                 DBInstanceIdentifier=self.id)
            log(f"{what} of {self.id} asked for")
            got = _ready(lambda: self._status(c.describe_db_snapshots(
                DBSnapshotIdentifier=name)["DBSnapshots"][0]), what, log,
                ("available",), self.FAILED)
            return self._snap(got)
        if p in ("rds", "docdb"):
            c.create_db_cluster_snapshot(DBClusterSnapshotIdentifier=name,
                                         DBClusterIdentifier=self.id)
            log(f"{what} of {self.id} asked for")
            got = _ready(lambda: self._status(c.describe_db_cluster_snapshots(
                DBClusterSnapshotIdentifier=name)["DBClusterSnapshots"][0]),
                what, log, ("available",), self.FAILED)
            return self._snap(got)
        if p == "elasticache":
            c.create_snapshot(ReplicationGroupId=self.id, SnapshotName=name)
            log(f"{what} of {self.id} asked for")
            got = _ready(lambda: self._status(c.describe_snapshots(
                SnapshotName=name)["Snapshots"][0]), what, log,
                ("available",), self.FAILED)
            return self._snap(got)
        if p == "memorydb":
            c.create_snapshot(ClusterName=self.id, SnapshotName=name)
            log(f"{what} of {self.id} asked for")
            got = _ready(lambda: self._status(c.describe_snapshots(
                SnapshotName=name)["Snapshots"][0]), what, log,
                ("available",), self.FAILED)
            return self._snap(got)
        if p == "redshift":
            c.create_cluster_snapshot(SnapshotIdentifier=name,
                                      ClusterIdentifier=self.id)
            log(f"{what} of {self.id} asked for")
            got = _ready(lambda: self._status(c.describe_cluster_snapshots(
                SnapshotIdentifier=name)["Snapshots"][0]), what, log,
                ("available",), self.FAILED)
            return self._snap(got)
        arn = c.create_backup(TableName=self.id, BackupName=name)[
            "BackupDetails"]["BackupArn"]
        log(f"{what} of {self.id} asked for")
        got = _ready(lambda: self._status(c.describe_backup(BackupArn=arn)[
            "BackupDescription"]["BackupDetails"]), what, log,
            ("available",), self.FAILED)
        return self._snap(got)

    @staticmethod
    def _status(x):
        return (x.get("Status") or x.get("SnapshotStatus")
                or x.get("BackupStatus") or ""), x

    def _snap(self, x):
        """A RestorePoint of what the product lists as one."""
        status = self._status(x)[0]
        when = (x.get("SnapshotCreateTime") or x.get("BackupCreationDateTime")
                or next((n.get("SnapshotCreateTime") for n in
                         x.get("NodeSnapshots") or ()), None))
        return RestorePoint(
            id=(x.get("DBSnapshotIdentifier")
                or x.get("DBClusterSnapshotIdentifier")
                or x.get("SnapshotIdentifier") or x.get("SnapshotName")
                or x.get("BackupArn") or x.get("Name") or ""),
            kind=self.PRODUCTS[self.product]["restore_point"],
            status=status, at=_stamp(when),
            ready=str(status).lower() == "available")

    def restore_points(self):
        c, p = self.client(), self.product
        if p == "rds" and self._rds()[0] == "instance":
            got = _pages(c.describe_db_snapshots, "DBSnapshots",
                         DBInstanceIdentifier=self.id)
        elif p in ("rds", "docdb"):
            got = _pages(c.describe_db_cluster_snapshots,
                         "DBClusterSnapshots", DBClusterIdentifier=self.id)
        elif p == "elasticache":
            got = _pages(c.describe_snapshots, "Snapshots",
                         ReplicationGroupId=self.id)
        elif p == "memorydb":
            got = _pages(c.describe_snapshots, "Snapshots",
                         ClusterName=self.id)
        elif p == "redshift":
            got = _pages(c.describe_cluster_snapshots, "Snapshots",
                         ClusterIdentifier=self.id)
        elif p == "dynamodb":
            got, kw = [], {"TableName": self.id}
            while True:
                page = c.list_backups(**kw)
                got += page.get("BackupSummaries") or []
                if not page.get("LastEvaluatedBackupArn"):
                    break
                kw["ExclusiveStartBackupArn"] = page["LastEvaluatedBackupArn"]
        else:
            self._offered("restore_point")
        return [self._snap(x) for x in got]

    # copies -----------------------------------------------------------------

    def _net(self):
        out = {}
        if self.conf.get("subnet"):
            out["DBSubnetGroupName"] = self.conf["subnet"]
        if self.conf.get("security_groups"):
            out["VpcSecurityGroupIds"] = list(self.conf["security_groups"])
        return out

    def _wait(self, describe, key, field_name, ident, what, log):
        return _ready(lambda: self._status(
            describe(**{field_name: ident})[key][0]), what, log,
            ("available", "active"), self.FAILED)

    def _clone(self, name, point, log):
        c, p = self.client(), self.product
        if p in ("rds", "docdb"):
            return self._clone_rds(c, name, point, log)
        if p == "dynamodb":
            if point is not None and point.kind == "backup":
                c.restore_table_from_backup(TargetTableName=name,
                                            BackupArn=point.id)
            else:
                c.restore_table_to_point_in_time(
                    SourceTableName=self.id, TargetTableName=name,
                    **({"RestoreDateTime": _time(point.at)} if point
                       else {"UseLatestRestorableTime": True}))
            log(f"the copy {name} of {self.id} asked for")
            _ready(lambda: (c.describe_table(TableName=name)["Table"][
                "TableStatus"], True), f"the copy {name}", log,
                ("active",),
                ("deleting", "inaccessible_encryption_credentials"))
            return {"host": f"dynamodb.{self.region}.amazonaws.com",
                    "port": 443, "id": name, "kind": "table"}
        snap = self._latest(self._no_time(point)).id
        d = self._describe()
        it, w = d["it"], d.get("writer") or {}
        if p == "elasticache":
            c.create_replication_group(
                ReplicationGroupId=name,
                ReplicationGroupDescription=f"a copy of {self.id}",
                SnapshotName=snap,
                CacheNodeType=self.conf.get("spec") or it["CacheNodeType"],
                Engine=w.get("Engine", "redis"),
                CacheSubnetGroupName=self.conf.get("subnet")
                or w.get("CacheSubnetGroupName"),
                SecurityGroupIds=list(self.conf.get("security_groups") or [
                    g["SecurityGroupId"] for g in w.get("SecurityGroups")
                    or ()]))
            log(f"the copy {name} of {self.id} asked for")
            got = self._wait(c.describe_replication_groups,
                             "ReplicationGroups", "ReplicationGroupId", name,
                             f"the copy {name}", log)
            ep = got.get("ConfigurationEndpoint") or next(
                (g.get("PrimaryEndpoint") for g in got.get("NodeGroups")
                 or () if g.get("PrimaryEndpoint")), {})
            return {"host": ep.get("Address", ""), "port": ep.get("Port"),
                    "id": name, "kind": "replication group"}
        if p == "memorydb":
            c.create_cluster(
                ClusterName=name, NodeType=self.conf.get("spec")
                or it["NodeType"], ACLName=it["ACLName"], SnapshotName=snap,
                SubnetGroupName=self.conf.get("subnet")
                or it.get("SubnetGroupName"),
                SecurityGroupIds=list(self.conf.get("security_groups") or [
                    g["SecurityGroupId"] for g in it.get("SecurityGroups")
                    or ()]))
            log(f"the copy {name} of {self.id} asked for")
            got = self._wait(c.describe_clusters, "Clusters", "ClusterName",
                             name, f"the copy {name}", log)
            ep = got.get("ClusterEndpoint") or {}
            return {"host": ep.get("Address", ""), "port": ep.get("Port"),
                    "id": name, "kind": "cluster"}
        c.restore_from_cluster_snapshot(
            ClusterIdentifier=name, SnapshotIdentifier=snap,
            SnapshotClusterIdentifier=self.id,
            ClusterSubnetGroupName=self.conf.get("subnet")
            or it.get("ClusterSubnetGroupName"),
            VpcSecurityGroupIds=list(self.conf.get("security_groups") or [
                g["VpcSecurityGroupId"] for g in it.get("VpcSecurityGroups")
                or ()]))
        log(f"the copy {name} of {self.id} asked for")
        got = self._wait(c.describe_clusters, "Clusters", "ClusterIdentifier",
                         name, f"the copy {name}", log)
        ep = got.get("Endpoint") or {}
        return {"host": ep.get("Address", ""), "port": ep.get("Port"),
                "id": name, "kind": "cluster"}

    def _clone_rds(self, c, name, point, log):
        """An RDS instance restored to a time or from a snapshot; a cluster
        cloned copy-on-write for the latest, restored to a time or from a
        snapshot otherwise, and given a writer as `cloudseed` gives one."""
        d = self._describe()
        it, w = d["it"], d.get("writer") or {}
        snap = point is not None and point.kind == "snapshot"
        if d["kind"] == "instance":
            extra = {"DBInstanceClass": self.conf["spec"]} \
                if self.conf.get("spec") else {}
            if snap:
                c.restore_db_instance_from_db_snapshot(
                    DBInstanceIdentifier=name, DBSnapshotIdentifier=point.id,
                    **extra, **self._net())
            else:
                c.restore_db_instance_to_point_in_time(
                    SourceDBInstanceIdentifier=self.id,
                    TargetDBInstanceIdentifier=name,
                    **({"RestoreTime": _time(point.at)} if point
                       else {"UseLatestRestorableTime": True}),
                    **extra, **self._net())
            log(f"the copy {name} of {self.id} asked for")
            got = self._wait(c.describe_db_instances, "DBInstances",
                             "DBInstanceIdentifier", name,
                             f"the copy {name}", log)
            ep = got.get("Endpoint") or {}
            return {"host": ep.get("Address", ""), "port": ep.get("Port"),
                    "id": name, "kind": "instance"}
        if snap:
            c.restore_db_cluster_from_snapshot(
                DBClusterIdentifier=name, SnapshotIdentifier=point.id,
                Engine=it["Engine"], **self._net())
        elif point is None:
            c.restore_db_cluster_to_point_in_time(
                DBClusterIdentifier=name, SourceDBClusterIdentifier=self.id,
                RestoreType="copy-on-write", UseLatestRestorableTime=True,
                **self._net())
        else:
            c.restore_db_cluster_to_point_in_time(
                DBClusterIdentifier=name, SourceDBClusterIdentifier=self.id,
                RestoreType="full-copy", RestoreToTime=_time(point.at),
                **self._net())
        log(f"the copy {name} of {self.id} asked for")
        self._wait(c.describe_db_clusters, "DBClusters",
                   "DBClusterIdentifier", name, f"the copy {name}", log)
        writer = f"{name}-1"
        c.create_db_instance(DBInstanceIdentifier=writer,
                             DBClusterIdentifier=name, Engine=it["Engine"],
                             DBInstanceClass=self.conf.get("spec")
                             or w.get("DBInstanceClass"))
        self._wait(c.describe_db_instances, "DBInstances",
                   "DBInstanceIdentifier", writer, f"the writer {writer}",
                   log)
        got = self._wait(c.describe_db_clusters, "DBClusters",
                         "DBClusterIdentifier", name, f"the copy {name}", log)
        return {"host": got.get("Endpoint", ""), "port": got.get("Port"),
                "id": name, "kind": "cluster"}

    # parameters -------------------------------------------------------------

    def _set(self, values, log):
        d = self._describe()
        rows = self._params(d, values)
        for n in values:
            if n not in rows:
                raise SystemExit(f"{self.id} has no parameter {n} in its"
                                 " parameter groups")
            if rows[n]["modifiable"] is False:
                raise SystemExit(f"{n} cannot be changed on {self.id}")
            if rows[n]["group"].startswith("default."):
                raise SystemExit(
                    f"{self.id} runs with the provider's default parameter"
                    f" group {rows[n]['group']}, which is never changed:"
                    " give it a group of its own first")
        c = self.client()
        by = {}
        for n, v in values.items():
            by.setdefault((rows[n]["kind"], rows[n]["group"]), []).append(
                (n, str(v), rows[n]["restart"]))
        for (kind, group), items in by.items():
            if self._shared(kind, group):
                log(f"the parameter group {group} is shared: every instance"
                    " that runs with it takes the change")
            for i in range(0, len(items), 20):
                part = items[i:i + 20]
                if kind in ("db", "cluster"):
                    ps = [{"ParameterName": n, "ParameterValue": v,
                           "ApplyMethod": "pending-reboot" if restart
                           else "immediate"} for n, v, restart in part]
                    if kind == "db":
                        c.modify_db_parameter_group(
                            DBParameterGroupName=group, Parameters=ps)
                    else:
                        c.modify_db_cluster_parameter_group(
                            DBClusterParameterGroupName=group, Parameters=ps)
                    continue
                pairs = [{"ParameterName": n, "ParameterValue": v}
                         for n, v, _ in part]
                if kind == "cache":
                    c.modify_cache_parameter_group(
                        CacheParameterGroupName=group,
                        ParameterNameValues=pairs)
                elif kind == "memorydb":
                    c.update_parameter_group(ParameterGroupName=group,
                                             ParameterNameValues=pairs)
                else:
                    c.modify_cluster_parameter_group(
                        ParameterGroupName=group, Parameters=pairs)
        log(f"{', '.join(values)} asked to change on {self.id}")

        def ask():
            now = self._params(self._describe(), values)
            if any(str(now[n]["value"]) != str(v) for n, v in
                   values.items()):
                return None
            states = self._apply_states()
            return None if "applying" in states else states
        states = until(ask, f"the parameters of {self.id}", log)
        waiting = [n for n in values if rows[n]["restart"]]
        if "pending-reboot" in states and waiting:
            log(f"{', '.join(waiting)} take effect at {self.id}'s next"
                " restart")
        return {n: rows[n]["value"] for n in values}

    def _apply_states(self):
        """The provider's word, per member, for whether its parameter
        groups are applied: in-sync, applying, pending-reboot."""
        d, p = self._describe(), self.product
        it, w = d["it"], d.get("writer") or {}
        if p in ("rds", "docdb"):
            states = [g.get("ParameterApplyStatus") for g in
                      w.get("DBParameterGroups") or ()]
            if d["kind"] == "cluster":
                states += [m.get("DBClusterParameterGroupStatus") for m in
                           it.get("DBClusterMembers") or ()]
        elif p == "elasticache":
            states = [(m.get("CacheParameterGroup") or {}).get(
                "ParameterApplyStatus") for m in d["members"]]
        elif p == "memorydb":
            states = [it.get("ParameterGroupStatus")]
        else:
            states = [g.get("ParameterApplyStatus") for g in
                      it.get("ClusterParameterGroups") or ()]
        return {str(s or "in-sync").lower() for s in states}


# --- Google Cloud ------------------------------------------------------------

GCP_SCOPE = "https://www.googleapis.com/auth/cloud-platform"


def _flag(value):
    """True, False or None for a yes/no a provider wrote as a bool or a
    word."""
    if value is None or value == "":
        return None
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in ("true", "1", "yes", "on")


class Gcp(Adapter):
    """Cloud SQL, AlloyDB, Memorystore and BigQuery through their
    discovery-built clients, as `cloudseed.Google` builds Cloud SQL's. A
    call is named by its resource path (`backupRuns.insert`)."""
    provider, WHO = "gcp", "Google Cloud"
    PRODUCTS = {
        "cloudsql": {"restore_point": "backup", "clone": True,
                     "parameters": True},
        "alloydb": {"restore_point": "backup", "clone": True,
                    "parameters": True},
        "memorystore": {"restore_point": "", "clone": False,
                        "parameters": True},
        "bigquery": {"restore_point": "point in time", "clone": False,
                     "parameters": False},
    }
    WHY = {
        ("memorystore", "restore_point"):
            "an instance is exported to a storage bucket as a file",
        ("memorystore", "clone"):
            "an instance is imported from an exported file",
        ("bigquery", "clone"):
            "a dataset is copied table by table, not as a whole",
        ("bigquery", "parameters"): "a dataset has no server parameters",
    }
    API = {"cloudsql": ("sqladmin", "v1beta4"), "alloydb": ("alloydb", "v1"),
           "memorystore": ("redis", "v1"), "bigquery": ("bigquery", "v2")}
    PORTS = {"postgres": 5432, "mysql": 3306, "sqlserver": 1433}

    def __init__(self, conf):
        super().__init__(conf)
        self._api = None
        self.project = str(self.conf.get("project") or "")

    def _credentials(self):
        import google.auth
        from google.auth import _cloud_sdk
        from google.auth.exceptions import DefaultCredentialsError
        try:
            creds, project = google.auth.default(scopes=[GCP_SCOPE])
        except DefaultCredentialsError:
            raise SystemExit(
                "no Google Cloud credentials: looked at the key file"
                " GOOGLE_APPLICATION_CREDENTIALS names, gcloud's application"
                " default login and the instance's service account") \
                from None
        self.project = self.project or project or ""
        where = ("the key file GOOGLE_APPLICATION_CREDENTIALS names"
                 if os.environ.get("GOOGLE_APPLICATION_CREDENTIALS") else
                 "gcloud's application default login" if Path(
                     _cloud_sdk.get_application_default_credentials_path()
                 ).exists() else "the instance's service account")
        if not self.role:
            return creds, where
        from google.auth import impersonated_credentials
        return (impersonated_credentials.Credentials(
            source_credentials=creds, target_principal=self.role,
            target_scopes=[GCP_SCOPE]),
            f"the service account {self.role}, impersonated over {where}")

    def _project(self):
        self.credentials()
        if not self.project:
            raise SystemExit("no Google Cloud project: name it (project) or"
                             " set gcloud's default project")
        return self.project

    def api(self):
        if self._api is None:
            from googleapiclient import discovery
            name, version = self.API[self.product]
            self._api = discovery.build(name, version,
                                        credentials=self.credentials()[0],
                                        cache_discovery=False)
        return self._api

    def _call(self, path, **kw):
        node = self.api()
        *chain, method = path.split(".")
        for resource in chain:
            node = getattr(node, resource)()
        return getattr(node, method)(**kw).execute()

    def _operation(self, op, what, log):
        """An operation waited for until done; one that ends in an error
        ends the wait."""
        if self.product == "cloudsql":
            def ask():
                got = self._call("operations.get", project=self._project(),
                                 operation=op["name"])
                if got.get("status") != "DONE":
                    return None
                errs = (got.get("error") or {}).get("errors") or []
                if errs:
                    raise SystemExit(f"{what} failed: " + "; ".join(
                        e.get("message", "") for e in errs))
                return got
        else:
            def ask():
                got = self._call("projects.locations.operations.get",
                                 name=op["name"])
                if not got.get("done"):
                    return None
                if got.get("error"):
                    raise SystemExit(f"{what} failed:"
                                     f" {got['error'].get('message', '')}")
                return got
        return until(ask, what, log)

    def _where(self):
        return f"projects/{self._project()}/locations/{self.region}"

    def _named(self, kind, ident=None):
        return f"{self._where()}/{kind}/{ident or self.id}"

    @staticmethod
    def _version(said):
        engine, _, rest = str(said or "").partition("_")
        return engine.lower(), rest.replace("_", ".")

    @staticmethod
    def _window(day, hours):
        """"Monday 03:00 UTC" for a weekly window; the provider's choice
        where it names no day."""
        if isinstance(day, str) and day and day.upper() != \
                "DAY_OF_WEEK_UNSPECIFIED":
            day = day.title()
        elif isinstance(day, int) and 1 <= day <= 7:
            day = DAYS[day - 1]
        else:
            return "any day, the provider's choice"
        return f"{day} {int(hours or 0):02d}:00 UTC"

    # Cloud SQL and AlloyDB --------------------------------------------------

    def _sql(self, name=None):
        return self._call("instances.get", project=self._project(),
                          instance=name or self.id)

    def _address(self, inst):
        want = str(self.conf.get("address") or "PRIVATE").upper()
        ips = inst.get("ipAddresses") or []
        pick = [a["ipAddress"] for a in ips if a.get("type") == want] or \
            [a["ipAddress"] for a in ips]
        return pick[0] if pick else inst.get("dnsName", "")

    def _sql_flags(self, version):
        """{flag: whether setting it restarts the instance} of the flags
        Cloud SQL lets an instance of `version` set."""
        got = self._call("flags.list", databaseVersion=version)
        return {f["name"]: bool(f.get("requiresRestart"))
                for f in got.get("items") or ()}

    def _tier(self, tier):
        m = re.match(r"db-custom-(\d+)-(\d+)$", str(tier))
        if m:
            return int(m.group(1)), _gb(m.group(2), 1024)
        for t in self._call("tiers.list", project=self._project()).get(
                "items") or ():
            if t.get("tier") == tier:
                m = re.match(r"db-perf-optimized-N-(\d+)$", str(tier))
                return (int(m.group(1)) if m else None,
                        _gb(t.get("RAM"), 1 << 30))
        return None, None

    def _primary(self, cluster=None):
        got = self._call("projects.locations.clusters.instances.list",
                         parent=cluster or self._named("clusters"))
        for i in got.get("instances") or ():
            if i.get("instanceType") == "PRIMARY":
                return i
        raise SystemExit(f"{cluster or self.id} has no primary instance")

    def _alloy_flags(self):
        got = self._call("projects.locations.supportedDatabaseFlags.list",
                         parent=self._where())
        return {f.get("flagName"): bool(f.get("requiresDbRestart"))
                for f in got.get("supportedDatabaseFlags") or ()}

    # what the product is ---------------------------------------------------

    def instance(self, names=()):
        got = Instance(self.provider, self.product, self.id)
        p = self.product
        if p == "cloudsql":
            it = self._sql()
            s = it.get("settings") or {}
            got.engine, got.version = self._version(it.get("databaseVersion"))
            got.klass = s.get("tier", "")
            got.cpu, got.memory_gb = self._tier(got.klass)
            got.storage_gb = _num(s.get("dataDiskSizeGb"))
            mw = s.get("maintenanceWindow") or {}
            got.maintenance = self._window(mw.get("day"), mw.get("hour"))
            got.time_zone, got.charset = s.get("timeZone", ""), s.get(
                "collation", "")
            got.host, got.port = self._address(it), self.PORTS.get(
                got.engine)
            got.status, got.group, got.group_shared = it.get(
                "state", ""), "", False
            if names:
                flags = {f["name"]: f.get("value") for f in
                         s.get("databaseFlags") or ()}
                catalog = self._sql_flags(it.get("databaseVersion"))
                got.parameters = {n: Param(flags.get(n), n in catalog,
                                           catalog.get(n)) for n in names}
        elif p == "alloydb":
            c = self._call("projects.locations.clusters.get",
                           name=self._named("clusters"))
            i = self._primary()
            got.engine, got.version = self._version(c.get("databaseVersion"))
            mc = i.get("machineConfig") or {}
            got.klass = mc.get("machineType") or f"{mc.get('cpuCount')} vCPU"
            got.cpu = _num(mc.get("cpuCount"))
            windows = (c.get("maintenanceUpdatePolicy") or {}).get(
                "maintenanceWindows") or [{}]
            got.maintenance = self._window(windows[0].get("day"), (
                windows[0].get("startTime") or {}).get("hours"))
            got.host, got.port = i.get("ipAddress", ""), 5432
            got.status, got.group, got.group_shared = c.get(
                "state", ""), "", False
            if names:
                flags = i.get("databaseFlags") or {}
                catalog = self._alloy_flags()
                got.parameters = {n: Param(flags.get(n), n in catalog,
                                           catalog.get(n)) for n in names}
        elif p == "memorystore":
            it = self._call("projects.locations.instances.get",
                            name=self._named("instances"))
            got.engine, got.version = self._version(it.get("redisVersion"))
            got.klass, got.memory_gb = it.get("tier", ""), _num(it.get(
                "memorySizeGb"))
            w = ((it.get("maintenancePolicy") or {}).get(
                "weeklyMaintenanceWindow") or [{}])[0]
            got.maintenance = self._window(w.get("day"), (
                w.get("startTime") or {}).get("hours"))
            got.host, got.port = it.get("host", ""), it.get("port")
            got.status, got.group, got.group_shared = it.get(
                "state", ""), "", False
            configs = it.get("redisConfigs") or {}
            got.parameters = {n: Param(configs.get(n)) for n in names}
        else:
            it = self._call("datasets.get", projectId=self._project(),
                            datasetId=self.id)
            got.engine, got.host, got.port = "bigquery", \
                "bigquery.googleapis.com", 443
            got.charset = it.get("defaultCollation", "")
            got.status = "available"
        return got

    def find(self, host, port=None):
        host, p = str(host), self.product
        if p == "cloudsql":
            kw = {"project": self._project()}
            while True:
                page = self._call("instances.list", **kw)
                for it in page.get("items") or ():
                    if host in [a.get("ipAddress") for a in
                                it.get("ipAddresses") or ()] + [
                            it.get("dnsName")]:
                        return it["name"]
                if not page.get("nextPageToken"):
                    return None
                kw["pageToken"] = page["nextPageToken"]
        if p == "alloydb":
            for c in self._call("projects.locations.clusters.list",
                                parent=self._where()).get("clusters") or ():
                for i in self._call(
                        "projects.locations.clusters.instances.list",
                        parent=c["name"]).get("instances") or ():
                    if i.get("ipAddress") == host:
                        return c["name"].rsplit("/", 1)[-1]
            return None
        if p == "memorystore":
            for it in self._call("projects.locations.instances.list",
                                 parent=self._where()).get(
                    "instances") or ():
                if it.get("host") == host:
                    return it["name"].rsplit("/", 1)[-1]
        return None

    # restore points --------------------------------------------------------

    @staticmethod
    def _run(r):
        return RestorePoint(id=str(r.get("id")), kind="backup",
                            status=r.get("status", ""),
                            at=_stamp(r.get("endTime") or r.get("startTime")),
                            ready=r.get("status") == "SUCCESSFUL")

    @staticmethod
    def _alloy_backup(b):
        return RestorePoint(id=b["name"].rsplit("/", 1)[-1], kind="backup",
                            status=b.get("state", ""),
                            at=_stamp(b.get("createCompletionTime")
                                      or b.get("createTime")),
                            ready=b.get("state") == "READY")

    def _make(self, name, log):
        what = f"the backup {name}"
        if self.product == "cloudsql":
            op = self._call("backupRuns.insert", project=self._project(),
                            instance=self.id, body={"description": name})
            log(f"{what} of {self.id} asked for")
            done = self._operation(op, what, log)
            made = str((done.get("backupContext") or {}).get("backupId")
                       or "")
            for r in self._runs():
                if (made and str(r.get("id")) == made) or (
                        not made and r.get("description") == name):
                    return self._run(r)
            raise SystemExit(f"{what} was made, and the provider does not"
                             " list it")
        op = self._call("projects.locations.backups.create",
                        parent=self._where(), backupId=name,
                        body={"clusterName": self._named("clusters"),
                              "type": "ON_DEMAND"})
        log(f"{what} of {self.id} asked for")
        self._operation(op, what, log)
        return self._alloy_backup(self._call(
            "projects.locations.backups.get",
            name=self._named("backups", name)))

    def _runs(self):
        out, kw = [], {"project": self._project(), "instance": self.id}
        while True:
            page = self._call("backupRuns.list", **kw)
            out += page.get("items") or []
            if not page.get("nextPageToken"):
                return out
            kw["pageToken"] = page["nextPageToken"]

    def restore_points(self):
        p = self.product
        if p == "cloudsql":
            return [self._run(r) for r in self._runs()]
        if p == "alloydb":
            got = self._call(
                "projects.locations.backups.list", parent=self._where(),
                filter=f'clusterName="{self._named("clusters")}"')
            return [self._alloy_backup(b) for b in got.get("backups") or ()]
        if p == "bigquery":
            it = self._call("datasets.get", projectId=self._project(),
                            datasetId=self.id)
            hours = int(it.get("maxTimeTravelHours") or 168)
            first = _stamp(_now() - datetime.timedelta(hours=hours))
            return [RestorePoint(id=first, kind="point in time", at=first,
                                 ready=True)]
        self._offered("restore_point")
        return []

    # copies ----------------------------------------------------------------

    def _clone(self, name, point, log):
        what = f"the copy {name}"
        if self.product == "cloudsql":
            if point is not None and point.kind == "backup":
                raise NotOffered(
                    "Google Cloud's API clones cloudsql to a point in time;"
                    " a backup is restored only into an instance that"
                    " exists, so the copy is asked for at a time")
            body = {"cloneContext": {"kind": "sql#cloneContext",
                                     "destinationInstanceName": name}}
            if point is not None:
                body["cloneContext"]["pointInTime"] = point.at
            op = self._call("instances.clone", project=self._project(),
                            instance=self.id, body=body)
            log(f"{what} of {self.id} asked for")
            self._operation(op, what, log)
            it = self._sql(name)
            return {"host": self._address(it),
                    "port": self.PORTS.get(self._version(
                        it.get("databaseVersion"))[0]),
                    "id": name, "kind": "instance"}
        c = self._call("projects.locations.clusters.get",
                       name=self._named("clusters"))
        body = {"clusterId": name, "cluster": {
            "networkConfig": c.get("networkConfig")
            or {"network": c.get("network")}}}
        if point is not None and point.kind == "backup":
            body["backupSource"] = {"backupName": self._named("backups",
                                                              point.id)}
        else:
            body["continuousBackupSource"] = {
                "cluster": self._named("clusters"),
                "pointInTime": point.at if point else _stamp(_now())}
        op = self._call("projects.locations.clusters.restore",
                        parent=self._where(), body=body)
        log(f"{what} of {self.id} asked for")
        self._operation(op, what, log)
        cpu = self.conf.get("spec") or (self._primary().get(
            "machineConfig") or {}).get("cpuCount")
        op = self._call("projects.locations.clusters.instances.create",
                        parent=self._named("clusters", name),
                        instanceId=f"{name}-primary",
                        body={"instanceType": "PRIMARY",
                              "machineConfig": {"cpuCount": int(cpu)}})
        self._operation(op, f"the primary of {name}", log)
        return {"host": self._primary(self._named("clusters", name)).get(
                    "ipAddress", ""), "port": 5432, "id": name,
                "kind": "cluster"}

    # parameters ------------------------------------------------------------

    def _set(self, values, log):
        what = f"the parameters of {self.id}"
        wanted = {n: str(v) for n, v in values.items()}
        if self.product == "cloudsql":
            it = self._sql()
            flags = {f["name"]: f.get("value") for f in (it.get(
                "settings") or {}).get("databaseFlags") or ()}
            catalog = self._sql_flags(it.get("databaseVersion"))
        elif self.product == "alloydb":
            primary = self._primary()
            flags = dict(primary.get("databaseFlags") or {})
            catalog = self._alloy_flags()
        else:
            it = self._call("projects.locations.instances.get",
                            name=self._named("instances"))
            flags, catalog = dict(it.get("redisConfigs") or {}), None
        for n in values:
            if catalog is not None and n not in catalog:
                raise SystemExit(f"{n} is no flag the provider lets"
                                 f" {self.id} set")
        before = {n: flags.get(n) for n in values}
        merged = {**flags, **wanted}
        if self.product == "cloudsql":
            op = self._call("instances.patch", project=self._project(),
                            instance=self.id, body={"settings": {
                                "databaseFlags": [
                                    {"name": k, **({"value": v} if v
                                                   is not None else {})}
                                    for k, v in merged.items()]}})
        elif self.product == "alloydb":
            op = self._call("projects.locations.clusters.instances.patch",
                            name=primary["name"], updateMask="databaseFlags",
                            body={"databaseFlags": merged})
        else:
            op = self._call("projects.locations.instances.patch",
                            name=self._named("instances"),
                            updateMask="redisConfigs",
                            body={"redisConfigs": merged})
        restart = [n for n in values if catalog and catalog.get(n)]
        log(f"{', '.join(values)} asked to change on {self.id}"
            + (f"; the provider restarts it for {', '.join(restart)}"
               if restart else ""))
        self._operation(op, what, log)
        return before


# --- Azure -------------------------------------------------------------------

#: which of the default chain's members found the credentials, in words
AZURE_WHERE = {
    "EnvironmentCredential":
        "the environment's service principal (AZURE_CLIENT_ID)",
    "WorkloadIdentityCredential":
        "the workload identity (AZURE_FEDERATED_TOKEN_FILE)",
    "ManagedIdentityCredential": "the instance's managed identity",
    "SharedTokenCacheCredential": "the shared token cache",
    "VisualStudioCodeCredential": "the editor's sign-in",
    "AzureCliCredential": "the az CLI's login",
    "AzurePowerShellCredential": "the PowerShell module's login",
    "AzureDeveloperCliCredential": "the developer CLI's login",
}


def _enum(value):
    return str(getattr(value, "value", value) or "")


def _sku_in(tree, sku):
    """(vCores, memory in GB) of the SKU named `sku` wherever it sits in a
    capability's tree, or None."""
    if isinstance(tree, dict):
        cores = tree.get("v_cores", tree.get("vCores"))
        per = tree.get("supported_memory_per_vcore_mb",
                       tree.get("supportedMemoryPerVcoreMb"))
        if tree.get("name") == sku and cores:
            return int(cores), (_gb(int(cores) * int(per), 1024)
                                if per else None)
        tree = list(tree.values())
    if isinstance(tree, list):
        for v in tree:
            got = _sku_in(v, sku)
            if got:
                return got
    return None


class Azure(Adapter):
    """Flexible Servers (PostgreSQL, MySQL), SQL Database, Cosmos DB and
    Azure Cache for Redis through their management clients, signed in as
    `cloudseed.Azure` signs in. A call is named by its operation group and
    method (`servers.get`)."""
    provider, WHO = "azure", "Azure"
    PRODUCTS = {
        "flexible-postgres": {"restore_point": "backup", "clone": True,
                              "parameters": True},
        "flexible-mysql": {"restore_point": "backup", "clone": True,
                           "parameters": True},
        "sql": {"restore_point": "point in time", "clone": True,
                "parameters": False},
        "cosmosdb": {"restore_point": "point in time", "clone": True,
                     "parameters": False},
        "redis": {"restore_point": "", "clone": False, "parameters": True},
    }
    WHY = {
        ("redis", "restore_point"):
            "a cache is exported to a storage account as files",
        ("redis", "clone"): "a cache is imported from exported files",
        ("sql", "parameters"):
            "a database's settings are its own scoped configuration, set in"
            " SQL",
        ("cosmosdb", "parameters"): "an account has no server parameters",
    }
    CLIENTS = {
        "flexible-postgres": ("azure.mgmt.postgresqlflexibleservers",
                              "PostgreSQLManagementClient"),
        "flexible-mysql": ("azure.mgmt.mysqlflexibleservers",
                           "MySQLManagementClient"),
        "sql": ("azure.mgmt.sql", "SqlManagementClient"),
        "cosmosdb": ("azure.mgmt.cosmosdb", "CosmosDBManagementClient"),
        "redis": ("azure.mgmt.redis", "RedisManagementClient"),
    }
    FAILED = ("failed", "canceled", "cancelled")

    def __init__(self, conf):
        super().__init__(conf)
        self._client = None
        self.pg = self.product == "flexible-postgres"

    def _credentials(self):
        if self.role:
            raise SystemExit(
                "Azure's API assumes no role: it acts as the identity signed"
                " in - name another with AZURE_CLIENT_ID or the az CLI's"
                " login")
        from azure.core.exceptions import ClientAuthenticationError
        from azure.identity import DefaultAzureCredential
        cred = DefaultAzureCredential()
        try:
            cred.get_token("https://management.azure.com/.default")
        except ClientAuthenticationError:
            raise SystemExit(
                "no Azure credentials: looked in the environment"
                " (AZURE_CLIENT_ID with a secret or a certificate), a"
                " workload identity, the instance's managed identity, the"
                " shared token cache, the az CLI's login, the PowerShell"
                " module's login and the developer CLI's login") from None
        used = type(getattr(cred, "_successful_credential", None)).__name__
        return cred, AZURE_WHERE.get(used, "the default chain")

    def _subscription(self):
        """The hop's subscription, else AZURE_SUBSCRIPTION_ID, else the az
        CLI's default one."""
        got = self.conf.get("subscription") or os.environ.get(
            "AZURE_SUBSCRIPTION_ID")
        if got:
            return got
        home = Path(os.environ.get("AZURE_CONFIG_DIR")
                    or Path.home() / ".azure")
        try:
            profile = json.loads((home / "azureProfile.json").read_text(
                encoding="utf-8-sig"))
        except (OSError, ValueError):
            profile = {}
        for s in profile.get("subscriptions") or ():
            if s.get("isDefault"):
                return s["id"]
        raise SystemExit("no Azure subscription: name it (subscription), set"
                         " AZURE_SUBSCRIPTION_ID or the az CLI's default")

    def _rg(self):
        if not self.conf.get("resource_group"):
            raise SystemExit("the Azure resource group is not named"
                             " (resource_group)")
        return self.conf["resource_group"]

    def client(self):
        if self._client is None:
            import importlib
            module, name = self.CLIENTS[self.product]
            self._client = getattr(importlib.import_module(module), name)(
                self.credentials()[0], self._subscription())
        return self._client

    def _op(self, path, *args, **kw):
        group, method = path.split(".")
        return getattr(getattr(self.client(), group), method)(*args, **kw)

    def _models(self):
        import importlib
        return importlib.import_module(self.CLIENTS[self.product][0]
                                       + ".models")

    def _poller(self, poller, what, log):
        """A long-running operation waited for until done: its result, or
        True for one that has none."""
        def ask():
            if not poller.done():
                return None
            status = _enum(poller.status()).lower()
            if status in self.FAILED:
                raise SystemExit(f"{what} is {status}")
            got = poller.result()
            return True if got is None else got
        return until(ask, what, log)

    # what the product is ---------------------------------------------------

    def _sku_size(self, location, sku):
        """(vCores, memory in GB) of a Flexible Server's SKU, as the
        region's capabilities list it."""
        try:
            caps = (self._op("capabilities_by_location.list", location)
                    if self.pg else
                    self._op("location_based_capabilities.list", location))
            for cap in caps:
                got = _sku_in(cap.as_dict(), sku)
                if got:
                    return got
        except Exception:  # noqa: BLE001 - the sizes are left unknown
            pass
        return None, None

    def _config(self, name):
        return self._op("configurations.get", self._rg(), self.id, name)

    def _server(self):
        return self._op("servers.get", self._rg(), self.id)

    def _sql_server(self):
        if not self.conf.get("server"):
            raise SystemExit("the SQL Database's server is not named"
                             " (server)")
        return self._op("servers.get", self._rg(), self.conf["server"])

    def instance(self, names=()):
        got = Instance(self.provider, self.product, self.id)
        p = self.product
        if p.startswith("flexible-"):
            s = self._server()
            got.engine = "postgres" if self.pg else "mysql"
            got.version, got.klass = _enum(s.version), s.sku.name
            got.cpu, got.memory_gb = self._sku_size(s.location, s.sku.name)
            got.storage_gb = _num(getattr(s.storage, "storage_size_gb",
                                          None))
            mw = s.maintenance_window
            got.maintenance = (
                f"{DAYS[(int(mw.day_of_week) - 1) % 7]}"
                f" {int(mw.start_hour or 0):02d}:"
                f"{int(mw.start_minute or 0):02d} UTC"
                if mw is not None and _enum(mw.custom_window).lower()
                == "enabled" else "the provider's own window")
            got.host = s.fully_qualified_domain_name or ""
            got.port = 5432 if self.pg else (getattr(s, "database_port",
                                                     None) or 3306)
            got.status, got.group, got.group_shared = _enum(s.state), "", \
                False
            got.time_zone = str(self._config(
                "timezone" if self.pg else "time_zone").value or "")
            if not self.pg:
                got.charset = str(self._config(
                    "character_set_server").value or "")
            for n in names:
                c = self._config(n)
                read_only, dynamic = _flag(c.is_read_only), _flag(
                    c.is_dynamic_config)
                got.parameters[n] = Param(
                    c.value, None if read_only is None else not read_only,
                    None if dynamic is None else not dynamic)
        elif p == "sql":
            srv, db = self._sql_server(), self._op(
                "databases.get", self._rg(), self.conf["server"], self.id)
            got.engine, got.version = "sqlserver", str(srv.version or "")
            sku = db.sku
            got.klass = f"{sku.tier} {sku.name}".strip() if sku else ""
            got.cpu = _num(sku.capacity) if sku and str(sku.tier) in (
                "GeneralPurpose", "BusinessCritical", "Hyperscale") else None
            got.storage_gb = _gb(db.max_size_bytes, 1 << 30)
            got.charset, got.status = db.collation or "", _enum(db.status)
            got.maintenance = (
                "the maintenance configuration "
                + str(db.maintenance_configuration_id).rsplit("/", 1)[-1]
                if db.maintenance_configuration_id else "")
            got.host, got.port = srv.fully_qualified_domain_name or "", 1433
            got.group, got.group_shared = "", False
        elif p == "cosmosdb":
            a = self._op("database_accounts.get", self._rg(), self.id)
            got.engine = _enum(a.kind).lower()
            got.version = str(getattr(a.api_properties, "server_version",
                                      "") or "") if a.api_properties else ""
            got.klass = _enum(a.database_account_offer_type)
            got.host, got.port = _host(a.document_endpoint), 443
            got.status, got.group, got.group_shared = str(
                a.provisioning_state or ""), "", False
        else:
            r = self._op("redis.get", self._rg(), self.id)
            got.engine, got.version = "redis", str(r.redis_version or "")
            got.klass = f"{_enum(r.sku.name)} {_enum(r.sku.family)}" \
                        f"{r.sku.capacity}"
            got.host, got.port = r.host_name or "", r.ssl_port or r.port
            got.status, got.group, got.group_shared = _enum(
                r.provisioning_state), "", False
            conf = self._redis_conf(r)
            got.parameters = {n: Param(conf.get(n)) for n in names}
        return got

    @staticmethod
    def _redis_conf(r):
        """A cache's settings by the names Redis gives them
        (`maxmemory-policy`)."""
        c = r.redis_configuration
        if c is None:
            return {}
        got = c.serialize(keep_readonly=True) if hasattr(c, "serialize") \
            else dict(c)
        return {k: v for k, v in got.items() if not isinstance(v, dict)}

    def find(self, host, port=None):
        host, p, rg = str(host), self.product, self._rg()
        if p.startswith("flexible-") or p == "sql":
            for s in self._op("servers.list_by_resource_group", rg):
                if s.fully_qualified_domain_name == host:
                    return s.name
        elif p == "cosmosdb":
            for a in self._op("database_accounts.list_by_resource_group",
                              rg):
                if host in (_host(a.document_endpoint),
                            f"{a.name}.mongo.cosmos.azure.com"):
                    return a.name
        else:
            for r in self._op("redis.list_by_resource_group", rg):
                if r.host_name == host:
                    return r.name
        return None

    # restore points --------------------------------------------------------

    def _continuous(self):
        """The account, and whether it keeps continuous backup - a point in
        time is restorable only then."""
        a = self._op("database_accounts.get", self._rg(), self.id)
        return a, _enum(getattr(a.backup_policy, "type", "")).lower() == \
            "continuous"

    def restore_point(self, name, log):
        if self.product == "cosmosdb" and not self._continuous()[1]:
            raise NotOffered(
                "Azure's API offers no restore point made on request for"
                f" {self.id}: the account keeps periodic backups, which the"
                " provider restores only on a support request; continuous"
                " backup gives it a point in time")
        return super().restore_point(name, log)

    def _make(self, name, log):
        what = f"the backup {name}"
        if self.pg:
            poller = self._op("backups_automatic_and_on_demand.begin_create",
                              self._rg(), self.id, name)
        else:
            poller = self._op("long_running_backup.begin_create", self._rg(),
                              self.id, name, self._models().ServerBackupV2(
                                  backup_name_v2=name))
        log(f"{what} of {self.id} asked for")
        got = self._poller(poller, what, log)
        return RestorePoint(id=name, kind="backup", status="succeeded",
                            at=_stamp(getattr(got, "completed_time", None)
                                      or _now()), ready=True)

    def restore_points(self):
        p, rg = self.product, self._rg()
        if p.startswith("flexible-"):
            got = self._op("backups_automatic_and_on_demand.list_by_server"
                           if self.pg else "backups.list_by_server", rg,
                           self.id)
            return [RestorePoint(id=b.name, kind="backup",
                                 status=_enum(b.backup_type),
                                 at=_stamp(b.completed_time), ready=True)
                    for b in got]
        if p == "sql":
            out = []
            for r in self._op("restore_points.list_by_database", rg,
                              self.conf.get("server"), self.id):
                when = _stamp(r.restore_point_creation_date
                              or r.earliest_restore_date)
                kind = _enum(r.restore_point_type)
                out.append(RestorePoint(
                    id=when, kind="point in time" if kind == "CONTINUOUS"
                    else "backup", status=kind, at=when, ready=True))
            return out
        if p == "cosmosdb":
            a, continuous = self._continuous()
            for r in (self._op("restorable_database_accounts"
                               ".list_by_location", a.location)
                      if continuous else ()):
                if r.account_name == self.id:
                    first = _stamp(r.oldest_restorable_time)
                    return [RestorePoint(id=first, kind="point in time",
                                         at=first, ready=True)]
            return []
        self._offered("restore_point")
        return []

    # copies ----------------------------------------------------------------

    def _when(self, point, log):
        """The time a copy is restored to: the latest is now; a backup's,
        the time it completed - these products restore to a time."""
        if point is None:
            return _now()
        if point.kind == "backup":
            log(f"the copy is restored to {point.at}, when the backup"
                f" {point.id} completed")
        return _time(point.at)

    def _clone(self, name, point, log):
        what, p, rg, m = f"the copy {name}", self.product, self._rg(), \
            self._models()
        when = self._when(point, log)
        if p.startswith("flexible-"):
            src = self._server()
            fields = {"location": src.location,
                      "create_mode": "PointInTimeRestore",
                      "source_server_resource_id": src.id,
                      "point_in_time_utc" if self.pg
                      else "restore_point_in_time": when}
            poller = self._op("servers.begin_create_or_update" if self.pg
                              else "servers.begin_create", rg, name,
                              m.Server(**fields))
            log(f"{what} of {self.id} asked for")
            got = self._poller(poller, what, log)
            return {"host": got.fully_qualified_domain_name,
                    "port": 5432 if self.pg else 3306, "id": name,
                    "kind": "server"}
        if p == "sql":
            srv = self._sql_server()
            db = self._op("databases.get", rg, self.conf["server"], self.id)
            poller = self._op(
                "databases.begin_create_or_update", rg, self.conf["server"],
                name, m.Database(location=db.location,
                                 properties=m.DatabaseProperties(
                                     create_mode="PointInTimeRestore",
                                     source_database_id=db.id,
                                     restore_point_in_time=when)))
            log(f"{what} of {self.id} asked for")
            self._poller(poller, what, log)
            return {"host": srv.fully_qualified_domain_name, "port": 1433,
                    "id": name, "kind": "database"}
        a, continuous = self._continuous()
        if not continuous:
            raise NotOffered(f"Azure's API copies {self.id} only from"
                             " continuous backup, and it keeps periodic"
                             " backups")
        source = next((r for r in self._op(
            "restorable_database_accounts.list_by_location", a.location)
            if r.account_name == self.id), None)
        if source is None:
            raise SystemExit(f"{self.id} is not restorable in {a.location}")
        poller = self._op(
            "database_accounts.begin_create_or_update", rg, name,
            m.DatabaseAccountCreateUpdateParameters(
                location=a.location, kind=a.kind,
                properties=m.DatabaseAccountCreateUpdateProperties(
                    locations=[m.Location(location_name=a.location,
                                          failover_priority=0)],
                    database_account_offer_type="Standard",
                    create_mode="Restore",
                    backup_policy=m.ContinuousModeBackupPolicy(),
                    restore_parameters=m.RestoreParameters(
                        restore_mode="PointInTime",
                        restore_source=source.id,
                        restore_timestamp_in_utc=when))))
        log(f"{what} of {self.id} asked for")
        got = self._poller(poller, what, log)
        return {"host": _host(getattr(got, "document_endpoint", "")),
                "port": 443, "id": name, "kind": "account"}

    # parameters ------------------------------------------------------------

    def _set(self, values, log):
        what, rg, m = f"the parameters of {self.id}", self._rg(), \
            self._models()
        if self.product == "redis":
            conf = self._redis_conf(self._op("redis.get", rg, self.id))
            before = {n: conf.get(n) for n in values}
            settings = m.RedisCommonPropertiesRedisConfiguration.from_dict(
                {n: str(v) for n, v in values.items()})
            poller = self._op("redis.begin_update", rg, self.id,
                              m.RedisUpdateParameters(
                                  redis_configuration=settings))
            log(f"{', '.join(values)} asked to change on {self.id}")
            self._poller(poller, what, log)
            return before
        before, restart = {}, []
        for n in values:
            c = self._config(n)
            if _flag(c.is_read_only):
                raise SystemExit(f"{n} cannot be changed on {self.id}")
            before[n] = c.value
            if _flag(c.is_dynamic_config) is False:
                restart.append(n)
        for n, v in values.items():
            body = (m.ConfigurationForUpdate if self.pg else m.Configuration)(
                value=str(v), source="user-override")
            self._poller(self._op("configurations.begin_update", rg, self.id,
                                  n, body), f"the parameter {n}", log)
        log(f"{', '.join(values)} changed on {self.id}")
        until(lambda: None if any(
            str(self._config(n).value) != str(v) for n, v in values.items())
            else True, what, log)
        if restart:
            log(f"{', '.join(restart)} take effect at {self.id}'s next"
                " restart")
        return before


# --- Tencent Cloud -----------------------------------------------------------

#: product -> (the SDK's package, the API version, its client)
TENCENT = {"cdb": ("cdb", "v20170320", "CdbClient"),
           "postgres": ("postgres", "v20170312", "PostgresClient"),
           "mongodb": ("mongodb", "v20190725", "MongodbClient"),
           "redis": ("redis", "v20180412", "RedisClient"),
           "cynosdb": ("cynosdb", "v20190107", "CynosdbClient"),
           "sqlserver": ("sqlserver", "v20180328", "SqlserverClient"),
           "mariadb": ("mariadb", "v20170312", "MariadbClient")}

#: the zone Tencent Cloud's API writes and reads its times in (its
#: documentation: UTC+8)
BEIJING = datetime.timezone(datetime.timedelta(hours=8))


def _beijing(when):
    return _time(when).astimezone(BEIJING).strftime("%Y-%m-%d %H:%M:%S")


def _tencent_assumed(over, role, session="migkit"):
    """`role` assumed with the credentials found - a temporary key and its
    token as well as a key pair, which the SDK's own assumed credential
    does not sign with - and assumed again before it expires."""
    from tencentcloud.common.common_client import CommonClient
    from tencentcloud.common.credential import STSAssumeRoleCredential
    from tencentcloud.common.profile.client_profile import ClientProfile
    from tencentcloud.common.profile.http_profile import HttpProfile

    class Assumed(STSAssumeRoleCredential):
        def get_sts_tmp_role_arn(self):
            got = CommonClient(
                credential=over, region=self._region, version=self._version,
                service=self._service, profile=ClientProfile(
                    httpProfile=HttpProfile(endpoint=self._endpoint))
            ).call_json("AssumeRole", {
                "RoleArn": self._role_arn,
                "RoleSessionName": self._role_session_name,
                "DurationSeconds": self._duration_seconds})["Response"]
            self._token = got["Credentials"]["Token"]
            self._tmp_secret_id = got["Credentials"]["TmpSecretId"]
            self._tmp_secret_key = got["Credentials"]["TmpSecretKey"]
            self._expired_time = got["ExpiredTime"] - \
                self._duration_seconds * 0.9
    return Assumed(None, None, role, session)


def _week(days):
    """A maintenance window in words from {day: ["03:00-04:00", ...]}."""
    spans = {d: ", ".join(v) for d, v in days.items() if v}
    if not spans:
        return ""
    if len(spans) == 7 and len(set(spans.values())) == 1:
        return f"every day {next(iter(spans.values()))} Beijing time"
    return "; ".join(f"{d} {s}" for d, s in spans.items()) + " Beijing time"


class Tencent(Adapter):
    """TencentDB for MySQL (cdb), PostgreSQL, MongoDB, Redis, TDSQL-C
    (cynosdb), SQL Server and MariaDB through Tencent Cloud's API. A call
    is named by its action (`DescribeDBInstances`) and its fields are the
    request model's own - a name the model does not have is refused
    before anything is sent."""
    provider, WHO = "tencent", "Tencent Cloud"
    PRODUCTS = {
        "cdb": {"restore_point": "backup", "clone": True,
                "parameters": True},
        "postgres": {"restore_point": "backup", "clone": True,
                     "parameters": True},
        "mongodb": {"restore_point": "backup", "clone": True,
                    "parameters": True},
        "redis": {"restore_point": "backup", "clone": True,
                  "parameters": True},
        "cynosdb": {"restore_point": "backup", "clone": True,
                    "parameters": True},
        "sqlserver": {"restore_point": "backup", "clone": False,
                      "parameters": True},
        "mariadb": {"restore_point": "", "clone": False, "parameters": True},
    }
    WHY = {
        ("sqlserver", "clone"):
            "a backup is restored into an instance that exists, or as a"
            " renamed database inside the same instance",
        ("mariadb", "restore_point"):
            "its backups are taken on the instance's own schedule, which"
            " the API sets",
        ("mariadb", "clone"): "a backup is restored through the console",
    }
    #: where the CLI keeps its profiles, and the SDK its own file
    CLI_DIR, SDK_FILE = ".tccli", Path(".tencentcloud") / "credentials"

    def __init__(self, conf):
        super().__init__(conf)
        self._client = None

    # credentials ------------------------------------------------------------

    def _env(self):
        from tencentcloud.common.credential import Credential
        sid = os.environ.get("TENCENTCLOUD_SECRET_ID")
        key = os.environ.get("TENCENTCLOUD_SECRET_KEY")
        if not (sid and key):
            return None
        cred = Credential(sid, key, os.environ.get("TENCENTCLOUD_TOKEN")
                          or os.environ.get("TENCENTCLOUD_SESSION_TOKEN")
                          or None)
        where = "the environment (TENCENTCLOUD_SECRET_ID)"
        arn = os.environ.get("TENCENTCLOUD_ROLE_ARN")
        session = os.environ.get("TENCENTCLOUD_ROLE_SESSION_NAME")
        if arn and session:
            return (_tencent_assumed(cred, arn, session),
                    f"the role {arn} the environment names, assumed over"
                    " the environment's key")
        return cred, where

    def _cli(self, profile):
        """The CLI's profile: ~/.tccli/<profile>.credential, a key pair (and
        a token, or a role to assume) or the instance's role."""
        from tencentcloud.common.credential import CVMRoleCredential, \
            Credential
        path = Path.home() / self.CLI_DIR / f"{profile}.credential"
        try:
            got = json.loads(path.read_text())
        except (OSError, ValueError):
            return None
        where = f"the CLI's profile {profile}"
        if got.get("type") == "cvm-role":
            role = CVMRoleCredential()
            return (role, f"the instance's role, as {where} says") \
                if role.get_credential() is not None else None
        if not (got.get("secretId") and got.get("secretKey")):
            return None
        cred = Credential(got["secretId"], got["secretKey"],
                          got.get("token") or None)
        if got.get("role-arn") and got.get("role-session-name"):
            return (_tencent_assumed(cred, got["role-arn"],
                                     got["role-session-name"]),
                    f"the role {got['role-arn']} {where} names, assumed over"
                    " its key")
        return cred, where

    def _cli_region(self, profile):
        path = Path.home() / self.CLI_DIR / f"{profile}.configure"
        try:
            return (json.loads(path.read_text()).get("_sys_param") or {}).get(
                "region", "")
        except (OSError, ValueError):
            return ""

    def _sdk_file(self):
        """The SDK's own file: ~/.tencentcloud/credentials, else the
        machine's /etc/tencentcloud/credentials, its [default] section."""
        import configparser

        from tencentcloud.common.credential import Credential
        for path in (Path.home() / self.SDK_FILE,
                     Path("/etc") / "tencentcloud" / "credentials"):
            ini = configparser.ConfigParser()
            if not ini.read(path) or not ini.has_section("default"):
                continue
            sid = ini.get("default", "secret_id", fallback="").strip()
            key = ini.get("default", "secret_key", fallback="").strip()
            if sid and key:
                return Credential(sid, key), (
                    "the SDK's own credentials file"
                    + ("" if path.is_relative_to(Path.home())
                       else " of the machine"))
        return None

    def _credentials(self):
        from tencentcloud.common.credential import CVMRoleCredential
        named = self.conf.get("profile")
        profile = named or os.environ.get("TCCLI_PROFILE") or "default"
        found = None if named else self._env()
        if found is None:
            found = self._cli(profile)
        if found is None and named:
            raise SystemExit(f"the CLI's profile {named} holds no Tencent"
                             " Cloud credentials")
        if found is None:
            found = self._sdk_file()
        if found is None:
            role = CVMRoleCredential()
            if role.get_credential() is not None:
                found = role, "the instance's role"
        if found is None:
            raise SystemExit(
                "no Tencent Cloud credentials: looked in the environment"
                " (TENCENTCLOUD_SECRET_ID and TENCENTCLOUD_SECRET_KEY), the"
                f" CLI's profile {profile}, the SDK's own credentials file"
                " and the instance's role")
        self.region = self.region or (
            "" if named else os.environ.get("TENCENTCLOUD_REGION", "")) \
            or self._cli_region(profile)
        if not self.region:
            raise SystemExit("no Tencent Cloud region: name it (region), set"
                             " TENCENTCLOUD_REGION or the CLI profile's"
                             " region")
        cred, where = found
        if self.role:
            return (_tencent_assumed(cred, self.role),
                    f"the role {self.role}, assumed over {where}")
        return cred, where

    # the calls --------------------------------------------------------------

    def _models(self):
        import importlib
        pkg, version, _ = TENCENT[self.product]
        return importlib.import_module(f"tencentcloud.{pkg}.{version}.models")

    def client(self):
        if self._client is None:
            import importlib

            from tencentcloud.common.profile.client_profile import \
                ClientProfile
            from tencentcloud.common.profile.http_profile import HttpProfile
            pkg, version, name = TENCENT[self.product]
            module = importlib.import_module(
                f"tencentcloud.{pkg}.{version}.{pkg}_client")
            cred = self.credentials()[0]
            self._client = getattr(module, name)(
                cred, self.region, ClientProfile(httpProfile=HttpProfile(
                    reqTimeout=60)))
        return self._client

    def _call(self, action, **fields):
        """The action asked with `fields`, its answer as the API writes it."""
        from tencentcloud.common.exception.tencent_cloud_sdk_exception \
            import TencentCloudSDKException
        fields = {k: v for k, v in fields.items() if v is not None}
        req = getattr(self._models(), action + "Request")()
        unknown = [k for k in fields if not hasattr(req, "_" + k)]
        if unknown:
            raise ValueError(f"{action} has no {', '.join(unknown)}")
        req._deserialize(fields)
        try:
            got = getattr(self.client(), action)(req)
        except TencentCloudSDKException as e:
            raise SystemExit(f"Tencent Cloud's API refused {action} for"
                             f" {self.id}: {e.get_message()}"
                             f" ({e.get_code()})") from None
        return json.loads(got.to_json_string())

    def _all(self, action, key, limit=100, page=False, **fields):
        """Every item of a listing, `Offset` counting items - or pages,
        where the product counts them so (`page`)."""
        out, offset = [], 0
        while True:
            got = self._call(action, Limit=limit, Offset=offset, **fields)
            items = got.get(key) or []
            out += items
            if len(items) < limit:
                return out
            offset += 1 if page else limit

    def _wait_async(self, request_id, what, log):
        """An asynchronous request of MySQL's or MongoDB's waited for."""
        return _ready(lambda: (self._call(
            "DescribeAsyncRequestInfo", AsyncRequestId=request_id).get(
            "Status", ""), True), what, log, ("success",),
            ("failed", "killed", "removed", "paused", "undoed"))

    # what the product is ---------------------------------------------------

    def _one(self):
        """The product's own description of the instance."""
        p = self.product
        if p == "cdb":
            items = self._call("DescribeDBInstances",
                               InstanceIds=[self.id]).get("Items")
        elif p == "postgres":
            items = [self._call("DescribeDBInstanceAttribute",
                                DBInstanceId=self.id).get("DBInstance")]
        elif p == "mongodb":
            items = self._call("DescribeDBInstances", InstanceIds=[
                self.id]).get("InstanceDetails")
        elif p == "redis":
            items = self._call("DescribeInstances", InstanceId=self.id).get(
                "InstanceSet")
        elif p == "cynosdb":
            items = [self._call("DescribeClusterDetail",
                                ClusterId=self.id).get("Detail")]
        elif p == "sqlserver":
            items = self._call("DescribeDBInstances", InstanceIdSet=[
                self.id]).get("DBInstances")
        else:
            items = self._call("DescribeDBInstances", InstanceIds=[
                self.id]).get("Instances")
        if not items or not items[0]:
            raise SystemExit(f"Tencent Cloud's API lists no {self.product}"
                             f" {self.id} in {self.region}")
        return items[0]

    def _param_rows(self):
        """{name: (value, modifiable, restart)} of every parameter the
        product lists for the instance."""
        p, out = self.product, {}
        if p in ("cdb", "sqlserver"):
            for r in self._call("DescribeInstanceParams",
                                InstanceId=self.id).get("Items") or ():
                out[r["Name"]] = (r.get("CurrentValue"),
                                  not r.get("IsNotSupportEdit"),
                                  _flag(r.get("NeedReboot")))
        elif p == "postgres":
            for r in self._call("DescribeDBInstanceParameters",
                                DBInstanceId=self.id).get("Detail") or ():
                out[r["Name"]] = (r.get("CurrentValue"), None,
                                  _flag(r.get("NeedReboot")))
        elif p in ("mongodb", "redis"):
            got = self._call("DescribeInstanceParams", InstanceId=self.id)
            for key in ("InstanceEnumParam", "InstanceIntegerParam",
                        "InstanceTextParam", "InstanceMultiParam"):
                for r in got.get(key) or ():
                    # the API lists what the instance may change
                    out[r["ParamName"]] = (r.get("CurrentValue"), True,
                                           _flag(r.get("NeedRestart")))
        elif p == "cynosdb":
            for r in self._call("DescribeClusterParams",
                                ClusterId=self.id).get("Items") or ():
                out[r["ParamName"]] = (
                    r.get("CurrentValue"),
                    _flag((r.get("ModifiableInfo") or {}).get(
                        "IsModifiable")), _flag(r.get("NeedReboot")))
        else:
            for r in self._call("DescribeDBParameters",
                                InstanceId=self.id).get("Params") or ():
                out[r["Param"]] = (r.get("Value"), None,
                                   _flag(r.get("NeedRestart")))
        return out

    def _connections(self, it):
        """A MongoDB spec's connection limit, as the region's specs list
        it for the instance's CPU, memory, version and kind."""
        for info in self._call("DescribeSpecInfo", Zone=it.get("Zone")).get(
                "SpecInfoList") or ():
            for s in info.get("SpecItems") or ():
                if (s.get("Cpu"), s.get("Memory"), s.get("MongoVersionCode"),
                        s.get("ClusterType")) == (
                        it.get("CpuNum"), it.get("Memory"),
                        it.get("MongoVersion"), it.get("ClusterType")):
                    return s.get("Conns")
        return None

    def instance(self, names=()):
        it, p = self._one(), self.product
        got = Instance(self.provider, p, self.id, group="",
                       group_shared=False)
        if p == "cdb":
            got.engine, got.version = "mysql", it.get("EngineVersion", "")
            got.klass, got.cpu = it.get("DeviceType", ""), it.get("Cpu")
            got.memory_gb, got.storage_gb = _gb(it.get("Memory"), 1024), \
                _num(it.get("Volume"))
            got.host, got.port = it.get("Vip", ""), it.get("Vport")
            got.status = {0: "creating", 1: "running", 4: "isolating",
                          5: "isolated"}.get(it.get("Status"),
                                             str(it.get("Status")))
            got.charset = self._call("DescribeDBInstanceCharset",
                                     InstanceId=self.id).get("Charset", "")
            w = self._call("DescribeTimeWindow", InstanceId=self.id)
            got.maintenance = _week({d: w.get(d) for d in DAYS})
        elif p == "postgres":
            got.engine = "postgres"
            got.version = it.get("DBVersion") or it.get("DBMajorVersion", "")
            got.klass, got.cpu = it.get("DBInstanceClass", ""), it.get(
                "DBInstanceCpu")
            got.memory_gb, got.storage_gb = _num(it.get(
                "DBInstanceMemory")), _num(it.get("DBInstanceStorage"))
            got.charset, got.status = it.get("DBCharset", ""), it.get(
                "DBInstanceStatus", "")
            net = next((n for n in it.get("DBInstanceNetInfo") or ()
                        if n.get("NetType") in ("private", "inner")), {})
            got.host, got.port = net.get("Ip") or net.get(
                "Address", ""), net.get("Port")
            w = self._call("DescribeMaintainTimeWindow", DBInstanceId=self.id)
            days = ", ".join(d.title() for d in w.get("MaintainWeekDays")
                             or ())
            got.maintenance = (
                f"{days} from {w.get('MaintainStartTime')} for"
                f" {w.get('MaintainDuration')} h Beijing time" if days
                else "")
        elif p == "mongodb":
            got.engine = "mongodb"
            got.version = re.sub(r"^MONGO_(\d)(\d)_.*$", r"\1.\2",
                                 str(it.get("MongoVersion", "")))
            got.klass, got.cpu = it.get("MachineType", ""), it.get("CpuNum")
            got.memory_gb, got.storage_gb = _gb(it.get("Memory"), 1024), \
                _gb(it.get("Volume"), 1024)
            got.host, got.port = it.get("Vip", ""), it.get("Vport")
            got.status = {0: "initializing", 1: "busy", 2: "running",
                          -2: "isolated", -3: "isolated"}.get(
                it.get("Status"), str(it.get("Status")))
            got.maintenance = (f"every day {it.get('MaintenanceStart')}-"
                               f"{it.get('MaintenanceEnd')} Beijing time"
                               if it.get("MaintenanceStart") else "")
            got.max_connections = self._connections(it)
        elif p == "redis":
            got.engine, got.version = "redis", it.get(
                "CurrentRedisVersion", "")
            got.klass = it.get("ProductType", "")
            got.memory_gb = _gb(it.get("Size"), 1024)
            got.host, got.port = it.get("WanIp", ""), it.get("Port")
            got.status = {0: "initializing", 1: "busy", 2: "running",
                          -2: "isolated", -3: "deleting"}.get(
                it.get("Status"), str(it.get("Status")))
            w = self._call("DescribeMaintenanceWindow", InstanceId=self.id)
            got.maintenance = (f"every day {w.get('StartTime')}-"
                               f"{w.get('EndTime')} Beijing time"
                               if w.get("StartTime") else "")
        elif p == "cynosdb":
            rw = next((i for i in it.get("InstanceSet") or ()
                       if str(i.get("InstanceRole")).lower() in (
                           "master", "rw")), (it.get("InstanceSet") or [{}])[
                0])
            got.engine = str(it.get("DbType", "mysql")).lower()
            got.version, got.charset = it.get("DbVersion", ""), it.get(
                "Charset", "")
            got.klass, got.cpu = rw.get("InstanceDeviceType", ""), rw.get(
                "InstanceCpu")
            got.memory_gb, got.storage_gb = _num(rw.get(
                "InstanceMemory")), _num(it.get("Storage"))
            got.host, got.port = it.get("Vip", ""), it.get("Vport")
            got.status = it.get("Status", "")
            if rw.get("MaintainWeekDays"):
                start = int(rw.get("MaintainStartTime") or 0)
                got.maintenance = (
                    f"{', '.join(rw['MaintainWeekDays'])} from"
                    f" {start // 3600:02d}:{start % 3600 // 60:02d} for"
                    f" {int(rw.get('MaintainDuration') or 0) // 60} min"
                    " Beijing time")
        elif p == "sqlserver":
            got.engine, got.version = "sqlserver", it.get(
                "VersionName") or it.get("Version", "")
            got.klass, got.cpu = it.get("Type", ""), it.get("Cpu")
            got.memory_gb, got.storage_gb = _num(it.get("Memory")), _num(
                it.get("Storage"))
            got.charset, got.time_zone = it.get("Collation", ""), it.get(
                "TimeZone", "")
            got.host, got.port = it.get("Vip", ""), it.get("Vport")
            got.status = {1: "applying", 2: "running", 4: "isolated",
                          7: "busy", 12: "restarting"}.get(
                it.get("Status"), str(it.get("Status")))
        else:
            got.engine, got.version = "mariadb", it.get("DbVersion", "")
            got.klass, got.cpu = it.get("Machine", ""), it.get("Cpu")
            got.memory_gb, got.storage_gb = _num(it.get("Memory")), _num(
                it.get("Storage"))
            got.host, got.port = it.get("Vip", ""), it.get("Vport")
            got.status = it.get("StatusDesc") or str(it.get("Status"))
        if names:
            rows = self._param_rows()
            got.parameters = {n: Param(*rows[n]) if n in rows else Param()
                              for n in names}
        return got

    def find(self, host, port=None):
        host, p = str(host), self.product

        def at(address, their_port):
            return address == host and (port is None or their_port is None
                                        or int(their_port) == int(port))
        if p == "cdb":
            for it in self._call("DescribeDBInstances", Vips=[host]).get(
                    "Items") or ():
                if at(it.get("Vip"), it.get("Vport")):
                    return it["InstanceId"]
        elif p == "postgres":
            for it in self._call("DescribeDBInstances", Filters=[
                    {"Name": "db-private-ip", "Values": [host]}]).get(
                    "DBInstanceSet") or ():
                if any(at(n.get("Ip") or n.get("Address"), n.get("Port"))
                       for n in it.get("DBInstanceNetInfo") or ()):
                    return it["DBInstanceId"]
        elif p == "sqlserver":
            for it in self._call("DescribeDBInstances", VipSet=[host]).get(
                    "DBInstances") or ():
                if at(it.get("Vip"), it.get("Vport")):
                    return it["InstanceId"]
        elif p == "redis":
            for it in self._all("DescribeInstances", "InstanceSet"):
                if at(it.get("WanIp"), it.get("Port")):
                    return it["InstanceId"]
        elif p == "cynosdb":
            for it in self._all("DescribeClusters", "ClusterSet"):
                if at(it.get("Vip"), it.get("Vport")):
                    return it["ClusterId"]
        else:
            key = "InstanceDetails" if p == "mongodb" else "Instances"
            for it in self._all("DescribeDBInstances", key):
                if at(it.get("Vip"), it.get("Vport")):
                    return it["InstanceId"]
        return None

    # restore points --------------------------------------------------------

    def _points(self):
        """[RestorePoint] the product lists for the instance, newest
        last."""
        p, out = self.product, []
        if p == "cdb":
            for b in self._all("DescribeBackups", "Items",
                               InstanceId=self.id):
                out.append(RestorePoint(
                    str(b.get("BackupId")), "backup", b.get("Status", ""),
                    _stamp(_time(b["Date"], BEIJING)) if b.get("Date")
                    else "", b.get("Status") == "SUCCESS"))
        elif p == "postgres":
            for b in self._all("DescribeBaseBackups", "BaseBackupSet",
                               Filters=[{"Name": "db-instance-id",
                                         "Values": [self.id]}]):
                out.append(RestorePoint(
                    b.get("Id", ""), "backup", b.get("State", ""),
                    _stamp(_time(b["FinishTime"], BEIJING))
                    if b.get("FinishTime") else "",
                    b.get("State") == "finished"))
        elif p == "mongodb":
            for b in self._all("DescribeDBBackups", "BackupList",
                               InstanceId=self.id):
                out.append(RestorePoint(
                    b.get("BackupName", ""), "backup", str(b.get("Status")),
                    _stamp(_time(b["EndTime"], BEIJING))
                    if b.get("EndTime") else "", b.get("Status") == 2))
        elif p == "redis":
            for b in self._all("DescribeInstanceBackups", "BackupSet",
                               InstanceId=self.id, Status=[1, 2, 3, 4]):
                out.append(RestorePoint(
                    b.get("BackupId", ""), "backup", str(b.get("Status")),
                    _stamp(_time(b["StartTime"], BEIJING))
                    if b.get("StartTime") else "",
                    b.get("Status") in (1, 2, 3, 4)))
        elif p == "cynosdb":
            for b in self._all("DescribeBackupList", "BackupList",
                               ClusterId=self.id):
                out.append(RestorePoint(
                    str(b.get("BackupId")), "backup",
                    b.get("BackupStatus", ""),
                    _stamp(_time(b["SnapshotTime"], BEIJING))
                    if b.get("SnapshotTime") else "",
                    b.get("BackupStatus") == "success"))
        elif p == "sqlserver":
            it = self._one()
            for b in self._all("DescribeBackups", "Backups", page=True,
                               InstanceId=self.id,
                               StartTime=it.get("CreateTime"),
                               EndTime=_beijing(_now())):
                out.append(RestorePoint(
                    str(b.get("Id")), "backup", str(b.get("Status")),
                    _stamp(_time(b["EndTime"], BEIJING))
                    if b.get("EndTime") else "", b.get("Status") == 1))
        else:
            for b in self._all("DescribeBackupFiles", "Files",
                               InstanceId=self.id, BackupType="Data"):
                out.append(RestorePoint(
                    b.get("FileName", ""), "backup", "",
                    _stamp(_time(b["EndTime"], BEIJING))
                    if b.get("EndTime") else "", True))
        return sorted(out, key=lambda r: r.at)

    def restore_points(self):
        return self._points()

    def _make(self, name, log):
        """The backup asked for, its own task waited for where the product
        gives one, and the backup then waited for in the product's list
        until it is ready."""
        p, what = self.product, f"the backup {name}"
        if p == "cdb":
            it = self._one()
            method = ("snapshot" if str(it.get("DeviceType", "")).startswith(
                "BASIC") or str(it.get("DiskType", "")).startswith("CLOUD")
                else "physical")
            made = str(self._call("CreateBackup", InstanceId=self.id,
                                  BackupMethod=method,
                                  ManualBackupName=name[:60])["BackupId"])
            log(f"{what} of {self.id} asked for")
        elif p == "postgres":
            made = self._call("CreateBaseBackup",
                              DBInstanceId=self.id)["BaseBackupId"]
            log(f"{what} of {self.id} asked for")
        elif p == "mongodb":
            request = self._call("CreateBackupDBInstance", InstanceId=self.id,
                                 BackupMethod=1, BackupRemark=name)[
                "AsyncRequestId"]
            log(f"{what} of {self.id} asked for")
            self._wait_async(request, what, log)
            made = next((b["BackupName"] for b in self._all(
                "DescribeDBBackups", "BackupList", InstanceId=self.id)
                if b.get("BackupDesc") == name), "")
        elif p == "redis":
            task = self._call("ManualBackupInstance", InstanceId=self.id,
                              Remark=name)["TaskId"]
            log(f"{what} of {self.id} asked for")
            _ready(lambda: (self._call("DescribeTaskInfo", TaskId=task).get(
                "Status", ""), True), what, log, ("succeed",),
                ("failed", "error"))
            made = next((b["BackupId"] for b in self._all(
                "DescribeInstanceBackups", "BackupSet", InstanceId=self.id,
                Status=[1, 2, 3, 4]) if b.get("Remark") == name), "")
        elif p == "cynosdb":
            flow = self._call("CreateBackup", ClusterId=self.id,
                              BackupType="snapshot",
                              BackupName=name)["FlowId"]
            log(f"{what} of {self.id} asked for")
            _ready(lambda: (self._call("DescribeFlow", FlowId=flow).get(
                "Status"), True), what, log, ("0",), ("1",))
            made = next((str(b["BackupId"]) for b in self._all(
                "DescribeBackupList", "BackupList", ClusterId=self.id,
                BackupMethod="manual") if b.get("BackupName") == name), "")
        else:
            flow = self._call("CreateBackup", Strategy=0, InstanceId=self.id,
                              BackupName=name)["FlowId"]
            log(f"{what} of {self.id} asked for")
            made = str(_ready(lambda: (lambda b: (b.get("Status"), b))(
                self._call("DescribeBackupByFlowId", InstanceId=self.id,
                           FlowId=str(flow))), what, log, ("1",),
                ("2",))["Id"])
        if not made:
            raise SystemExit(f"{what} was made, and the provider does not"
                             " list it")

        def ask():
            got = next((r for r in self._points() if r.id == made), None)
            if got is None:
                return "", None
            return ("ready" if got.ready else got.status or ""), got
        return _ready(ask, what, log, ("ready",),
                      ("failed", "canceled", "fail"))

    # copies ----------------------------------------------------------------

    def _net(self, it, vpc="UniqVpcId", subnet="UniqSubnetId"):
        return (self.conf.get("vpc") or it.get(vpc),
                self.conf.get("subnet") or it.get(subnet))

    def _clone(self, name, point, log):
        p, it, what = self.product, self._one(), f"the copy {name}"
        backup = point is not None and point.kind == "backup"
        when = _beijing(point.at if point is not None and point.at
                        else _now())
        if p == "cdb":
            vpc, subnet = self._net(it)
            self._call("CreateCloneInstance", InstanceId=self.id,
                       SpecifiedBackupId=int(point.id) if backup else None,
                       SpecifiedRollbackTime=None if backup else when,
                       InstanceName=name, UniqVpcId=vpc, UniqSubnetId=subnet,
                       Memory=it.get("Memory"), Volume=it.get("Volume"),
                       Cpu=it.get("Cpu"), DeviceType=it.get("DeviceType"))
            log(f"{what} of {self.id} asked for")

            def job():
                mine = {x["InstanceId"] for x in self._call(
                    "DescribeDBInstances", InstanceNames=[name]).get(
                    "Items") or ()}
                for j in self._call("DescribeCloneList",
                                    InstanceId=self.id).get("Items") or ():
                    if j.get("DstInstanceId") in mine:
                        return j.get("TaskStatus", ""), j
                return "initial", None
            made = _ready(job, what, log, ("success",), ("failed",))[
                "DstInstanceId"]
            new = _ready(lambda: (lambda i: (i.get("Status"), i))(
                self._call("DescribeDBInstances", InstanceIds=[made])[
                    "Items"][0]), what, log, ("1",), ("5",))
            return {"host": new.get("Vip"), "port": new.get("Vport"),
                    "id": made, "kind": "instance"}
        if p == "postgres":
            vpc, subnet = self._net(it, "VpcId", "SubnetId")
            made = self._call(
                "CloneDBInstance", DBInstanceId=self.id,
                SpecCode=self.conf.get("spec") or it.get("DBInstanceClass"),
                Storage=int(self.conf.get("storage")
                            or it.get("DBInstanceStorage")),
                Period=1, AutoRenewFlag=0, VpcId=vpc, SubnetId=subnet,
                Name=name, InstanceChargeType="POSTPAID_BY_HOUR",
                DBNodeSet=[{"Role": "Primary", "Zone": it.get("Zone")},
                           {"Role": "Standby", "Zone": it.get("Zone")}],
                BackupSetId=point.id if backup else None,
                RecoveryTargetTime=None if backup else when)["DBInstanceId"]
            log(f"{what} of {self.id} asked for")
            new = _ready(lambda: (lambda i: (i.get("DBInstanceStatus"), i))(
                self._call("DescribeDBInstanceAttribute",
                           DBInstanceId=made)["DBInstance"]), what, log,
                ("running",), ("isolated", "recycled", "offline"))
            net = next((n for n in new.get("DBInstanceNetInfo") or ()
                        if n.get("NetType") in ("private", "inner")), {})
            return {"host": net.get("Ip") or net.get("Address"),
                    "port": net.get("Port"), "id": made, "kind": "instance"}
        if p == "mongodb":
            vpc, subnet = self._net(it, "VpcId", "SubnetId")
            made = self._call(
                "CreateDBInstanceHour", Clone=5, Father=self.id,
                RestoreTime=when, InstanceName=name, GoodsNum=1,
                Memory=int(it.get("Memory") or 0) // 1024,
                Volume=int(it.get("Volume") or 0) // 1024,
                ReplicateSetNum=it.get("ReplicationSetNum"),
                NodeNum=int(it.get("SecondaryNum") or 0) + 1,
                MongoVersion=it.get("MongoVersion"),
                MachineCode=it.get("MachineType"),
                ClusterType="SHARD" if it.get("ClusterType") == 1
                else "REPLSET", Zone=it.get("Zone"), VpcId=vpc,
                SubnetId=subnet, Password=self.conf.get("password"))[
                "InstanceIds"][0]
            log(f"{what} of {self.id} asked for")
            new = _ready(lambda: (lambda i: (i.get("Status"), i))(
                self._call("DescribeDBInstances", InstanceIds=[made])[
                    "InstanceDetails"][0]), what, log, ("2",), ("-2", "-3"))
            return {"host": new.get("Vip"), "port": new.get("Vport"),
                    "id": made, "kind": "instance"}
        if p == "redis":
            if point is None:
                point = self._latest(None)
                backup = True
            vpc, subnet = self._net(it)
            made = self._call(
                "CloneInstances", InstanceId=self.id, GoodsNum=1,
                ZoneId=it.get("ZoneId"), BillingMode=0,
                BackupId=point.id if backup else None,
                CloneTime=None if backup else when, VpcId=vpc,
                SubnetId=subnet, InstanceName=name,
                NoAuth=bool(it.get("NoAuth")),
                Password=self.conf.get("password"))["InstanceIds"][0]
            log(f"{what} of {self.id} asked for")
            new = _ready(lambda: (lambda i: (i.get("Status"), i))(
                self._call("DescribeInstances", InstanceId=made)[
                    "InstanceSet"][0]), what, log, ("2",), ("-2", "-3"))
            return {"host": new.get("WanIp"), "port": new.get("Port"),
                    "id": made, "kind": "instance"}
        vpc, subnet = self._net(it, "VpcId", "SubnetId")
        rw = next((i for i in it.get("InstanceSet") or ()
                   if str(i.get("InstanceRole")).lower() in ("master", "rw")),
                  {})
        made = self._call(
            "RollbackToNewCluster", Zone=it.get("Zone"),
            OriginalClusterId=self.id, UniqVpcId=vpc, UniqSubnetId=subnet,
            ClusterName=name, RollbackId=int(point.id) if backup else 0,
            ExpectTime=when if not backup or point.at else None,
            InstanceInitInfos=[{"Cpu": rw.get("InstanceCpu"),
                                "Memory": rw.get("InstanceMemory"),
                                "InstanceType": "rw", "InstanceCount": 1}])[
            "ClusterIds"][0]
        log(f"{what} of {self.id} asked for")
        new = _ready(lambda: (lambda c: (c.get("Status"), c))(
            self._call("DescribeClusterDetail", ClusterId=made)["Detail"]),
            what, log, ("running",), ("isolated", "offlined", "deleted"))
        return {"host": new.get("Vip"), "port": new.get("Vport"), "id": made,
                "kind": "cluster"}

    # parameters ------------------------------------------------------------

    def _set(self, values, log):
        p, what = self.product, f"the parameters of {self.id}"
        rows = self._param_rows()
        for n in values:
            if n not in rows:
                raise SystemExit(f"{self.id} has no parameter {n}")
            if rows[n][1] is False:
                raise SystemExit(f"{n} cannot be changed on {self.id}")
        wanted = {n: str(v) for n, v in values.items()}
        restart = [n for n in values if rows[n][2]]
        if p == "cdb":
            self._wait_async(self._call(
                "ModifyInstanceParam", InstanceIds=[self.id],
                ParamList=[{"Name": n, "CurrentValue": v} for n, v in
                           wanted.items()], WaitSwitch=0)["AsyncRequestId"],
                what, log)
        elif p == "postgres":
            task = self._call("ModifyDBInstanceParameters",
                              DBInstanceId=self.id,
                              ParamList=[{"Name": n, "ExpectedValue": v} for
                                         n, v in wanted.items()]).get(
                "TaskId")
            if task:
                _ready(lambda: (((self._call("DescribeTasks", TaskId=task)
                                  .get("TaskSet") or [{}])[0]).get(
                    "Status", ""), True), what, log, ("success",),
                    ("fail", "pause"))
        elif p in ("mongodb", "redis"):
            got = self._call("ModifyInstanceParams", InstanceId=self.id,
                             InstanceParams=[{"Key": n, "Value": v} for n, v
                                             in wanted.items()],
                             **({"ModifyType": "IMMEDIATELY"}
                                if p == "mongodb" else {}))
            if got.get("Changed") is False:
                raise SystemExit(f"Tencent Cloud's API did not change"
                                 f" {', '.join(values)} on {self.id}")
            if p == "redis" and got.get("TaskId"):
                _ready(lambda: (self._call(
                    "DescribeTaskInfo", TaskId=got["TaskId"]).get(
                    "Status", ""), True), what, log, ("succeed",),
                    ("failed", "error"))
        elif p == "cynosdb":
            self._call("ModifyClusterParam", ClusterId=self.id,
                       IsInMaintainPeriod="no",
                       ParamList=[{"ParamName": n, "CurrentValue": v,
                                   "OldValue": str(rows[n][0])}
                                  for n, v in wanted.items()])
        elif p == "sqlserver":
            self._call("ModifyInstanceParam", InstanceIds=[self.id],
                       WaitSwitch=0, ParamList=[
                           {"Name": n, "CurrentValue": v}
                           for n, v in wanted.items()])
        else:
            for r in self._call("ModifyDBParameters", InstanceId=self.id,
                                Params=[{"Param": n, "Value": v} for n, v in
                                        wanted.items()]).get("Result") or ():
                if r.get("Code") != 0:
                    raise SystemExit(f"Tencent Cloud's API did not change"
                                     f" {r.get('Param')} on {self.id}"
                                     f" (code {r.get('Code')})")
        log(f"{', '.join(values)} asked to change on {self.id}")
        now = [n for n in wanted if n not in restart]
        until(lambda: None if any(str(self._param_rows()[n][0]) != wanted[n]
                                  for n in now) else True, what, log)
        if restart:
            log(f"{', '.join(restart)} take effect when {self.id} restarts;"
                " the provider restarts it for them")
        return {n: rows[n][0] for n in values}


# --- Alibaba Cloud -----------------------------------------------------------

#: product -> the SDK's package
ALIBABA = {"rds": "alibabacloud_rds20140815",
           "dds": "alibabacloud_dds20151201",
           "polardb": "alibabacloud_polardb20170801",
           "kvstore": "alibabacloud_r_kvstore20150101"}


def _snake(action):
    """The SDK's method for an action: `DescribeDBInstanceAttribute` ->
    `describe_dbinstance_attribute`."""
    return re.sub(r"(?<=[a-z0-9])(?=[A-Z])", "_", action).lower()


def _items(got, *path):
    """The list at `path` of an answer, [] where any step is missing."""
    for key in path:
        got = (got or {}).get(key) if isinstance(got, dict) else None
    return got if isinstance(got, list) else [got] if got else []


def _ali_time(when):
    return _time(when).astimezone(datetime.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ")


class Alibaba(Adapter):
    """ApsaraDB RDS, ApsaraDB for MongoDB (dds), PolarDB and Tair / Redis
    (kvstore) through Alibaba Cloud's API. A call is named by its action
    (`DescribeDBInstanceAttribute`) and its fields are the request
    model's own - a name the model does not have is refused before
    anything is sent."""
    provider, WHO = "alibaba", "Alibaba Cloud"
    PRODUCTS = {
        "rds": {"restore_point": "backup", "clone": True,
                "parameters": True},
        "dds": {"restore_point": "backup", "clone": True,
                "parameters": True},
        "polardb": {"restore_point": "backup", "clone": True,
                    "parameters": True},
        "kvstore": {"restore_point": "backup", "clone": True,
                    "parameters": True},
    }
    CLI_FILE = Path(".aliyun") / "config.json"
    SDK_FILE = Path(".alibabacloud") / "credentials.ini"

    def __init__(self, conf):
        super().__init__(conf)
        self._client = None

    # credentials ------------------------------------------------------------

    @staticmethod
    def _works(provider):
        try:
            return provider.get_credentials() is not None
        except Exception:  # noqa: BLE001 - this place holds none
            return False

    def _cli(self, named):
        """The CLI's profile - the named one, else ALIBABA_CLOUD_PROFILE,
        else the CLI's current one - and the region it names."""
        from alibabacloud_credentials.provider import \
            CLIProfileCredentialsProvider
        path = Path.home() / self.CLI_FILE
        if os.environ.get("ALIBABA_CLOUD_CLI_PROFILE_DISABLED", "").lower() \
                == "true" or not path.exists():
            return None
        try:
            config = json.loads(path.read_text())
        except ValueError:
            return None
        name = named or os.environ.get("ALIBABA_CLOUD_PROFILE") or \
            config.get("current") or "default"
        got = CLIProfileCredentialsProvider(profile_name=name,
                                            profile_file=str(path))
        if not self._works(got):
            return None
        self.region = self.region or next(
            (p.get("region_id", "") for p in config.get("profiles") or ()
             if p.get("name") == name), "")
        return got, f"the CLI's profile {name}"

    def _credentials(self):
        from alibabacloud_credentials.client import Client
        from alibabacloud_credentials.provider import (
            EcsRamRoleCredentialsProvider, OIDCRoleArnCredentialsProvider,
            ProfileCredentialsProvider, RamRoleArnCredentialsProvider,
            StaticAKCredentialsProvider, StaticSTSCredentialsProvider,
            URLCredentialsProvider)
        env, named, found = os.environ, self.conf.get("profile"), None
        if not named and env.get("ALIBABA_CLOUD_ACCESS_KEY_ID") and \
                env.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET"):
            kw = {"access_key_id": env["ALIBABA_CLOUD_ACCESS_KEY_ID"],
                  "access_key_secret": env["ALIBABA_CLOUD_ACCESS_KEY_SECRET"]}
            found = ((StaticSTSCredentialsProvider(
                security_token=env["ALIBABA_CLOUD_SECURITY_TOKEN"], **kw)
                if env.get("ALIBABA_CLOUD_SECURITY_TOKEN")
                else StaticAKCredentialsProvider(**kw)),
                "the environment (ALIBABA_CLOUD_ACCESS_KEY_ID)")
        if found is None and not named and all(env.get(k) for k in (
                "ALIBABA_CLOUD_ROLE_ARN", "ALIBABA_CLOUD_OIDC_PROVIDER_ARN",
                "ALIBABA_CLOUD_OIDC_TOKEN_FILE")):
            found = (OIDCRoleArnCredentialsProvider(
                role_arn=env["ALIBABA_CLOUD_ROLE_ARN"],
                oidc_provider_arn=env["ALIBABA_CLOUD_OIDC_PROVIDER_ARN"],
                oidc_token_file_path=env["ALIBABA_CLOUD_OIDC_TOKEN_FILE"],
                role_session_name="migkit"),
                "the OIDC token ALIBABA_CLOUD_OIDC_TOKEN_FILE names")
        if found is None:
            found = self._cli(named)
        if found is None and named:
            raise SystemExit(f"the CLI's profile {named} holds no Alibaba"
                             " Cloud credentials")
        if found is None:
            path = env.get("ALIBABA_CLOUD_CREDENTIALS_FILE") or str(
                Path.home() / self.SDK_FILE)
            got = ProfileCredentialsProvider(
                profile_file=path,
                profile_name=env.get("ALIBABA_CLOUD_PROFILE") or "default")
            if Path(path).exists() and self._works(got):
                found = got, "the SDK's own credentials file"
        if found is None and env.get(
                "ALIBABA_CLOUD_ECS_METADATA_DISABLED", "").lower() != "true":
            got = EcsRamRoleCredentialsProvider(
                role_name=env.get("ALIBABA_CLOUD_ECS_METADATA") or None)
            if self._works(got):
                found = got, "the instance's RAM role"
        if found is None and env.get("ALIBABA_CLOUD_CREDENTIALS_URI"):
            got = URLCredentialsProvider(
                uri=env["ALIBABA_CLOUD_CREDENTIALS_URI"])
            if self._works(got):
                found = got, "the address ALIBABA_CLOUD_CREDENTIALS_URI names"
        if found is None:
            raise SystemExit(
                "no Alibaba Cloud credentials: looked in the environment"
                " (ALIBABA_CLOUD_ACCESS_KEY_ID and"
                " ALIBABA_CLOUD_ACCESS_KEY_SECRET), an OIDC token, the CLI's"
                " profile, the SDK's own credentials file, the instance's RAM"
                " role and a credentials address")
        self.region = self.region or next(
            (env[k] for k in ("ALIBABA_CLOUD_REGION_ID",
                              "ALIBABACLOUD_REGION_ID", "ALICLOUD_REGION_ID")
             if env.get(k)), "")
        if not self.region:
            raise SystemExit("no Alibaba Cloud region: name it (region) or"
                             " set the CLI profile's region")
        provider, where = found
        if self.role:
            provider = RamRoleArnCredentialsProvider(
                credentials_provider=provider, role_arn=self.role,
                role_session_name="migkit")
            where = f"the RAM role {self.role}, assumed over {where}"
        return Client(provider=provider), where

    # the calls --------------------------------------------------------------

    def _models(self):
        import importlib
        return importlib.import_module(ALIBABA[self.product] + ".models")

    def client(self):
        if self._client is None:
            import importlib

            from alibabacloud_tea_openapi.models import Config
            cred = self.credentials()[0]
            self._client = importlib.import_module(
                ALIBABA[self.product] + ".client").Client(
                Config(credential=cred, region_id=self.region))
        return self._client

    def _call(self, action, **fields):
        """The action asked with `fields`, its answer as the API writes it."""
        from Tea.exceptions import TeaException
        fields = {k: v for k, v in fields.items() if v is not None}
        req = getattr(self._models(), action + "Request")().from_map(fields)
        unknown = sorted(set(fields) - set(req.to_map()))
        if unknown:
            raise ValueError(f"{action} has no {', '.join(unknown)}")
        try:
            got = getattr(self.client(), _snake(action))(req)
        except TeaException as e:
            raise SystemExit(f"Alibaba Cloud's API refused {action} for"
                             f" {self.id}: {e.message} ({e.code})") from None
        return got.body.to_map() if got.body is not None else {}

    def _pages(self, action, *path, **fields):
        out, page = [], 1
        while True:
            got = self._call(action, PageNumber=page, PageSize=100, **fields)
            items = _items(got, *path)
            out += items
            if len(items) < 100:
                return out
            page += 1

    # what the product is ---------------------------------------------------

    def _one(self):
        p = self.product
        if p == "rds":
            got = _items(self._call("DescribeDBInstanceAttribute",
                                    DBInstanceId=self.id),
                         "Items", "DBInstanceAttribute")
        elif p == "dds":
            got = _items(self._call("DescribeDBInstanceAttribute",
                                    DBInstanceId=self.id),
                         "DBInstances", "DBInstance")
        elif p == "polardb":
            got = [self._call("DescribeDBClusterAttribute",
                              DBClusterId=self.id)]
        else:
            got = _items(self._call("DescribeInstanceAttribute",
                                    InstanceId=self.id),
                         "Instances", "DBInstanceAttribute")
        if not got or not got[0]:
            raise SystemExit(f"Alibaba Cloud's API lists no {self.product}"
                             f" {self.id} in {self.region}")
        return got[0]

    def _param_rows(self, it=None):
        """{name: (value, modifiable, restart)} of the parameters the
        instance runs with."""
        p, out = self.product, {}
        if p == "rds":
            got = self._call("DescribeParameters", DBInstanceId=self.id)
            it = it or self._one()
            rules = {r.get("ParameterName"): r for r in _items(self._call(
                "DescribeParameterTemplates", Engine=it.get("Engine"),
                EngineVersion=it.get("EngineVersion"),
                Category=it.get("Category")), "Parameters",
                "TemplateRecord")}
            for r in _items(got, "RunningParameters", "DBInstanceParameter"):
                rule = rules.get(r.get("ParameterName"), {})
                out[r["ParameterName"]] = (r.get("ParameterValue"),
                                           _flag(rule.get("ForceModify")),
                                           _flag(rule.get("ForceRestart")))
            return out
        if p == "polardb":
            got = _items(self._call("DescribeDBClusterParameters",
                                    DBClusterId=self.id),
                         "RunningParameters", "Parameter")
            for r in got:
                out[r["ParameterName"]] = (r.get("ParameterValue"),
                                           _flag(r.get("IsModifiable")),
                                           _flag(r.get("ForceRestart")))
            return out
        for r in _items(self._call("DescribeParameters",
                                   DBInstanceId=self.id),
                        "RunningParameters", "Parameter"):
            out[r["ParameterName"]] = (r.get("ParameterValue"),
                                       _flag(r.get("ModifiableStatus")),
                                       _flag(r.get("ForceRestart")))
        return out

    def _address(self):
        """(host, port) the instance answers at inside its network."""
        p = self.product
        if p == "rds":
            nets = _items(self._call("DescribeDBInstanceNetInfo",
                                     DBInstanceId=self.id),
                          "DBInstanceNetInfos", "DBInstanceNetInfo")
            n = next((n for n in nets if n.get("IPType") in (
                "Private", "Inner")), nets[0] if nets else {})
            return n.get("ConnectionString", ""), _num(n.get("Port"))
        if p == "polardb":
            for e in _items(self._call("DescribeDBClusterEndpoints",
                                       DBClusterId=self.id), "Items"):
                if e.get("EndpointType") in ("Cluster", "Primary"):
                    a = next((a for a in e.get("AddressItems") or ()
                              if a.get("NetType") == "Private"), {})
                    if a:
                        return a.get("ConnectionString", ""), _num(
                            a.get("Port"))
            return "", None
        return "", None

    def instance(self, names=()):
        it, p = self._one(), self.product
        got = Instance(self.provider, p, self.id, group="",
                       group_shared=False)
        if p == "rds":
            got.engine, got.version = str(it.get("Engine", "")).lower(), \
                it.get("EngineVersion", "")
            got.klass, got.cpu = it.get("DBInstanceClass", ""), _num(it.get(
                "DBInstanceCPU"))
            got.memory_gb = _gb(it.get("DBInstanceMemory"), 1024)
            got.storage_gb = _num(it.get("DBInstanceStorage"))
            got.max_connections = _num(it.get("MaxConnections"))
            got.charset, got.time_zone = it.get("Collation", ""), it.get(
                "TimeZone", "")
            got.maintenance = (f"every day {it['MaintainTime']} UTC"
                               .replace("Z", "") if it.get("MaintainTime")
                               else "")
            got.status = it.get("DBInstanceStatus", "")
            got.host, got.port = self._address()
            got.host = got.host or it.get("ConnectionString", "")
            got.port = got.port or _num(it.get("Port"))
        elif p == "dds":
            got.engine, got.version = "mongodb", it.get("EngineVersion", "")
            got.klass = it.get("DBInstanceClass", "")
            got.storage_gb = _num(it.get("DBInstanceStorage"))
            got.max_connections = _num(it.get("MaxConnections"))
            got.maintenance = (
                f"every day {it['MaintainStartTime']}-"
                f"{it.get('MaintainEndTime')} UTC".replace("Z", "")
                if it.get("MaintainStartTime") else "")
            got.status = it.get("DBInstanceStatus", "")
            primary = next((r for r in _items(it, "ReplicaSets", "ReplicaSet")
                            if r.get("ReplicaSetRole") == "Primary"), None)
            mongos = _items(it, "MongosList", "MongosAttribute")
            got.host = (primary or {}).get("ConnectionDomain") or (
                mongos[0].get("ConnectString") if mongos else "")
            got.port = _num((primary or {}).get("ConnectionPort") or (
                mongos[0].get("Port") if mongos else None))
        elif p == "polardb":
            writer = next((n for n in it.get("DBNodes") or ()
                           if n.get("DBNodeRole") == "Writer"),
                          (it.get("DBNodes") or [{}])[0])
            got.engine, got.version = str(it.get("DBType", "")).lower(), \
                it.get("DBVersion", "")
            got.klass, got.cpu = writer.get("DBNodeClass", ""), _num(
                writer.get("CpuCores"))
            got.memory_gb = _gb(writer.get("MemorySize"), 1024)
            got.max_connections = _num(writer.get("MaxConnections"))
            got.storage_gb = _gb(it.get("StorageSpace"), 1 << 30)
            got.maintenance = (f"every day {it['MaintainTime']} UTC"
                               .replace("Z", "") if it.get("MaintainTime")
                               else "")
            got.status = it.get("DBClusterStatus", "")
            got.host, got.port = self._address()
        else:
            got.engine, got.version = str(it.get("InstanceType", "redis")) \
                .lower(), it.get("EngineVersion", "")
            got.klass = it.get("InstanceClass", "")
            got.memory_gb = _gb(it.get("Capacity"), 1024)
            got.max_connections = _num(it.get("Connections"))
            got.maintenance = (
                f"every day {it['MaintainStartTime']}-"
                f"{it.get('MaintainEndTime')} UTC".replace("Z", "")
                if it.get("MaintainStartTime") else "")
            got.status = it.get("InstanceStatus", "")
            got.host, got.port = it.get("ConnectionDomain", ""), _num(
                it.get("Port"))
        if names:
            rows = self._param_rows(it if p == "rds" else None)
            got.parameters = {n: Param(*rows[n]) if n in rows else Param()
                              for n in names}
        return got

    def find(self, host, port=None):
        host, p = str(host), self.product
        if p == "rds":
            got = _items(self._call("DescribeDBInstances",
                                    ConnectionString=host),
                         "Items", "DBInstance")
            if got:
                return got[0]["DBInstanceId"]
            for i in self._pages("DescribeDBInstances", "Items",
                                 "DBInstance"):
                for n in _items(self._call(
                        "DescribeDBInstanceNetInfo",
                        DBInstanceId=i["DBInstanceId"]),
                        "DBInstanceNetInfos", "DBInstanceNetInfo"):
                    if host in (n.get("IPAddress"),
                                n.get("ConnectionString")):
                        return i["DBInstanceId"]
            return None
        if p == "dds":
            got = _items(self._call("DescribeDBInstances",
                                    ConnectionDomain=host),
                         "DBInstances", "DBInstance")
            return got[0]["DBInstanceId"] if got else None
        if p == "polardb":
            got = _items(self._call("DescribeDBClusters",
                                    ConnectionString=host),
                         "Items", "DBCluster")
            return got[0]["DBClusterId"] if got else None
        for i in self._pages("DescribeInstances", "Instances",
                             "KVStoreInstance"):
            if host in (i.get("ConnectionDomain"), i.get("PrivateIp")):
                return i["InstanceId"]
        return None

    # restore points --------------------------------------------------------

    def _backups(self, **fields):
        """The backups the product lists for the instance, as the API
        writes them. MongoDB's and Redis's listings are asked for the
        instance's whole life, the span they require."""
        p = self.product
        if p == "rds":
            return self._pages("DescribeBackups", "Items", "Backup",
                               DBInstanceId=self.id, **fields)
        if p == "polardb":
            return self._pages("DescribeBackups", "Items", "Backup",
                               DBClusterId=self.id, **fields)
        it = self._one()
        born = it.get("CreationTime") or it.get("CreateTime")
        span = {"StartTime": _time(born).astimezone(datetime.timezone.utc)
                .strftime("%Y-%m-%dT%H:%MZ") if born else None,
                "EndTime": _now().strftime("%Y-%m-%dT%H:%MZ")}
        if p == "dds":
            return self._pages("DescribeBackups", "Backups", "Backup",
                               DBInstanceId=self.id, **span, **fields)
        return self._pages("DescribeBackups", "Backups", "Backup",
                           InstanceId=self.id, **span, **fields)

    @staticmethod
    def _point_of(b):
        status = str(b.get("BackupStatus", ""))
        return RestorePoint(
            str(b.get("BackupId")), "backup", status,
            _stamp(b["BackupEndTime"]) if b.get("BackupEndTime") else "",
            status.lower() == "success")

    def restore_points(self):
        return sorted((self._point_of(b) for b in self._backups()),
                      key=lambda r: r.at)

    def _make(self, name, log):
        """The backup asked for, its task waited for where the product
        gives one, and the backup then waited for in the product's list
        until it is ready - found by its id, or on PolarDB by the comment
        it was made with."""
        p, what, made = self.product, f"the backup {name}", ""
        if p == "rds":
            job = self._call("CreateBackup", DBInstanceId=self.id,
                             BackupMethod="Physical")["BackupJobId"]
            log(f"{what} of {self.id} asked for")
            made = str(_ready(lambda: (lambda j: (j.get("BackupStatus"), j))(
                (_items(self._call("DescribeBackupTasks",
                                   DBInstanceId=self.id,
                                   BackupJobId=int(job)),
                        "Items", "BackupJob") or [{}])[0]), what, log,
                ("finished",), ("failed",)).get("BackupId") or "")
        elif p == "dds":
            made = str(self._call("CreateBackup", DBInstanceId=self.id,
                                  BackupMethod="Physical")["BackupId"])
            log(f"{what} of {self.id} asked for")
        elif p == "polardb":
            self._call("CreateBackup", DBClusterId=self.id, Comment=name)
            log(f"{what} of {self.id} asked for")
        else:
            job = str(self._call("CreateBackup",
                                 InstanceId=self.id)["BackupJobID"])
            log(f"{what} of {self.id} asked for")
            _ready(lambda: (((_items(self._call(
                "DescribeBackupTasks", InstanceId=self.id, BackupJobId=job),
                "BackupJobs") or [{}])[0]).get("BackupProgressStatus"),
                True), what, log, ("finished",), ("failed",))
            made = next((str(b.get("BackupId")) for b in self._backups(
                BackupJobId=int(job))), "")
        if p != "polardb" and not made:
            raise SystemExit(f"{what} was made, and the provider does not"
                             " list it")

        def ask():
            for b in self._backups():
                if (str(b.get("BackupId")) == made) if made else (
                        b.get("Comment") == name):
                    got = self._point_of(b)
                    return ("ready" if got.ready else got.status), got
            return "", None
        return _ready(ask, what, log, ("ready",), ("failed",))

    # copies ----------------------------------------------------------------

    def _clone(self, name, point, log):
        p, it, what = self.product, self._one(), f"the copy {name}"
        backup = point is not None and point.kind == "backup"
        when = _ali_time(point.at if point is not None and point.at
                         else _now())
        vpc = self.conf.get("vpc") or it.get("VpcId") or it.get("VPCId")
        subnet = self.conf.get("subnet") or it.get("VSwitchId")
        if p == "rds":
            made = self._call(
                "CloneDBInstance", DBInstanceId=self.id, PayType="Postpaid",
                BackupId=point.id if backup else None,
                RestoreTime=None if backup else when,
                DBInstanceClass=self.conf.get("spec")
                or it.get("DBInstanceClass"),
                DBInstanceStorage=int(self.conf.get("storage")
                                      or it.get("DBInstanceStorage")),
                DBInstanceStorageType=it.get("DBInstanceStorageType"),
                ZoneId=it.get("ZoneId"), VPCId=vpc, VSwitchId=subnet,
                InstanceNetworkType="VPC")["DBInstanceId"]
            log(f"{what} of {self.id} asked for")
            clone = Alibaba({**self.conf, "id": made})
            clone._found, clone._client = self._found, self._client
            _ready(lambda: (clone._one().get("DBInstanceStatus"), True),
                   what, log, ("running",), ("deleting", "released"))
            host, port = clone._address()
            return {"host": host, "port": port, "id": made,
                    "kind": "instance"}
        if p == "dds":
            made = self._call(
                "CreateDBInstance", SrcDBInstanceId=self.id, Engine="MongoDB",
                EngineVersion=it.get("EngineVersion"),
                DBInstanceClass=self.conf.get("spec")
                or it.get("DBInstanceClass"),
                DBInstanceStorage=int(self.conf.get("storage")
                                      or it.get("DBInstanceStorage")),
                BackupId=point.id if backup else None,
                RestoreTime=None if backup else when,
                RestoreType="1" if backup else "0", ZoneId=it.get("ZoneId"),
                NetworkType="VPC", VpcId=vpc, VSwitchId=subnet,
                ChargeType="PostPaid", DBInstanceDescription=name,
                AccountPassword=self.conf.get("password"))["DBInstanceId"]
        elif p == "polardb":
            made = self._call(
                "CreateDBCluster", CreationOption="CloneFromPolarDB",
                SourceResourceId=self.id,
                CloneDataPoint=point.id if backup else when
                if point is not None else "LATEST",
                DBType=it.get("DBType"), DBVersion=it.get("DBVersion"),
                DBNodeClass=self.conf.get("spec") or next(
                    (n.get("DBNodeClass") for n in it.get("DBNodes") or ()),
                    None), PayType="Postpaid",
                ZoneId=str(it.get("ZoneIds") or "").split(",")[0] or None,
                VPCId=vpc, VSwitchId=subnet, ClusterNetworkType="VPC",
                DBClusterDescription=name)["DBClusterId"]
        else:
            if point is None:
                point, backup = self._latest(None), True
            made = self._call(
                "CreateInstance", SrcDBInstanceId=self.id,
                BackupId=point.id if backup else None,
                RestoreTime=None if backup else when,
                InstanceClass=self.conf.get("spec") or it.get("InstanceClass"),
                EngineVersion=it.get("EngineVersion"),
                InstanceType=it.get("InstanceType"), ZoneId=it.get("ZoneId"),
                NetworkType="VPC", VpcId=vpc, VSwitchId=subnet,
                ChargeType="PostPaid", InstanceName=name,
                Password=self.conf.get("password"))["InstanceId"]
        log(f"{what} of {self.id} asked for")
        clone = Alibaba({**self.conf, "id": made})
        clone._found, clone._client = self._found, self._client
        status = {"dds": "DBInstanceStatus", "polardb": "DBClusterStatus",
                  "kvstore": "InstanceStatus"}[p]
        _ready(lambda: (clone._one().get(status), True), what, log,
               ("running", "normal"), ("deleting", "released", "deleted"))
        got = clone.instance()
        return {"host": got.host, "port": got.port, "id": made,
                "kind": "cluster" if p == "polardb" else "instance"}

    # parameters ------------------------------------------------------------

    def _set(self, values, log):
        p, what = self.product, f"the parameters of {self.id}"
        rows = self._param_rows()
        for n in values:
            if n not in rows:
                raise SystemExit(f"{self.id} has no parameter {n}")
            if rows[n][1] is False:
                raise SystemExit(f"{n} cannot be changed on {self.id}")
        wanted = {n: str(v) for n, v in values.items()}
        restart = [n for n in values if rows[n][2]]
        body = json.dumps(wanted)
        if p == "rds":
            self._call("ModifyParameter", DBInstanceId=self.id,
                       Parameters=body, Forcerestart=bool(restart))
        elif p == "dds":
            self._call("ModifyParameters", DBInstanceId=self.id,
                       Parameters=body)
        elif p == "polardb":
            self._call("ModifyDBClusterParameters", DBClusterId=self.id,
                       Parameters=body)
        else:
            self._call("ModifyInstanceConfig", InstanceId=self.id,
                       Config=body)
        log(f"{', '.join(values)} asked to change on {self.id}"
            + (f"; the provider restarts it for {', '.join(restart)}"
               if restart else ""))
        until(lambda: None if any(
            str(self._param_rows().get(n, (None,))[0]) != v
            for n, v in wanted.items()) else True, what, log)
        return {n: rows[n][0] for n in values}


# --- Huawei Cloud ------------------------------------------------------------

class Huawei(Adapter):
    """RDS and DDS through Huawei Cloud's API. Its SDK pins libraries older
    than migkit's, so it runs in an environment of its own and is driven as
    a program (`huawei_sdk`): one request in, one answer out. The
    credentials are the keys found, passed to it with each request; none,
    where the instance's own agency answers."""
    provider, WHO = "huawei", "Huawei Cloud"
    PRODUCTS = {
        "rds": {"restore_point": "backup", "clone": True,
                "parameters": True},
        "dds": {"restore_point": "backup", "clone": True,
                "parameters": False},
    }
    WHY = {("dds", "parameters"):
           "a DDS instance's parameters are set per node group through its"
           " parameter templates"}
    CLI_DIR = ".hcloud"
    #: the CLI's configuration: its name and its profiles' fields are the
    #: CLI's documentation's, not read off a build here - the CLI is a
    #: program this machine need not have
    CLI_FILES = ("config.json", "hcloud.json")
    SDK_FILE = Path(".huaweicloud") / "credentials"
    SCRIPT = Path(__file__).with_name("huawei_sdk.py")
    TIMEOUT = 600

    def _run(self, ask):
        """One request answered by the SDK's program in its own
        environment."""
        import subprocess

        from . import tools
        python = tools.own_env_python(tools.HUAWEI_SDK)
        if not python:
            raise SystemExit("Huawei Cloud's API is driven from an"
                             " environment of its own, which is not"
                             " installed on this machine: migkit doctor"
                             " --install")
        p = subprocess.run([python, str(self.SCRIPT)], input=json.dumps(ask),
                           capture_output=True, text=True,
                           timeout=self.TIMEOUT)
        try:
            got = json.loads(p.stdout or "{}")
        except ValueError:
            got = {"error": (p.stderr or p.stdout or "").strip()[-300:]}
        if p.returncode or "error" in got:
            raise SystemExit(f"Huawei Cloud's API refused"
                             f" {ask.get('action') or ask['service']} for"
                             f" {self.id}: {got.get('error')}")
        return got

    # credentials ------------------------------------------------------------

    def _cli(self, named):
        """The CLI's profile - the named one, else its current one."""
        for name in self.CLI_FILES:
            path = Path.home() / self.CLI_DIR / name
            try:
                config = json.loads(path.read_text())
            except (OSError, ValueError):
                continue
            want = named or config.get("current") or "default"
            for p in config.get("profiles") or ():
                if p.get("name") != want or not (
                        p.get("accessKeyId") and p.get("secretAccessKey")):
                    continue
                self.region = self.region or p.get("region", "")
                return ({"ak": p["accessKeyId"], "sk": p["secretAccessKey"],
                         "security_token": p.get("securityToken") or None,
                         "project_id": p.get("projectId") or None},
                        f"the CLI's profile {want}")
        return None

    def _sdk_file(self):
        """The SDK's own file, its [basic] section."""
        import configparser
        path = Path(os.environ.get("HUAWEICLOUD_SDK_CREDENTIALS_FILE")
                    or Path.home() / self.SDK_FILE)
        ini = configparser.ConfigParser()
        if not ini.read(path) or not ini.has_section("basic"):
            return None
        keys = {k: ini.get("basic", k, fallback="").strip() or None for k in
                ("ak", "sk", "security_token", "project_id")}
        return (keys, "the SDK's own credentials file") if keys["ak"] and \
            keys["sk"] else None

    def _credentials(self):
        env, named, found = os.environ, self.conf.get("profile"), None
        if not named and env.get("HUAWEICLOUD_SDK_AK") and \
                env.get("HUAWEICLOUD_SDK_SK"):
            found = ({"ak": env["HUAWEICLOUD_SDK_AK"],
                      "sk": env["HUAWEICLOUD_SDK_SK"],
                      "security_token": env.get(
                          "HUAWEICLOUD_SDK_SECURITY_TOKEN") or None,
                      "project_id": env.get("HUAWEICLOUD_SDK_PROJECT_ID")
                      or None}, "the environment (HUAWEICLOUD_SDK_AK)")
        if found is None:
            found = self._cli(named)
        if found is None and named:
            raise SystemExit(f"the CLI's profile {named} holds no Huawei"
                             " Cloud keys")
        if found is None:
            found = self._sdk_file()
        if found is None:
            try:
                if self._run({"service": "credentials"}).get("found"):
                    found = {}, "the instance's agency"
            except SystemExit:
                pass
        if found is None:
            raise SystemExit(
                "no Huawei Cloud credentials: looked in the environment"
                " (HUAWEICLOUD_SDK_AK and HUAWEICLOUD_SDK_SK), the CLI's"
                " profile, the SDK's own credentials file and the instance's"
                " agency")
        if not self.region:
            raise SystemExit("no Huawei Cloud region: name it (region) or"
                             " set the CLI profile's region")
        keys, where = found
        keys = {k: v for k, v in keys.items() if v}
        if self.conf.get("project_id"):
            keys["project_id"] = self.conf["project_id"]
        if not self.role:
            return keys, where
        return self._assume(keys), \
            f"the agency {self.role}, assumed over {where}"

    def _assume(self, keys):
        """Temporary keys of an agency: `iam::<account id>:agency:<name>`,
        or `<account name>/<name>`."""
        m = re.match(r"iam::(\w+):agency:(.+)$", self.role)
        owner, agency = (m.group(1), m.group(2)) if m else \
            self.role.split("/", 1)
        got = self._run({
            "service": "iam",
            "action": "create_temporary_access_key_by_agency",
            "region": self.region, **keys,
            "request": {"body": {"auth": {"identity": {
                "methods": ["assume_role"],
                "assume_role": {"agency_name": agency,
                                "domain_id" if m else "domain_name": owner,
                                "duration_seconds": 43200}}}}}})["credential"]
        return {"ak": got["access"], "sk": got["secret"],
                "security_token": got["securitytoken"],
                **({"project_id": keys["project_id"]}
                   if keys.get("project_id") else {})}

    def _call(self, action, **request):
        keys = self.credentials()[0]
        return self._run({"service": self.product, "action": action,
                          "request": request, "region": self.region, **keys})

    # what the product is ---------------------------------------------------

    def _one(self, ident=None):
        got = self._call("list_instances", id=ident or self.id).get(
            "instances") or []
        if not got:
            raise SystemExit(f"Huawei Cloud's API lists no {self.product}"
                             f" {ident or self.id} in {self.region}")
        return got[0]

    def _rds_rows(self):
        return {r["name"]: (r.get("value"), not r.get("readonly"),
                            _flag(r.get("restart_required")))
                for r in self._call("show_instance_configuration",
                                    instance_id=self.id).get(
                    "configuration_parameters") or ()}

    @staticmethod
    def _dds_address(it):
        nodes = [n for g in it.get("groups") or () for n in
                 g.get("nodes") or ()]
        primary = next((n for n in nodes if str(n.get("role")).lower()
                        == "primary"), None) or next(
            (n for n in nodes if str(n.get("role")).lower() == "mongos"),
            nodes[0] if nodes else {})
        return primary.get("private_ip", ""), _num(it.get("port")), primary

    def instance(self, names=()):
        it, p = self._one(), self.product
        got = Instance(self.provider, p, self.id, group="",
                       group_shared=False)
        got.version = (it.get("datastore") or {}).get("version", "")
        got.status = it.get("status", "")
        got.time_zone = it.get("time_zone") or ""
        got.maintenance = (f"every day {it['maintenance_window']} UTC"
                           if it.get("maintenance_window") else "")
        if p == "rds":
            kind = str((it.get("datastore") or {}).get("type", "")).lower()
            got.engine = "postgres" if kind == "postgresql" else kind
            got.klass, got.cpu = it.get("flavor_ref", ""), _num(it.get("cpu"))
            got.memory_gb = _num(it.get("mem"))
            got.storage_gb = _num((it.get("volume") or {}).get("size"))
            got.host = ((it.get("private_ips") or [""])[0]
                        or (it.get("private_dns_names") or [""])[0])
            got.port = _num(it.get("port"))
            if names:
                rows = self._rds_rows()
                got.parameters = {n: Param(*rows[n]) if n in rows
                                  else Param() for n in names}
        else:
            got.engine, got.klass = "mongodb", it.get("mode", "")
            got.host, got.port, node = self._dds_address(it)
            got.storage_gb = _num(next((g["volume"].get("size") for g in
                                        it.get("groups") or ()
                                        if g.get("volume")), None))
            got.klass = f"{got.klass} {node.get('spec_code', '')}".strip()
        return got

    def find(self, host, port=None):
        host, offset = str(host), 0
        while True:
            page = self._call("list_instances", offset=offset, limit=100).get(
                "instances") or []
            for it in page:
                if self.product == "rds":
                    here = (it.get("private_ips") or []) + (
                        it.get("private_dns_names") or [])
                else:
                    here = [n.get("private_ip") for g in it.get("groups")
                            or () for n in g.get("nodes") or ()]
                if host in here and (port is None or not it.get("port")
                                     or int(it["port"]) == int(port)):
                    return it["id"]
            if len(page) < 100:
                return None
            offset += 100

    # restore points --------------------------------------------------------

    def _backups(self, **fields):
        out, offset = [], 0
        while True:
            page = self._call("list_backups", instance_id=self.id,
                              offset=offset, limit=100, **fields).get(
                "backups") or []
            out += page
            if len(page) < 100:
                return out
            offset += 100

    @staticmethod
    def _point_of(b):
        return RestorePoint(b.get("id", ""), "backup", b.get("status", ""),
                            _stamp(b.get("end_time") or b.get("begin_time")),
                            b.get("status") == "COMPLETED")

    def restore_points(self):
        return sorted((self._point_of(b) for b in self._backups()),
                      key=lambda r: r.at)

    def _make(self, name, log):
        what = f"the backup {name}"
        if self.product == "rds":
            made = self._call("create_manual_backup", body={
                "instance_id": self.id, "name": name,
                "description": "a restore point"})["backup"]["id"]
        else:
            made = self._call("create_manual_backup", body={"backup": {
                "instance_id": self.id, "name": name,
                "description": "a restore point"}})["backup_id"]
        log(f"{what} of {self.id} asked for")
        return _ready(lambda: (lambda b: (b.get("status", ""), b and
                                          self._point_of(b)))(
            next(iter(self._backups(backup_id=made)), {})), what, log,
            ("completed",), ("failed", "deleted"))

    # copies ----------------------------------------------------------------

    def _job(self, job, what, log):
        action = "list_job_info" if self.product == "rds" \
            else "show_job_detail"
        return _ready(lambda: ((self._call(action, id=job).get("job") or {})
                               .get("status", ""), True), what, log,
                      ("completed",), ("failed",))

    def _clone(self, name, point, log):
        it, what = self._one(), f"the copy {name}"
        backup = point is not None and point.kind == "backup"
        restore = {"instance_id": self.id, "type": "backup",
                   "backup_id": point.id} if backup else {
            "instance_id": self.id, "type": "timestamp",
            "restore_time": int(_time(point.at if point else _now())
                                .timestamp() * 1000)}
        net = {"vpc_id": self.conf.get("vpc") or it.get("vpc_id"),
               "subnet_id": self.conf.get("subnet") or it.get("subnet_id"),
               "security_group_id": it.get("security_group_id")}
        if self.conf.get("password"):
            net["password"] = self.conf["password"]
        if self.product == "rds":
            zones = sorted({n.get("availability_zone") for n in
                            it.get("nodes") or () if n.get(
                                "availability_zone")})
            body = {"name": name, "availability_zone": ",".join(zones),
                    "flavor_ref": self.conf.get("spec") or it["flavor_ref"],
                    "volume": {"type": it["volume"]["type"],
                               "size": int(self.conf.get("storage")
                                           or it["volume"]["size"])},
                    "restore_point": restore, **net}
            if str(it.get("type")).lower() == "ha":
                body["ha"] = {"mode": "Ha", "replication_mode": (
                    it.get("ha") or {}).get("replication_mode")}
            got = self._call("create_restore_instance", body=body)
            made, job = got["instance"]["id"], got.get("job_id")
        else:
            restore["restore_time"] = str(restore.get("restore_time")) \
                if not backup else None
            kinds = {}
            for g in it.get("groups") or ():
                nodes = g.get("nodes") or [{}]
                entry = kinds.setdefault(g["type"], {
                    "type": g["type"], "num": 0,
                    "spec_code": nodes[0].get("spec_code")})
                entry["num"] += len(nodes) if g["type"] in (
                    "mongos", "replica", "single") else 1
                if (g.get("volume") or {}).get("size"):
                    entry["size"] = str(g["volume"]["size"])
            zone = next((n.get("availability_zone") for g in it.get(
                "groups") or () for n in g.get("nodes") or ()
                if n.get("availability_zone")), None)
            got = self._call("restore_new_instance", body={
                "name": name, "availability_zone": zone,
                "flavor": [{**k, "num": str(k["num"])} for k in
                           kinds.values()],
                "restore_point": {k: v for k, v in restore.items()
                                  if v is not None}, **net})
            made, job = got["id"], got.get("job_id")
        log(f"{what} of {self.id} asked for")
        if job:
            self._job(job, what, log)
        new = _ready(lambda: (lambda i: (i.get("status", ""), i))(
            self._one(made)), what, log, ("active", "normal"),
            ("failed", "abnormal"))
        if self.product == "rds":
            host, port = (new.get("private_ips") or [""])[0], _num(
                new.get("port"))
        else:
            host, port, _ = self._dds_address(new)
        return {"host": host, "port": port, "id": made, "kind": "instance"}

    # parameters ------------------------------------------------------------

    def _set(self, values, log):
        what, rows = f"the parameters of {self.id}", self._rds_rows()
        for n in values:
            if n not in rows:
                raise SystemExit(f"{self.id} has no parameter {n}")
            if rows[n][1] is False:
                raise SystemExit(f"{n} cannot be changed on {self.id}")
        wanted = {n: str(v) for n, v in values.items()}
        self._call("update_instance_configuration", instance_id=self.id,
                   body={"values": wanted})
        restart = [n for n in values if rows[n][2]]
        log(f"{', '.join(values)} asked to change on {self.id}")
        now = [n for n in wanted if n not in restart]
        until(lambda: None if any(str(self._rds_rows()[n][0]) != wanted[n]
                                  for n in now) else True, what, log)
        if restart:
            log(f"{', '.join(restart)} take effect at {self.id}'s next"
                " restart")
        return {n: rows[n][0] for n in values}


# --- Oracle Cloud ------------------------------------------------------------

class Oracle(Adapter):
    """HeatWave MySQL DB systems and Autonomous Databases through Oracle
    Cloud's API."""
    provider, WHO = "oracle", "Oracle Cloud"
    PRODUCTS = {
        "mysql": {"restore_point": "backup", "clone": True,
                  "parameters": True},
        "autonomous": {"restore_point": "backup", "clone": True,
                       "parameters": False},
    }
    WHY = {("autonomous", "parameters"):
           "an Autonomous Database's settings are set in SQL, not through"
           " the control plane"}
    FAILED = ("failed", "deleted", "deleting", "terminated", "terminating")

    def _credentials(self):
        if self.role:
            raise SystemExit("Oracle Cloud's API assumes no role: its"
                             " policies grant a user or an instance"
                             " directly")
        import oci
        named = self.conf.get("profile")
        profile = named or os.environ.get("OCI_CLI_PROFILE") or "DEFAULT"
        path = Path(os.environ.get("OCI_CLI_CONFIG_FILE")
                    or Path.home() / ".oci" / "config")
        if path.exists():
            try:
                config = oci.config.from_file(file_location=str(path),
                                              profile_name=profile)
                oci.config.validate_config(config)
            except oci.exceptions.ClientError:
                config = None
            if config:
                self.region = self.region or config.get("region", "")
                return ({"config": config, "signer": None},
                        f"the CLI's config file, profile {profile}")
        if named:
            raise SystemExit(f"the CLI's config file holds no usable profile"
                             f" {named}")
        try:
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        except Exception:  # noqa: BLE001 - not an instance of Oracle's
            raise SystemExit(
                "no Oracle Cloud credentials: looked in the CLI's config"
                f" file (profile {profile}) and the instance's principal") \
                from None
        self.region = self.region or getattr(signer, "region", "")
        return ({"config": {"region": self.region}, "signer": signer},
                "the instance's principal")

    def client(self, name):
        """A client of the SDK's: "mysql.DbSystemClient"."""
        import importlib
        module, cls = name.rsplit(".", 1)
        got = self.credentials()[0]
        config = {**got["config"], "region": self.region
                  or got["config"].get("region")}
        make = getattr(importlib.import_module(f"oci.{module}"), cls)
        return make(config, signer=got["signer"]) if got["signer"] \
            else make(config)

    def _state(self, get, ident, what, log, done=("active", "available")):
        return _ready(lambda: (lambda d: (d.lifecycle_state, d))(
            get(ident).data), what, log, done, self.FAILED)

    def _compartment(self, it=None):
        got = self.conf.get("compartment") or getattr(it, "compartment_id",
                                                      None)
        if not got:
            raise SystemExit("the Oracle Cloud compartment is not named"
                             " (compartment)")
        return got

    # what the product is ---------------------------------------------------

    def _db(self, ident=None):
        if self.product == "mysql":
            return self.client("mysql.DbSystemClient").get_db_system(
                ident or self.id).data
        return self.client("database.DatabaseClient") \
            .get_autonomous_database(ident or self.id).data

    def _variables(self, configuration):
        import oci
        got = self.client("mysql.MysqlaasClient").get_configuration(
            configuration).data
        return oci.util.to_dict(got.variables) if got.variables else {}

    @staticmethod
    def _mysql_address(db):
        ep = (db.endpoints or [None])[0]
        return (getattr(ep, "ip_address", None) or db.ip_address or "",
                getattr(ep, "port", None) or db.port)

    def instance(self, names=()):
        db, p = self._db(), self.product
        got = Instance(self.provider, p, self.id,
                       status=str(db.lifecycle_state or ""))
        if p == "mysql":
            import oci
            got.engine, got.version = "mysql", db.mysql_version or ""
            got.klass = db.shape_name or ""
            shapes = self.client("mysql.MysqlaasClient").list_shapes(
                compartment_id=db.compartment_id, name=db.shape_name).data
            if shapes:
                got.cpu, got.memory_gb = shapes[0].cpu_core_count, _num(
                    shapes[0].memory_size_in_gbs)
            got.storage_gb = _num(db.data_storage_size_in_gbs or getattr(
                db.data_storage, "data_storage_size_in_gbs", None))
            window = getattr(db.maintenance, "window_start_time", "")
            got.maintenance = f"{window} UTC" if window else ""
            got.host, got.port = self._mysql_address(db)
            got.group = db.configuration_id or ""
            got.group_shared = len([s for s in self.client(
                "mysql.DbSystemClient").list_db_systems(
                compartment_id=db.compartment_id,
                configuration_id=db.configuration_id).data
                if s.id != self.id]) > 0
            if names:
                have = self._variables(db.configuration_id)
                known = oci.mysql.models.ConfigurationVariables().swagger_types
                got.parameters = {n: Param(have.get(n), n in known)
                                  for n in names}
        else:
            got.engine, got.version = "oracle", db.db_version or ""
            got.klass = f"{db.db_workload or ''} {db.compute_model or ''}" \
                .strip()
            got.cpu = _num(db.compute_count or db.cpu_core_count)
            per = db.memory_per_compute_unit_in_gbs
            got.memory_gb = _num(got.cpu * per) if got.cpu and per else None
            got.storage_gb = _num(db.data_storage_size_in_gbs) or (
                _num(db.data_storage_size_in_tbs * 1024)
                if db.data_storage_size_in_tbs else None)
            got.charset = db.character_set or ""
            got.host = db.private_endpoint_ip or db.private_endpoint or ""
            got.port, got.group, got.group_shared = 1522, "", False
        return got

    def find(self, host, port=None):
        host = str(host)
        if self.product == "mysql":
            c = self.client("mysql.DbSystemClient")
            for s in c.list_db_systems(
                    compartment_id=self._compartment()).data:
                for ep in s.endpoints or ():
                    if host in (ep.ip_address, ep.hostname):
                        return s.id
            return None
        for a in self.client("database.DatabaseClient") \
                .list_autonomous_databases(
                compartment_id=self._compartment()).data:
            if host in (a.private_endpoint_ip, a.private_endpoint):
                return a.id
        return None

    # restore points --------------------------------------------------------

    @staticmethod
    def _point_of(b):
        state = str(b.lifecycle_state or "")
        return RestorePoint(b.id, "backup", state, _stamp(
            getattr(b, "time_ended", None) or getattr(b, "time_created",
                                                      None)),
                            state.upper() == "ACTIVE")

    def restore_points(self):
        if self.product == "mysql":
            db = self._db()
            got = self.client("mysql.DbBackupsClient").list_backups(
                compartment_id=db.compartment_id, db_system_id=self.id).data
        else:
            got = self.client("database.DatabaseClient") \
                .list_autonomous_database_backups(
                autonomous_database_id=self.id).data
        return sorted((self._point_of(b) for b in got), key=lambda r: r.at)

    def _make(self, name, log):
        import oci
        what = f"the backup {name}"
        if self.product == "mysql":
            c = self.client("mysql.DbBackupsClient")
            made = c.create_backup(oci.mysql.models.CreateBackupDetails(
                db_system_id=self.id, backup_type="FULL",
                display_name=name)).data.id
            log(f"{what} of {self.id} asked for")
            return self._point_of(self._state(c.get_backup, made, what, log))
        c = self.client("database.DatabaseClient")
        made = c.create_autonomous_database_backup(
            oci.database.models.CreateAutonomousDatabaseBackupDetails(
                autonomous_database_id=self.id, display_name=name)).data.id
        log(f"{what} of {self.id} asked for")
        return self._point_of(self._state(c.get_autonomous_database_backup,
                                          made, what, log))

    # copies ----------------------------------------------------------------

    def _clone(self, name, point, log):
        import oci
        db, what = self._db(), f"the copy {name}"
        backup = point is not None and point.kind == "backup"
        if self.product == "mysql":
            m = oci.mysql.models
            source = (m.CreateDbSystemSourceFromBackupDetails(
                backup_id=point.id) if backup else
                m.CreateDbSystemSourceFromPitrDetails(
                    db_system_id=self.id,
                    recovery_point=_time(point.at) if point else None))
            c = self.client("mysql.DbSystemClient")
            made = c.create_db_system(m.CreateDbSystemDetails(
                compartment_id=db.compartment_id, display_name=name,
                shape_name=self.conf.get("spec") or db.shape_name,
                subnet_id=self.conf.get("subnet") or db.subnet_id,
                availability_domain=db.availability_domain,
                configuration_id=db.configuration_id,
                data_storage_size_in_gbs=int(self.conf.get("storage") or (
                    db.data_storage_size_in_gbs or db.data_storage
                    .data_storage_size_in_gbs)),
                source=source)).data.id
            log(f"{what} of {self.id} asked for")
            host, port = self._mysql_address(self._state(
                c.get_db_system, made, what, log))
            return {"host": host, "port": port, "id": made,
                    "kind": "db system"}
        m = oci.database.models
        common = {"compartment_id": db.compartment_id, "display_name": name,
                  "db_name": self.conf.get("db_name") or re.sub(
                      r"[^A-Za-z0-9]", "", name)[:14],
                  "clone_type": "FULL", "compute_model": db.compute_model,
                  "compute_count": db.compute_count,
                  "data_storage_size_in_tbs": db.data_storage_size_in_tbs,
                  "admin_password": self.conf.get("password"),
                  "subnet_id": self.conf.get("subnet") or db.subnet_id}
        if backup:
            details = m.CreateAutonomousDatabaseFromBackupDetails(
                autonomous_database_backup_id=point.id, **common)
        elif point is not None:
            details = m.CreateAutonomousDatabaseFromBackupTimestampDetails(
                autonomous_database_id=self.id, timestamp=_time(point.at),
                **common)
        else:
            details = m.CreateAutonomousDatabaseCloneDetails(
                source_id=self.id, **common)
        c = self.client("database.DatabaseClient")
        made = c.create_autonomous_database(details).data.id
        log(f"{what} of {self.id} asked for")
        new = self._state(c.get_autonomous_database, made, what, log)
        return {"host": new.private_endpoint_ip or new.private_endpoint or "",
                "port": 1522, "id": made, "kind": "autonomous database"}

    # parameters ------------------------------------------------------------

    def _set(self, values, log):
        """A new configuration made from the DB system's own with the
        changed variables - a configuration is never changed in place - and
        the DB system switched to it, waited for until it is active
        again."""
        import oci
        m, db = oci.mysql.models, self._db()
        known = m.ConfigurationVariables().swagger_types
        for n in values:
            if n not in known:
                raise SystemExit(f"{n} is no variable Oracle Cloud's API"
                                 " sets on a DB system")
        have = self._variables(db.configuration_id)
        cast = {"int": int, "float": float, "str": str,
                "bool": lambda v: str(v).lower() in ("true", "1", "on")}
        made = self.client("mysql.MysqlaasClient").create_configuration(
            m.CreateConfigurationDetails(
                compartment_id=db.compartment_id, shape_name=db.shape_name,
                display_name=f"{self.id[-12:]}-{_now():%Y%m%d%H%M%S}",
                parent_configuration_id=db.configuration_id,
                variables=m.ConfigurationVariables(**{
                    n: cast.get(known[n], str)(v)
                    for n, v in values.items()}))).data.id
        c = self.client("mysql.DbSystemClient")
        c.update_db_system(self.id, m.UpdateDbSystemDetails(
            configuration_id=made))
        log(f"{', '.join(values)} asked to change on {self.id}: a"
            f" configuration of its own ({made}) made from its current one")
        self._state(c.get_db_system, self.id, f"the parameters of {self.id}",
                    log, done=("active",))
        return {n: have.get(n) for n in values}


# --- IBM Cloud ---------------------------------------------------------------

#: IBM Cloud Databases' API, per region, and the Resource Controller's
ICD_URL = "https://api.{region}.databases.cloud.ibm.com/v5/ibm"
RC_URL = "https://resource-controller.cloud.ibm.com"


class Ibm(Adapter):
    """IBM Cloud Databases through its REST API, signed by the core
    library's authenticators; a copy is a new deployment the Resource
    Controller makes from a backup or a point in time."""
    provider, WHO = "ibm", "IBM Cloud"
    PRODUCTS = {"icd": {"restore_point": "backup", "clone": True,
                        "parameters": True}}
    #: the CLI's own home, and the file its login is kept in
    CLI_FILE = Path(".bluemix") / "config.json"

    def _credentials(self):
        from ibm_cloud_sdk_core.authenticators import (
            BearerTokenAuthenticator, IAMAssumeAuthenticator,
            IAMAuthenticator, VPCInstanceAuthenticator)
        env = os.environ
        profile = ({"iam_profile_crn": self.role}
                   if self.role.startswith("crn:") else
                   {"iam_profile_id": self.role}) if self.role else {}
        path = Path(env.get("IBMCLOUD_HOME") or Path.home()) / self.CLI_FILE
        try:
            config = json.loads(path.read_text())
        except (OSError, ValueError):
            config = {}
        # the field names are the CLI's, read off its documentation
        self.region = self.region or config.get("Region", "")
        key = env.get("IBMCLOUD_API_KEY") or env.get("IC_API_KEY")
        if key:
            where = "the environment (IBMCLOUD_API_KEY)"
            if self.role:
                return (IAMAssumeAuthenticator(key, **profile),
                        f"the trusted profile {self.role}, assumed over"
                        f" {where}")
            return IAMAuthenticator(key), where
        token = str(config.get("IAMToken") or "").split(" ", 1)[-1]
        if token:
            if self.role:
                raise SystemExit(
                    "a trusted profile is assumed with an API key, and the"
                    " CLI's login is a token: set IBMCLOUD_API_KEY")
            return BearerTokenAuthenticator(token), "the CLI's login"
        auth = VPCInstanceAuthenticator(**profile)
        try:
            auth.token_manager.get_token()
        except Exception:  # noqa: BLE001 - not an instance of IBM's
            raise SystemExit(
                "no IBM Cloud credentials: looked in the environment"
                " (IBMCLOUD_API_KEY or IC_API_KEY), the CLI's login and the"
                " instance's identity") from None
        return auth, ("the instance's identity" + (
            f", as the trusted profile {self.role}" if self.role else ""))

    def _send(self, method, path, body=None, base=None, **values):
        """One request to the API: `path` with the deployment's CRN and
        `values` put in, each escaped."""
        from urllib.parse import quote

        from ibm_cloud_sdk_core import ApiException, BaseService
        auth = self.credentials()[0]
        if not base and not self.region:
            raise SystemExit("no IBM Cloud region: name it (region) or log"
                             " the CLI in to one")
        service = BaseService(service_url=base or ICD_URL.format(
            region=self.region), authenticator=auth)
        url = path.format(**{k: quote(str(v), safe="") for k, v in {
            "id": self.id, **values}.items()})
        req = service.prepare_request(method=method, url=url, data=body,
                                      headers={"Accept": "application/json"})
        try:
            return service.send(req).get_result() or {}
        except ApiException as e:
            raise SystemExit(f"IBM Cloud's API refused {method} {path} for"
                             f" {self.id}: {e.message} ({e.status_code})") \
                from None

    def _task(self, task, what, log):
        return _ready(lambda: (lambda t: (t.get("status", ""), t))(
            self._send("GET", "/tasks/{task}", task=task).get("task") or {}),
            what, log, ("completed",), ("failed",))

    # what the product is ---------------------------------------------------

    def _deployment(self):
        return self._send("GET", "/deployments/{id}").get("deployment") or {}

    def _connection(self, ident=None):
        """(host, port) of a deployment's endpoint of the kind the hop
        names (endpoint: private | public)."""
        got = self._send("GET", "/deployments/{id}/users/{kind}/{user}/"
                                "connections/{endpoint}",
                         id=ident or self.id, kind="database", user="admin",
                         endpoint=self.conf.get("endpoint") or "private")
        for detail in (got.get("connection") or {}).values():
            hosts = (detail or {}).get("hosts") if isinstance(
                detail, dict) else None
            if hosts:
                return hosts[0].get("hostname", ""), _num(
                    hosts[0].get("port"))
        return "", None

    def _schema(self):
        return self._send("GET", "/deployments/{id}/configuration/schema") \
            .get("schema") or {}

    def instance(self, names=()):
        d = self._deployment()
        got = Instance(self.provider, self.product, self.id, group="",
                       group_shared=False)
        got.engine = str(d.get("type", "")).replace("databases-for-", "")
        got.version, got.status = str(d.get("version", "")), str(
            d.get("state", ""))
        for g in self._send("GET", "/deployments/{id}/groups").get(
                "groups") or ():
            if g.get("id") != "member":
                continue
            members = int((g.get("members") or {}).get(
                "allocation_count") or g.get("count") or 1)
            got.cpu = _num((g.get("cpu") or {}).get("allocation_count")
                           and (g["cpu"]["allocation_count"] / members))
            got.memory_gb = _gb((g.get("memory") or {}).get(
                "allocation_mb"), 1024 * members)
            got.storage_gb = _gb((g.get("disk") or {}).get(
                "allocation_mb"), 1024 * members)
            got.klass = f"{members} members"
        got.host, got.port = self._connection()
        if names:
            schema = self._schema()
            got.parameters = {n: Param(None, n in schema, (
                schema.get(n) or {}).get("requires_restart")) for n in names}
        return got

    def find(self, host, port=None):
        """The deployment in the region whose endpoint is `host`: the
        Resource Controller lists the account's database deployments, and
        each is asked for its endpoint."""
        host, url = str(host), "/v2/resource_instances?type=service_instance"
        while url:
            page = self._send("GET", url, base=RC_URL)
            for r in page.get("resources") or ():
                crn = r.get("crn", "")
                if ":databases-for-" not in crn or r.get(
                        "region_id") != self.region:
                    continue
                if self._connection(crn)[0] == host:
                    return crn
            url = (page.get("next_url") or "").replace(RC_URL, "") or None
        return None

    # restore points --------------------------------------------------------

    @staticmethod
    def _point_of(b):
        return RestorePoint(b.get("id", ""), "backup", b.get("status", ""),
                            _stamp(b.get("created_at")),
                            b.get("status") == "completed"
                            and b.get("is_restorable", True) is not False)

    def _backups(self):
        return self._send("GET", "/deployments/{id}/backups").get(
            "backups") or []

    def restore_points(self):
        return sorted((self._point_of(b) for b in self._backups()),
                      key=lambda r: r.at)

    def _make(self, name, log):
        """An on-demand backup: its task waited for, then the newest
        on-demand backup made since the task began."""
        what = f"the backup of {self.id}"
        task = self._send("POST", "/deployments/{id}/backups").get(
            "task") or {}
        log(f"{what} asked for")
        self._task(task["id"], what, log)
        since = _stamp(task.get("created_at")) if task.get(
            "created_at") else ""
        got = [b for b in self._backups() if b.get("type") == "on_demand"
               and _stamp(b.get("created_at")) >= since]
        if not got:
            raise SystemExit(f"{what} was made, and the provider does not"
                             " list it")
        return self._point_of(max(got, key=lambda b: _stamp(
            b.get("created_at"))))

    # copies ----------------------------------------------------------------

    def _clone(self, name, point, log):
        d, what = self._deployment(), f"the copy {name}"
        if not self.conf.get("resource_group"):
            raise SystemExit("the IBM Cloud resource group is not named"
                             " (resource_group)")
        backup = point is not None and point.kind == "backup"
        made = self._send("POST", "/v2/resource_instances", base=RC_URL,
                          body={
            "name": name, "target": self.region,
            "resource_group": self.conf["resource_group"],
            "resource_plan_id": self.conf.get("spec")
            or f"{d.get('type')}-standard",
            "parameters": {"backup_id": point.id} if backup else {
                "point_in_time_recovery_deployment_id": self.id,
                "point_in_time_recovery_time": point.at if point
                else _stamp(_now())}})
        log(f"{what} of {self.id} asked for")
        got = _ready(lambda: (lambda r: (r.get("state", ""), r))(
            self._send("GET", "/v2/resource_instances/{guid}", base=RC_URL,
                       guid=made["guid"])), what, log, ("active",),
            ("failed", "removed"))
        host, port = self._connection(got.get("crn") or made.get("crn"))
        return {"host": host, "port": port, "id": got.get("crn")
                or made.get("crn"), "kind": "deployment"}

    # parameters ------------------------------------------------------------

    def _set(self, values, log):
        """The settings changed through the deployment's configuration, and
        its task waited for. The API does not say a setting's value before
        it changes, so each value before is None."""
        schema = self._schema()
        for n in values:
            if n not in schema:
                raise SystemExit(f"{n} is no setting IBM Cloud's API changes"
                                 f" on {self.id}")
        cast = {"integer": int, "number": float,
                "boolean": lambda v: str(v).lower() in ("true", "1", "on")}
        task = self._send("PATCH", "/deployments/{id}/configuration", body={
            "configuration": {n: cast.get((schema[n] or {}).get("type"),
                                          str)(v)
                              for n, v in values.items()}}).get("task") or {}
        restart = [n for n in values if (schema[n] or {}).get(
            "requires_restart")]
        log(f"{', '.join(values)} asked to change on {self.id}; the"
            " provider's API does not say their values before"
            + (f"; it restarts the deployment for {', '.join(restart)}"
               if restart else ""))
        self._task(task["id"], f"the parameters of {self.id}", log)
        return {n: None for n in values}


# --- the registry ------------------------------------------------------------

ADAPTERS = {"aws": Aws, "gcp": Gcp, "azure": Azure, "tencent": Tencent,
            "alibaba": Alibaba, "huawei": Huawei, "oracle": Oracle,
            "ibm": Ibm}


def adapter(provider, conf):
    """The provider's API for the instance `conf` names."""
    cls = ADAPTERS.get(str(provider or "").lower())
    if cls is None:
        raise SystemExit(f"migkit asks no API of {provider!r}; it asks"
                         f" {', '.join(sorted(ADAPTERS))}")
    return cls(conf)


def offered(provider, product, what):
    """Whether the provider's API offers `what` ("restore_point", "clone",
    "parameters") for the product - read, not asked."""
    cls = ADAPTERS.get(str(provider or "").lower())
    got = (cls.PRODUCTS.get(product) or {}) if cls else {}
    return bool(got.get(what))


# --- the account of what is used (backlog 0f) ---------------------------

#: the reason a restart is not asked for, shared by every product's
RESTART = "a restart is never asked here; a change that waits for one is said"
#: and a rollback's, which rewinds the instance itself
IN_PLACE = "it rolls the instance itself back; a copy is a new instance"

#: per SDK: (its licence, {command: the calls it offers for restore points,
#: copies, facts, parameters and credentials, as the code names them},
#: ("command call: why" for each not asked)). Written as text, so each use
#: is read off the code that makes the call, never off this table
SDKS = {
    "aws-sdk": ("Apache-2.0", {
        "rds": "describe_db_clusters, describe_db_instances,"
               " describe_db_parameters, describe_db_cluster_parameters,"
               " modify_db_parameter_group,"
               " modify_db_cluster_parameter_group, create_db_snapshot,"
               " describe_db_snapshots, create_db_cluster_snapshot,"
               " describe_db_cluster_snapshots,"
               " restore_db_instance_from_db_snapshot,"
               " restore_db_instance_to_point_in_time,"
               " restore_db_cluster_from_snapshot,"
               " restore_db_cluster_to_point_in_time, create_db_instance",
        "docdb": "describe_db_clusters, describe_db_instances,"
                 " describe_db_cluster_parameters,"
                 " modify_db_cluster_parameter_group,"
                 " create_db_cluster_snapshot, describe_db_cluster_snapshots,"
                 " restore_db_cluster_from_snapshot,"
                 " restore_db_cluster_to_point_in_time, create_db_instance",
        "elasticache": "describe_replication_groups, describe_cache_clusters,"
                       " describe_cache_parameters,"
                       " modify_cache_parameter_group, create_snapshot,"
                       " describe_snapshots, create_replication_group",
        "memorydb": "describe_clusters, describe_parameters,"
                    " update_parameter_group, create_snapshot,"
                    " describe_snapshots, create_cluster",
        "redshift": "describe_clusters, describe_cluster_parameters,"
                    " modify_cluster_parameter_group,"
                    " create_cluster_snapshot, describe_cluster_snapshots,"
                    " restore_from_cluster_snapshot",
        "dynamodb": "describe_table, create_backup, describe_backup,"
                    " list_backups, restore_table_from_backup,"
                    " restore_table_to_point_in_time",
        "opensearch": "describe_domain, list_domain_names, describe_domains",
        "msk": "describe_cluster_v2",
        "ec2": "describe_instance_types",
        "sts": "assume_role"}, (
        "rds copy_db_snapshot: a snapshot copied across regions or accounts"
        " is the seed's own step",
        "rds copy_db_cluster_snapshot: see copy_db_snapshot",
        "rds reboot_db_instance: " + RESTART,
        "rds modify_db_instance: an instance's class and storage are read"
        " here, never changed",
        "elasticache reboot_cache_cluster: " + RESTART,
        "redshift reboot_cluster: " + RESTART,
        "dynamodb export_table_to_point_in_time: a table's export is the"
        " move's own copy, not a restore point",
        "opensearch update_domain_config: a domain's settings are read"
        " here, never changed",
        "msk get_bootstrap_brokers: the brokers' addresses are the hop's"
        " own, not the provider's")),
    "google-api-client": ("Apache-2.0", {
        "cloudsql": "instances.get, instances.list, instances.clone,"
                    " instances.patch, backupRuns.insert, backupRuns.list,"
                    " operations.get, flags.list, tiers.list",
        "alloydb": "projects.locations.clusters.get,"
                   " projects.locations.clusters.list,"
                   " projects.locations.clusters.restore,"
                   " projects.locations.clusters.instances.list,"
                   " projects.locations.clusters.instances.create,"
                   " projects.locations.clusters.instances.patch,"
                   " projects.locations.backups.create,"
                   " projects.locations.backups.get,"
                   " projects.locations.backups.list,"
                   " projects.locations.operations.get,"
                   " projects.locations.supportedDatabaseFlags.list",
        "memorystore": "projects.locations.instances.get,"
                       " projects.locations.instances.list,"
                       " projects.locations.instances.patch,"
                       " projects.locations.operations.get",
        "bigquery": "datasets.get"}, (
        "cloudsql instances.restoreBackup: a backup is restored only into an"
        " instance that exists; a copy is a clone to a time",
        "cloudsql instances.restart: " + RESTART,
        "alloydb projects.locations.clusters.instances.restart: " + RESTART,
        "memorystore projects.locations.instances.export: an export is a"
        " file in a bucket, not a restore point")),
    "azure-sdk": ("MIT", {
        "flexible-postgres": "servers.get, servers.list_by_resource_group,"
                             " servers.begin_create_or_update,"
                             " configurations.get,"
                             " configurations.begin_update,"
                             " backups_automatic_and_on_demand.begin_create,"
                             " backups_automatic_and_on_demand"
                             ".list_by_server,"
                             " capabilities_by_location.list",
        "flexible-mysql": "servers.get, servers.list_by_resource_group,"
                          " servers.begin_create, configurations.get,"
                          " configurations.begin_update,"
                          " long_running_backup.begin_create,"
                          " backups.list_by_server,"
                          " location_based_capabilities.list",
        "sql": "servers.get, servers.list_by_resource_group, databases.get,"
               " databases.begin_create_or_update,"
               " restore_points.list_by_database",
        "cosmosdb": "database_accounts.get,"
                    " database_accounts.list_by_resource_group,"
                    " database_accounts.begin_create_or_update,"
                    " restorable_database_accounts.list_by_location",
        "redis": "redis.get, redis.list_by_resource_group, redis.begin_update",
        "identity": "DefaultAzureCredential"}, (
        "flexible-postgres servers.begin_restart: " + RESTART,
        "flexible-mysql servers.begin_restart: " + RESTART,
        "flexible-mysql backups.put: the long-running backup is asked and"
        " waited for instead",
        "sql restore_points.begin_create: a restore point on request is a"
        " data warehouse's only",
        "redis redis.begin_export_data: an export is files in a storage"
        " account, not a restore point")),
    "tencentcloud-sdk": ("Apache-2.0", {
        "cdb": "DescribeDBInstances, DescribeInstanceParams,"
               " DescribeDBInstanceCharset, DescribeTimeWindow, CreateBackup,"
               " DescribeBackups, CreateCloneInstance, DescribeCloneList,"
               " ModifyInstanceParam, DescribeAsyncRequestInfo",
        "postgres": "DescribeDBInstanceAttribute, DescribeDBInstances,"
                    " DescribeDBInstanceParameters,"
                    " ModifyDBInstanceParameters, CreateBaseBackup,"
                    " DescribeBaseBackups, CloneDBInstance,"
                    " DescribeMaintainTimeWindow, DescribeTasks",
        "mongodb": "DescribeDBInstances, DescribeInstanceParams,"
                   " ModifyInstanceParams, CreateBackupDBInstance,"
                   " DescribeAsyncRequestInfo, DescribeDBBackups,"
                   " DescribeSpecInfo, CreateDBInstanceHour",
        "redis": "DescribeInstances, DescribeInstanceParams,"
                 " ModifyInstanceParams, ManualBackupInstance,"
                 " DescribeTaskInfo, DescribeInstanceBackups, CloneInstances,"
                 " DescribeMaintenanceWindow",
        "cynosdb": "DescribeClusterDetail, DescribeClusters,"
                   " DescribeClusterParams, ModifyClusterParam, CreateBackup,"
                   " DescribeFlow, DescribeBackupList, RollbackToNewCluster",
        "sqlserver": "DescribeDBInstances, DescribeInstanceParams,"
                     " ModifyInstanceParam, CreateBackup,"
                     " DescribeBackupByFlowId, DescribeBackups",
        "mariadb": "DescribeDBInstances, DescribeDBParameters,"
                   " ModifyDBParameters, DescribeBackupFiles",
        "sts": "AssumeRole",
        "credentials": "Credential, CVMRoleCredential,"
                       " STSAssumeRoleCredential"}, (
        "cdb RestartDBInstances: " + RESTART,
        "mongodb RestoreDBInstance: " + IN_PLACE,
        "redis RestoreInstance: " + IN_PLACE,
        "cynosdb RollBackCluster: " + IN_PLACE,
        "sqlserver RestoreInstance: a backup is restored into an instance"
        " that exists, not copied",
        "sqlserver RollbackInstance: see RestoreInstance")),
    "alibabacloud-sdk": ("Apache-2.0", {
        "rds": "DescribeDBInstanceAttribute, DescribeDBInstances,"
               " DescribeDBInstanceNetInfo, DescribeParameters,"
               " DescribeParameterTemplates, ModifyParameter, CreateBackup,"
               " DescribeBackupTasks, DescribeBackups, CloneDBInstance",
        "dds": "DescribeDBInstanceAttribute, DescribeDBInstances,"
               " DescribeParameters, ModifyParameters, CreateBackup,"
               " DescribeBackups, CreateDBInstance",
        "polardb": "DescribeDBClusterAttribute, DescribeDBClusters,"
                   " DescribeDBClusterEndpoints, DescribeDBClusterParameters,"
                   " ModifyDBClusterParameters, CreateBackup, DescribeBackups,"
                   " CreateDBCluster",
        "kvstore": "DescribeInstanceAttribute, DescribeInstances,"
                   " DescribeParameters, ModifyInstanceConfig, CreateBackup,"
                   " DescribeBackupTasks, DescribeBackups, CreateInstance",
        "credentials": "StaticAKCredentialsProvider,"
                       " StaticSTSCredentialsProvider,"
                       " OIDCRoleArnCredentialsProvider,"
                       " CLIProfileCredentialsProvider,"
                       " ProfileCredentialsProvider,"
                       " EcsRamRoleCredentialsProvider,"
                       " URLCredentialsProvider,"
                       " RamRoleArnCredentialsProvider"}, (
        "rds RestartDBInstance: " + RESTART,
        "rds RecoveryDBInstance: a backup is restored into an instance that"
        " exists, not copied",
        "dds RestoreDBInstance: " + IN_PLACE,
        "polardb RestartDBNode: " + RESTART)),
    "huaweicloud-sdk": ("Apache-2.0", {
        "rds": "list_instances, show_instance_configuration,"
               " update_instance_configuration, create_manual_backup,"
               " list_backups, create_restore_instance, list_job_info",
        "dds": "list_instances, create_manual_backup, list_backups,"
               " restore_new_instance, show_job_detail",
        "iam": "create_temporary_access_key_by_agency"}, (
        "rds restore_to_existing_instance: a backup is restored into an"
        " instance that exists, not copied",
        "rds start_instance_restart_action: " + RESTART,
        "dds restore_instance: " + IN_PLACE,
        "dds update_entity_configuration: a node group's parameters are set"
        " through its templates, not here")),
    "oci-sdk": ("UPL-1.0 OR Apache-2.0", {
        "mysql": "get_db_system, list_db_systems, create_db_system,"
                 " update_db_system, create_backup, get_backup, list_backups,"
                 " get_configuration, create_configuration, list_shapes",
        "autonomous": "get_autonomous_database, list_autonomous_databases,"
                      " create_autonomous_database,"
                      " create_autonomous_database_backup,"
                      " get_autonomous_database_backup,"
                      " list_autonomous_database_backups",
        "credentials": "from_file, InstancePrincipalsSecurityTokenSigner"}, (
        "mysql restart_db_system: " + RESTART,
        "autonomous restore_autonomous_database: " + IN_PLACE,
        "autonomous restart_autonomous_database: " + RESTART)),
    "ibm-cloud-sdk-core": ("Apache-2.0", {
        "icd": "GET /deployments/{id}, GET /deployments/{id}/groups,"
               " GET /deployments/{id}/users/{kind}/{user}/connections/"
               "{endpoint}, GET /deployments/{id}/configuration/schema,"
               " PATCH /deployments/{id}/configuration,"
               " POST /deployments/{id}/backups,"
               " GET /deployments/{id}/backups, GET /tasks/{task}",
        "resource-controller": "GET /v2/resource_instances?type="
                               "service_instance,"
                               " POST /v2/resource_instances,"
                               " GET /v2/resource_instances/{guid}",
        "credentials": "IAMAuthenticator, IAMAssumeAuthenticator,"
                       " BearerTokenAuthenticator, VPCInstanceAuthenticator"},
        ("icd POST /deployments/{id}/restores: no such call copies a"
         " deployment; the Resource Controller makes the copy",)),
}


def _register():
    from . import tooluse
    for name, (licence, calls, unused) in SDKS.items():
        declared = {c: {o.strip() for o in s.split(",") if o.strip()}
                    for c, s in calls.items()}
        why = {}
        for line in unused:
            head, _, reason = line.partition(": ")
            command, _, option = head.partition(" ")
            declared.setdefault(command, set()).add(option)
            why[(command, option)] = (tooluse.UNUSED, reason)
        tooluse.register(
            name, licence=licence,
            declared={c: frozenset(o) for c, o in declared.items()},
            reads=(["cloudapi.py"], {(c, o): o.rsplit(" ", 1)[-1]
                                     for c, opts in declared.items()
                                     for o in opts}),
            options=why)


_register()
