"""What only a cloud's control plane can do, driven through its API
(backlog R19 lever 1; the owner's "structural four", 2026-09-28): the
target made from the source's own storage - an Aurora copy-on-write clone,
an RDS or Aurora snapshot copied across accounts and regions and restored,
an Azure Flexible Server restored to a point in time, a Cloud SQL clone -
and carried on from the exact position that copy holds, as a physical copy
is (`physical`).

A terabyte arrives in the minutes a clone takes, and nothing is read
through a connection at all. What the provider does not give is imitated
by `physical`'s own copy of the pages, where the source is a server
migkit may reach that way.

The position the changes go on from is the one the copy itself says:

* Aurora PostgreSQL: `aurora_volume_logical_start_lsn()` on the clone or
  the restored cluster - the LSN its storage volume starts from.
* RDS PostgreSQL, Azure, Cloud SQL PostgreSQL: the restored server's
  recovery end, `pg_last_wal_replay_lsn()`, read before anything restarts
  it (`physical.replay_end`).
* MySQL with GTIDs on: the restored server's `gtid_executed`, turned into
  the position in the source's binlog where that set ends
  (`position_of`), and proved against the source's executed set
  (`physical.position_proof`).
* Cloud SQL MySQL: the clone is asked for exactly the binlog coordinates
  migkit read from the source just before, so the position is known
  before the clone exists; proved as above where GTIDs are on.
* Aurora or RDS MySQL with GTIDs off: the restored server's own error log,
  "Binlog position from crash recovery is <file> <pos>" (Aurora) or
  InnoDB's "Last MySQL binlog file position 0 <pos>, file name <file>".
  Refused where the source runs Aurora's enhanced binlog, whose restored
  coordinates were wrong until Aurora MySQL 3.12 / 8.4.8.

**What is proved here and what only on a real account.** The control-plane
calls (snapshot, share, copy, restore, clone, the cluster's writer, the
waits, the endpoint) are driven against a mock of the AWS API in the
tests, and the Azure and Google calls against stand-ins shaped like their
clients; the continuation is the same code `physical` proves on two real
servers. Not provable here: that a real restored RDS, Azure or Cloud SQL
instance still answers `pg_last_wal_replay_lsn()` when first reached (a
provider that restarts it after recovery would lose it, and the seed is
then refused, not guessed), `aurora_volume_logical_start_lsn()`'s value,
the IAM, KMS, RAM and network permissions of an account, and the time a
provider's copy takes.

**The decision rule, per provider** (the `decide.Rung`s below, climbed by
`physical.choose`): taken where the hop names the provider (`physical:
{cloud: aws}`), no other provider runs the source (its address and its
server, as `clouds` reads them), the provider's API sees the source, the
target does not exist yet (or is the one this hop's earlier run made),
the hop carries the whole server, and - to go on from the copy - the
source keeps a change log the tail can read from the copy's position; not
where this hop measured it slower than a logical copy. On AWS the clone is
tried first (copy-on-write, minutes whatever the size), and the snapshot
where the target is in another region (a clone stays in its region) or
the source is an RDS instance (only Aurora clones). A restored RDS
instance fills its storage from S3 as it is first read: the first reads
of every table are slow, which is the provider's and not the copy's.

The option:

    physical:
      cloud: aws                      # aws | azure | google
      source_id: app-db               # the source's instance or cluster
      target_id: app-db-seeded        # the one to make; must not exist
      region: eu-west-1
      instance_class: db.r6g.large    # a cluster's writer, or the restore
      target_region: eu-central-1     # AWS: copy the snapshot there
      target_role_arn: arn:aws:iam::<account>:role/migkit
      kms_key: arn:aws:kms:eu-central-1:<account>:key/CHANGE_ME
      way: snapshot                   # AWS: clone | snapshot, else chosen
      subscription: <subscription-id>                     # azure
      resource_group: rg-app                               # azure
      project: my-project                                  # google
"""
import os
import time

from . import physical
from .physical import GIVES, Need, Rung, Unread

AWS_CLONE = "a copy-on-write clone of the source's storage"
AWS_SNAPSHOT = "a snapshot of the source restored as the target"
AZURE_RESTORE = "a point-in-time restore of the source as the target"
GOOGLE_CLONE = "a clone of the source instance"

#: seconds between two looks at something the provider is making, and the
#: most a make is waited for
POLL, WAIT = 20, 6 * 3600


def _opts(hop):
    return physical.settings(hop) or {}


def _until(ask, what, log, poll=None, most=None):
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


# --- AWS ---------------------------------------------------------------------

def _account_of(arn):
    parts = str(arn or "").split(":")
    return parts[4] if len(parts) > 4 else ""


def _aws_error(e):
    code = getattr(e, "response", {}).get("Error", {}).get("Code", "")
    return code or type(e).__name__


class Aws:
    """RDS and Aurora through their API: the source's account and region,
    and the target's (another account through `target_role_arn`, another
    region through `target_region`)."""

    def __init__(self, hop, opts):
        self.hop, self.opts = hop, opts
        self.region = opts.get("region")
        self.target_region = opts.get("target_region") or self.region
        self.role = opts.get("target_role_arn")

    def client(self, where="source", service="rds"):
        from .config import _aws_client
        if where == "source":
            return _aws_client(service, self.region)
        return _aws_client(service, self.target_region, self.role)

    def source(self):
        """{"kind": "cluster" | "instance", "engine", "arn", "encrypted",
        ...} of the source, as the API describes it."""
        rds, sid = self.client(), self.opts["source_id"]
        try:
            c = rds.describe_db_clusters(DBClusterIdentifier=sid)[
                "DBClusters"][0]
            return {"kind": "cluster", "engine": c["Engine"],
                    "arn": c["DBClusterArn"],
                    "encrypted": bool(c.get("StorageEncrypted")),
                    "kms": c.get("KmsKeyId", ""),
                    "version": c.get("EngineVersion", "")}
        except Exception as e:  # noqa: BLE001 - an instance, then
            if _aws_error(e) not in ("DBClusterNotFoundFault",
                                     "DBClusterNotFound"):
                raise
        i = rds.describe_db_instances(DBInstanceIdentifier=sid)[
            "DBInstances"][0]
        return {"kind": "instance", "engine": i["Engine"],
                "arn": i["DBInstanceArn"],
                "encrypted": bool(i.get("StorageEncrypted")),
                "kms": i.get("KmsKeyId", ""),
                "version": i.get("EngineVersion", "")}

    def target_exists(self):
        rds, tid = self.client("target"), self.opts["target_id"]
        for ask, fault in ((lambda: rds.describe_db_clusters(
                DBClusterIdentifier=tid), "DBClusterNotFound"),
                           (lambda: rds.describe_db_instances(
                               DBInstanceIdentifier=tid),
                            "DBInstanceNotFound")):
            try:
                ask()
                return True
            except Exception as e:  # noqa: BLE001 - classified below
                if not _aws_error(e).startswith(fault):
                    raise
        return False

    def crosses_account(self):
        return bool(self.role)

    def crosses_region(self):
        return self.target_region != self.region

    # the waits ---------------------------------------------------------------

    def _status(self, describe, key, field, ident, want="available"):
        def ask():
            got = describe(**{field: ident})[key][0]
            status = got.get("Status") or got.get("DBClusterStatus") or \
                got.get("DBInstanceStatus")
            if status in ("failed", "incompatible-restore",
                          "incompatible-parameters", "deleting"):
                raise SystemExit(f"{ident} is {status}")
            return got if status == want else None
        return ask

    def _wait_cluster(self, rds, ident, log):
        return _until(self._status(rds.describe_db_clusters, "DBClusters",
                                   "DBClusterIdentifier", ident),
                      f"the cluster {ident}", log)

    def _wait_instance(self, rds, ident, log):
        return _until(self._status(rds.describe_db_instances, "DBInstances",
                                   "DBInstanceIdentifier", ident),
                      f"the instance {ident}", log)

    def _writer(self, rds, cluster, engine, log):
        """A cluster restored or cloned has no instance: its writer is made,
        and the cluster's endpoint answers once it is available."""
        writer = f"{self.opts['target_id']}-1"
        rds.create_db_instance(
            DBInstanceIdentifier=writer, DBClusterIdentifier=cluster,
            Engine=engine, DBInstanceClass=self.opts["instance_class"])
        self._wait_instance(rds, writer, log)
        got = self._wait_cluster(rds, cluster, log)
        return {"host": got["Endpoint"], "port": int(got["Port"]),
                "id": cluster, "writer": writer, "kind": "cluster"}

    def _net(self):
        out = {}
        if self.opts.get("subnet_group"):
            out["DBSubnetGroupName"] = self.opts["subnet_group"]
        if self.opts.get("security_groups"):
            out["VpcSecurityGroupIds"] = list(self.opts["security_groups"])
        return out

    # the two ways ------------------------------------------------------------

    def clone(self, src, log):
        """An Aurora clone: copy-on-write on the source's volume, in the
        source's region; in another account through a RAM share the
        target account accepts."""
        rds = self.client("target")
        source = src["arn"] if self.crosses_account() else \
            self.opts["source_id"]
        if self.crosses_account():
            self._share_cluster(src["arn"], log)
        rds.restore_db_cluster_to_point_in_time(
            DBClusterIdentifier=self.opts["target_id"],
            SourceDBClusterIdentifier=source, RestoreType="copy-on-write",
            UseLatestRestorableTime=True, **self._net())
        log(f"clone {self.opts['target_id']} of {self.opts['source_id']}"
            " asked for")
        self._wait_cluster(rds, self.opts["target_id"], log)
        return self._writer(rds, self.opts["target_id"], src["engine"], log)

    def _share_cluster(self, arn, log):
        ram = self.client("source", "ram")
        account = _account_of(self.role)
        share = ram.create_resource_share(
            name=f"migkit-{self.opts['target_id']}", resourceArns=[arn],
            principals=[account], allowExternalPrincipals=True)[
            "resourceShare"]["resourceShareArn"]
        theirs = self.client("target", "ram")
        for inv in theirs.get_resource_share_invitations(
                resourceShareArns=[share]).get(
                "resourceShareInvitations", []):
            if inv.get("status") == "PENDING":
                theirs.accept_resource_share_invitation(
                    resourceShareInvitationArn=inv[
                        "resourceShareInvitationArn"])
        log(f"the source cluster shared with account {account}")

    def snapshot(self, src, log):
        """A snapshot of the source, shared and copied into the target's
        account and region where they differ, then restored."""
        rds, tid = self.client(), self.opts["target_id"]
        name = f"migkit-{tid}-{time.strftime('%Y%m%d%H%M%S')}"
        cluster = src["kind"] == "cluster"
        if cluster:
            rds.create_db_cluster_snapshot(
                DBClusterSnapshotIdentifier=name,
                DBClusterIdentifier=self.opts["source_id"])
            got = _until(self._status(
                rds.describe_db_cluster_snapshots, "DBClusterSnapshots",
                "DBClusterSnapshotIdentifier", name), f"the snapshot {name}",
                log)
            arn = got["DBClusterSnapshotArn"]
        else:
            rds.create_db_snapshot(DBSnapshotIdentifier=name,
                                   DBInstanceIdentifier=self.opts["source_id"])
            got = _until(self._status(
                rds.describe_db_snapshots, "DBSnapshots",
                "DBSnapshotIdentifier", name), f"the snapshot {name}", log)
            arn = got["DBSnapshotArn"]
        log(f"snapshot {name} of {self.opts['source_id']} taken")
        tgt = self.client("target")
        snap = name
        if self.crosses_account():
            share = (rds.modify_db_cluster_snapshot_attribute if cluster
                     else rds.modify_db_snapshot_attribute)
            share(**{("DBClusterSnapshotIdentifier" if cluster
                      else "DBSnapshotIdentifier"): name},
                  AttributeName="restore",
                  ValuesToAdd=[_account_of(self.role)])
            log(f"the snapshot shared with account {_account_of(self.role)}")
        if self.crosses_account() or self.crosses_region():
            snap = self._copy(tgt, arn, name, cluster, log)
        if cluster:
            tgt.restore_db_cluster_from_snapshot(
                DBClusterIdentifier=tid, SnapshotIdentifier=snap,
                Engine=src["engine"], **self._net())
            self._wait_cluster(tgt, tid, log)
            return self._writer(tgt, tid, src["engine"], log)
        extra = ({"DBInstanceClass": self.opts["instance_class"]}
                 if self.opts.get("instance_class") else {})
        tgt.restore_db_instance_from_db_snapshot(
            DBInstanceIdentifier=tid, DBSnapshotIdentifier=snap,
            **extra, **self._net())
        got = self._wait_instance(tgt, tid, log)
        return {"host": got["Endpoint"]["Address"],
                "port": int(got["Endpoint"]["Port"]), "id": tid,
                "kind": "instance"}

    def _copy(self, tgt, arn, name, cluster, log):
        """The shared snapshot copied into the target's account and region:
        an encrypted one under the hop's key, which the source's account
        shared with the target's (the default RDS key cannot be)."""
        extra = {}
        if self.opts.get("kms_key"):
            extra["KmsKeyId"] = self.opts["kms_key"]
        if self.crosses_region():
            extra["SourceRegion"] = self.region
        if cluster:
            tgt.copy_db_cluster_snapshot(
                SourceDBClusterSnapshotIdentifier=arn,
                TargetDBClusterSnapshotIdentifier=name, **extra)
            _until(self._status(tgt.describe_db_cluster_snapshots,
                                "DBClusterSnapshots",
                                "DBClusterSnapshotIdentifier", name),
                   f"the copy of {name}", log)
        else:
            tgt.copy_db_snapshot(SourceDBSnapshotIdentifier=arn,
                                 TargetDBSnapshotIdentifier=name, **extra)
            _until(self._status(tgt.describe_db_snapshots, "DBSnapshots",
                                "DBSnapshotIdentifier", name),
                   f"the copy of {name}", log)
        log(f"the snapshot copied to {self.target_region}"
            + (f", account {_account_of(self.role)}" if self.role else ""))
        return name

    def copy(self, way, src, log):
        return self.clone(src, log) if way == "clone" else \
            self.snapshot(src, log)

    def binlog_position(self, target, log):
        """The source's binlog position the restored MySQL server holds,
        from its own error log: Aurora's "Binlog position from crash
        recovery is <file> <pos>", or InnoDB's "Last MySQL binlog file
        position 0 <pos>, file name <file>"."""
        import re
        rds = self.client("target")
        ident = target.get("writer") or target["id"]
        files = rds.describe_db_log_files(DBInstanceIdentifier=ident).get(
            "DescribeDBLogFiles", [])
        for f in sorted(files, key=lambda f: f.get("LastWritten", 0)):
            name = f["LogFileName"]
            if "error" not in name:
                continue
            text = rds.download_db_log_file_portion(
                DBInstanceIdentifier=ident, LogFileName=name, Marker="0",
                NumberOfLines=10000).get("LogFileData", "")
            got = re.findall(r"Binlog position from crash recovery is"
                             r" (\S+) (\d+)", text)
            if got:
                return {"log_file": got[-1][0].rsplit("/", 1)[-1],
                        "log_pos": int(got[-1][1])}
            got = re.findall(r"Last MySQL binlog file position 0 (\d+),"
                             r" file name (\S+)", text)
            if got:
                return {"log_file": got[-1][1].rsplit("/", 1)[-1],
                        "log_pos": int(got[-1][0])}
        return None


# --- Azure -------------------------------------------------------------------

class Azure:
    """A Flexible Server (PostgreSQL or MySQL) restored to a point in time
    as a new server, through the management API."""

    def __init__(self, hop, opts, engine):
        self.hop, self.opts, self.engine = hop, opts, engine

    def client(self):
        from azure.identity import DefaultAzureCredential
        if self.engine == "postgres":
            from azure.mgmt.postgresqlflexibleservers import \
                PostgreSQLManagementClient as Client
        else:
            from azure.mgmt.mysqlflexibleservers import \
                MySQLManagementClient as Client
        return Client(DefaultAzureCredential(), self.opts["subscription"])

    def _server(self, **fields):
        """The request body, as the engine's own client models it."""
        if self.engine == "postgres":
            from azure.mgmt.postgresqlflexibleservers.models import Server
        else:
            from azure.mgmt.mysqlflexibleservers.models import Server
        return Server(**fields)

    def source(self):
        got = self.client().servers.get(self.opts["resource_group"],
                                        self.opts["source_id"])
        return {"kind": "server", "id": got.id, "location": got.location,
                "version": str(getattr(got, "version", "") or "")}

    def target_exists(self):
        try:
            self.client().servers.get(self.opts["resource_group"],
                                      self.opts["target_id"])
            return True
        except Exception as e:  # noqa: BLE001 - classified below
            if type(e).__name__ in ("ResourceNotFoundError",) or \
                    getattr(e, "status_code", None) == 404:
                return False
            raise

    def copy(self, way, src, log):
        """Restored to now: the slots were made before this call, so the
        point restored to is after them."""
        import datetime
        now = datetime.datetime.now(datetime.timezone.utc)
        fields = {"location": self.opts.get("location") or src["location"],
                  "create_mode": "PointInTimeRestore",
                  "source_server_resource_id": src["id"]}
        fields["point_in_time_utc" if self.engine == "postgres"
               else "restore_point_in_time"] = now
        servers = self.client().servers
        # the PostgreSQL client names it create-or-update, MySQL's create
        make = getattr(servers, "begin_create_or_update", None) or \
            servers.begin_create
        poller = make(self.opts["resource_group"], self.opts["target_id"],
                      self._server(**fields))
        log(f"restore of {self.opts['source_id']} as"
            f" {self.opts['target_id']} asked for")
        got = poller.result()
        return {"host": got.fully_qualified_domain_name,
                "port": 5432 if self.engine == "postgres" else 3306,
                "id": self.opts["target_id"], "kind": "server"}

    def binlog_position(self, target, log):
        return None


# --- Google ------------------------------------------------------------------

class Google:
    """A Cloud SQL instance cloned, through the Cloud SQL Admin API."""

    def __init__(self, hop, opts, engine):
        self.hop, self.opts, self.engine = hop, opts, engine

    def client(self):
        from googleapiclient import discovery
        return discovery.build("sqladmin", "v1beta4", cache_discovery=False)

    def _get(self, name):
        return self.client().instances().get(
            project=self.opts["project"], instance=name).execute()

    def source(self):
        got = self._get(self.opts["source_id"])
        return {"kind": "instance", "version": got.get("databaseVersion", ""),
                "engine": got.get("databaseVersion", "")}

    def target_exists(self):
        try:
            self._get(self.opts["target_id"])
            return True
        except Exception as e:  # noqa: BLE001 - classified below
            status = getattr(getattr(e, "resp", None), "status", None)
            if str(status) == "404":
                return False
            raise

    def copy(self, way, src, log, at=None):
        """The clone; `at` - binlog coordinates read from the source just
        before - makes it hold exactly the transactions up to there."""
        body = {"cloneContext": {"kind": "sql#cloneContext",
                                 "destinationInstanceName":
                                     self.opts["target_id"]}}
        if at:
            body["cloneContext"]["binLogCoordinates"] = {
                "kind": "sql#binLogCoordinates",
                "binLogFileName": at["log_file"],
                "binLogPosition": str(at["log_pos"])}
        api = self.client()
        op = api.instances().clone(project=self.opts["project"],
                                   instance=self.opts["source_id"],
                                   body=body).execute()
        log(f"clone {self.opts['target_id']} of {self.opts['source_id']}"
            " asked for")

        def done():
            got = api.operations().get(project=self.opts["project"],
                                       operation=op["name"]).execute()
            if got.get("status") != "DONE":
                return None
            if got.get("error"):
                raise SystemExit("the clone failed: " + "; ".join(
                    e.get("message", "") for e in got["error"].get(
                        "errors", [])))
            return got
        _until(done, f"the clone {self.opts['target_id']}", log)
        inst = self._get(self.opts["target_id"])
        want = str(self.opts.get("address") or "PRIVATE").upper()
        ips = inst.get("ipAddresses") or []
        pick = [a["ipAddress"] for a in ips if a.get("type") == want] or \
            [a["ipAddress"] for a in ips]
        return {"host": pick[0] if pick else "",
                "port": 5432 if self.engine == "postgres" else 3306,
                "id": self.opts["target_id"], "kind": "instance"}

    def binlog_position(self, target, log):
        return None


PROVIDERS = {"aws": Aws, "azure": Azure, "google": Google}


def provider(hop, engine=None):
    opts = _opts(hop)
    cls = PROVIDERS[str(opts.get("cloud")).lower()]
    if cls is Aws:
        return cls(hop, opts)
    return cls(hop, opts, engine or physical._engine_of(hop))


# --- facts and needs ---------------------------------------------------------

def _facts_class(engine):
    return physical.PgFacts if engine == "postgres" else physical.MyFacts


class _CloudFacts:
    """What a cloud rung reads: the provider's view of the source and the
    target, besides the source's own settings."""

    def _cloud(self):
        return provider(self.hop)

    def _source(self):
        return self["cloud"].source()

    def _target_exists(self):
        return self["cloud"].target_exists()

    def _source_cloud(self):
        """The cloud the source's address and its own answers name
        (`clouds.of`), "" where none does."""
        from . import clouds
        return clouds.of(self.eng, "src", self.hop.source).cloud

    def _is_ours(self, rec):
        """The target this hop's earlier run made, which it goes on from."""
        return str(rec.get("rung", "")).startswith("cloud-") and (
            rec.get("target") or {}).get("id") == self["options"].get(
            "target_id")

    def _sdk(self):
        name = str(self["options"].get("cloud")).lower()
        mods = {"aws": ("boto3",),
                "azure": ("azure.identity",
                          "azure.mgmt.postgresqlflexibleservers"
                          if physical._engine_of(self.hop) == "postgres"
                          else "azure.mgmt.mysqlflexibleservers"),
                "google": ("googleapiclient.discovery",)}.get(name, ())
        import importlib
        for m in mods:
            importlib.import_module(m)
        return True

    def _enhanced_binlog(self):
        try:
            got = self.eng._q("src", "select @@aurora_enhanced_binlog")
        except Exception:  # noqa: BLE001 - not Aurora 3, or not on
            return False
        return str(got[0][0]) in ("1", "ON")


def facts_for(hop, eng, mode):
    base = _facts_class(physical._engine_of(hop))
    cls = type("CloudFacts", (_CloudFacts, base), {})
    return cls(hop, eng, mode)


def _names(provider_name):
    return Need(f"the hop to ask for {provider_name}'s copy", ("options",),
                lambda f: str(f["options"].get("cloud", "")).lower()
                == provider_name,
                f"the hop does not ask for {provider_name}'s copy")


def _says(keys):
    return Need("the hop to name the source and the target", ("options",),
                lambda f: all(f["options"].get(k) for k in keys),
                lambda f: ("the hop's physical option needs "
                           + ", ".join(k for k in keys
                                       if not f["options"].get(k))))


_SDK = Need("the provider's client on this machine", ("sdk",),
            lambda f: f["sdk"] is True)

_NEW_TARGET = Need("a target that does not exist yet", ("target_exists",),
                   # or the one this hop's earlier run made
                   lambda f: not f["target_exists"] or f["our_seed"] is True,
                   lambda f: (f"{f['options'].get('target_id')} exists"
                              " already; the copy makes the target, so it"
                              " names one that is not there"))

_SOURCE_SEEN = Need("a source the credentials can see", ("source",),
                    lambda f: bool(f["source"]))

#: the hop's word for a provider, as `clouds` names it
CLOUD_OF = {"aws": "aws", "azure": "azure", "google": "gcp"}


def _theirs(provider_name):
    """The source is not another cloud's: where its address or the server
    names a provider, it is the one the hop asks to copy it. A server
    nothing names (a private address, a proxy) is left to the provider's
    own answer (`_SOURCE_SEEN`)."""
    want = CLOUD_OF[provider_name]
    return Need(f"a source {provider_name} runs", ("source_cloud",),
                lambda f: f["source_cloud"] in ("", want),
                lambda f: (f"the hop asks {provider_name} to copy the"
                           " source, and its address and its server name"
                           f" {f['source_cloud']} as the one that runs it"))


def engine_of(said):
    """"postgres", "mysql" or "" for what a provider calls an engine
    (`aurora-postgresql`, `mysql`, `POSTGRES_16`, `MYSQL_8_0`...)."""
    said = str(said or "").lower()
    if "postgres" in said:
        return "postgres"
    if "mysql" in said:
        return "mysql"
    return ""


def _engine_matches(engine):
    return Need("a source of the hop's engine", ("source",),
                lambda f: engine_of(f["source"].get("engine")) in (
                    "", engine),
                lambda f: (f"the source is {f['source'].get('engine')}, and"
                           f" the hop moves {engine}"))


def _follows(engine, provider_name):
    """What the continuation needs of the source: a change log that goes
    on from the copy's position."""
    following = physical._following
    if engine == "postgres":
        return (Need("logical decoding on the source to go on from the copy",
                     ("wal_level",), lambda f: f["wal_level"] == "logical",
                     lambda f: (f"wal_level is {f['wal_level']} on the"
                                " source (rds.logical_replication,"
                                " azure.replication_support or"
                                " cloudsql.logical_decoding turns it on),"
                                " and the changes after the copy are read"
                                " through a logical slot"), when=following),)
    out = [Need("the binlog on the source to go on from the copy",
                ("log_bin",), lambda f: f["log_bin"],
                "the binlog is off on the source, and the changes after the"
                " copy are read from it", when=following)]
    if provider_name == "azure":
        out.append(Need("GTIDs on the source", ("gtid_mode",),
                        lambda f: f["gtid_mode"] == "ON",
                        "a restored Azure server says where it ends only"
                        " by its GTID set, and GTIDs are off on the source",
                        when=following))
    if provider_name == "aws":
        out.append(Need("a restored position the source's log agrees with",
                        ("gtid_mode", "enhanced_binlog"),
                        lambda f: f["gtid_mode"] == "ON"
                        or not f["enhanced_binlog"],
                        "the source runs Aurora's enhanced binlog with GTIDs"
                        " off, and the binlog position a restored copy of it"
                        " reports was wrong until Aurora MySQL 3.12 / 8.4.8;"
                        " with GTIDs on, the copy's GTID set places it"
                        " exactly", when=following))
    return tuple(out)


def _aws_way(way):
    def holds(f):
        src = f["source"]
        opts = f["options"]
        forced = str(opts.get("way") or "").lower()
        if forced and forced != way:
            return False
        if way == "clone":
            return src["kind"] == "cluster" and not (
                opts.get("target_region")
                and opts.get("target_region") != opts.get("region"))
        return True

    def why(f):
        forced = str(f["options"].get("way") or "").lower()
        if forced and forced != way:
            return f"the hop asks for the {forced} way"
        if f["source"]["kind"] != "cluster":
            return "only an Aurora cluster is cloned; the source is an" \
                   " instance"
        return "a clone stays in its source's region, and the target is in" \
               f" {f['options'].get('target_region')}"
    return Need(f"a source the {way} way can copy", ("source", "options"),
                holds, why)


def _aws_writer_class():
    return Need("the class of the target's writer", ("source", "options"),
                lambda f: f["source"]["kind"] != "cluster"
                or bool(f["options"].get("instance_class")),
                "a cluster is restored without an instance, and the hop's"
                " physical option names the class of the one migkit makes"
                " (instance_class)")


def _encrypted_across():
    return Need("a key the target's account may use", ("source", "options"),
                lambda f: not (f["source"].get("encrypted")
                               and f["options"].get("target_role_arn")
                               and not f["options"].get("kms_key")),
                "the source is encrypted and the target is in another"
                " account: a snapshot under the default RDS key cannot be"
                " shared, so the hop names a customer-managed key shared"
                " with that account (kms_key)")


def rungs():
    out = []
    for engine in ("postgres", "mysql"):
        base = (physical._WHOLE, _SDK, _SOURCE_SEEN)
        aws = (_names("aws"), _says(("source_id", "target_id")),
               _theirs("aws"), *base, _engine_matches(engine))
        out += [
            Rung("cloud-aws-clone", AWS_CLONE, gives=GIVES,
                 needs=(*aws, _aws_way("clone"), _aws_writer_class(),
                        _NEW_TARGET, *_follows(engine, "aws")),
                 footprint=1, engine=engine, cloud="aws",
                 because="a copy-on-write clone takes minutes whatever the"
                         " size"),
            Rung("cloud-aws-snapshot", AWS_SNAPSHOT, gives=GIVES,
                 needs=(*aws, _aws_way("snapshot"), _aws_writer_class(),
                        _encrypted_across(), _NEW_TARGET,
                        *_follows(engine, "aws")),
                 footprint=2, engine=engine, cloud="aws",
                 because="a snapshot copies storage, not rows"),
            Rung("cloud-azure-restore", AZURE_RESTORE, gives=GIVES,
                 needs=(_names("azure"),
                        _says(("subscription", "resource_group", "source_id",
                               "target_id")), _theirs("azure"),
                        *base, _NEW_TARGET, *_follows(engine, "azure")),
                 footprint=1, engine=engine, cloud="azure",
                 because="a restore copies storage, not rows"),
            Rung("cloud-google-clone", GOOGLE_CLONE, gives=GIVES,
                 needs=(_names("google"),
                        _says(("project", "source_id", "target_id")),
                        _theirs("google"), *base, _NEW_TARGET,
                        *_follows(engine, "google")),
                 footprint=1, engine=engine, cloud="google",
                 because="a clone copies storage, not rows"),
        ]
    return tuple(out)


# --- the seed position -------------------------------------------------------

def aurora_start(eng):
    """The LSN an Aurora PostgreSQL clone's or restored cluster's storage
    starts from - where its changes from the slot made before go on."""
    got = eng._psql("dst", "postgres",
                    "select aurora_volume_logical_start_lsn()").strip()
    if not got:
        raise SystemExit("the Aurora copy did not say the LSN its storage"
                         " starts from")
    return {"lsn": got}


def _previous_gtids(eng, name):
    for row in eng._q("src", f"show binlog events in '{name}' limit 3"):
        if str(row[2]) == "Previous_gtids":
            return physical.gtid_set(row[5])
    return {}


def position_of(eng, have, scan=None):
    """Where in the source's binlog the GTID set `have` ends: the start of
    the first transaction it does not hold, in the newest file whose
    previous set it holds whole. The position the tail reads from."""
    files = [str(r[0]) for r in eng._q("src", "show binary logs")]
    start = None
    for name in reversed(files):
        if not physical.gtid_minus(_previous_gtids(eng, name), have):
            start = name
            break
    if start is None:
        raise SystemExit(
            "the copy holds less than the oldest binlog the source still"
            f" keeps ({files[0] if files else 'none'}): the changes between"
            " were purged, and nothing can carry them")
    return (scan or _first_missing)(eng, start, have)


def _first_missing(eng, name, have):
    from pymysqlreplication import BinLogStreamReader
    from pymysqlreplication.event import GtidEvent
    ep = eng.hop.source
    stream = BinLogStreamReader(
        connection_settings={"host": ep.host, "port": ep.port,
                             "user": ep.user, "passwd": ep.password},
        server_id=int((eng.hop.options or {}).get("server_id", 4379)) + 1,
        blocking=False, resume_stream=True, log_file=name, log_pos=4,
        only_events=[GtidEvent])
    try:
        for ev in stream:
            if physical.gtid_minus(physical.gtid_add({}, ev.gtid), have):
                # the header's next position less the whole event's size:
                # where this transaction's GTID event starts
                return {"log_file": stream.log_file,
                        "log_pos": int(ev.packet.log_pos) - int(
                            ev.packet.event_size)}
    finally:
        stream.close()
    return eng.change_point("src", "")


# --- the move ----------------------------------------------------------------

def carry(hop, eng, rung, db, mode, go, log):
    opts = _opts(hop)
    engine = rung.engine
    dbs = eng.databases()
    follow = mode == "full+cdc"
    cloud = provider(hop, engine)
    rec = physical.seeded(hop)
    if not (rec and rec.get("rung") == rung.name
            and rec.get("target", {}).get("id") == opts["target_id"]):
        way = rung.name.rsplit("-", 1)[-1]
        first = (f"a slot on {', '.join(dbs)} first, then "
                 if follow and engine == "postgres" else "")
        log(f"plan: {first}{rung.said}: {opts['source_id']} to"
            f" {opts['target_id']} ({way}), then the changes after it from"
            " the position it holds")
        if not go:
            log("dry-run: nothing asked of the provider")
            return
        facts = facts_for(hop, eng, mode)
        began = time.time()
        slots = (physical._pg_slots_before(hop, eng, dbs, log)
                 if follow and engine == "postgres" else {})
        at = None
        if engine == "mysql" and opts["cloud"] == "google":
            at = eng.change_point("src", "")
        try:
            src = facts["source"]
            if isinstance(src, Unread):
                raise SystemExit(src.why)
            target = (cloud.copy(way, src, log, at=at) if at
                      else cloud.copy(way, src, log))
        except BaseException:
            physical._pg_drop_slots(eng, slots)
            raise
        rec = {"rung": rung.name, "at": time.strftime("%Y-%m-%d %H:%M:%S"),
               "seconds": round(time.time() - began, 1), "slots": slots,
               "target": target, "asked_at": at,
               "aurora": "aurora" in str(src.get("engine", "")),
               "system": facts["src_system"] if engine == "postgres"
               and not isinstance(facts["src_system"], Unread) else None,
               "gtid": (facts["gtid_mode"] == "ON") if engine == "mysql"
               else None}
        physical._save_seed(hop, rec)
        from . import planner
        planner.record_rate(hop, rung.name, physical._rows(eng, dbs),
                            rec["seconds"])
        log(f"{opts['target_id']} is up at {target['host']}:{target['port']}"
            f" after {rec['seconds']:,.0f}s")
    target = rec["target"]
    if (str(hop.target.host), int(hop.target.port or 0)) != (
            str(target["host"]), int(target["port"])):
        raise SystemExit(
            f"the copy is {opts['target_id']} at {target['host']}:"
            f"{target['port']}, and the hop's target is {hop.target.host}:"
            f"{hop.target.port}. Point the hop's target there and move"
            " again: it goes on from the copy, which is kept")
    if engine == "postgres":
        if not rec.get("lsn"):
            point = (aurora_start(eng) if rec.get("aurora") else
                     physical.replay_end(eng, None, rec.get("system")))
            rec.update(point)
            physical._save_seed(hop, rec)
            log(f"the copy goes on from {rec['lsn']}")
        if follow:
            physical.pg_continue(hop, eng, dbs, rec, go, log)
        return
    if not rec.get("log_file"):
        token = _mysql_point(hop, eng, rec, cloud, log)
        rec.update(token)
        physical._save_seed(hop, rec)
    token = {"log_file": rec["log_file"], "log_pos": rec["log_pos"]}
    physical._record_positions(hop, dbs, token, token)


def _mysql_point(hop, eng, rec, cloud, log):
    """The source's binlog position the copy holds, proved where GTIDs
    let it be."""
    if rec.get("gtid"):
        have = str(eng._q("dst", "select @@global.gtid_executed")[0][0])
        token = rec.get("asked_at") or position_of(
            eng, physical.gtid_set(have))
        why = physical.position_proof(eng, token, have)
        if why:
            raise SystemExit(f"the copy's position is not exact: {why}")
        log(f"the copy holds the source up to {token['log_file']}:"
            f"{token['log_pos']}, proved against the source's executed set")
        return token
    if rec.get("asked_at"):
        return rec["asked_at"]
    token = cloud.binlog_position(rec["target"], log)
    if not token:
        raise SystemExit(
            "the restored server does not say which binlog position it holds,"
            " and GTIDs are off on the source: the position is never guessed."
            " Turn GTIDs on, or copy with the logical copy")
    log(f"the copy holds the source up to {token['log_file']}:"
        f"{token['log_pos']}, as its own log says")
    return token
