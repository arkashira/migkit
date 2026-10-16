"""The cloud side of readiness (backlog R25.2): what a managed side's
provider says of its network, subnets, security groups, pending
maintenance and restore points, audited before the move, each gap a
blocker with the next step.

Stand-ins: AWS through the SDK's own models on an in-memory account (VPCs,
subnets, security groups, peering, RDS instances and snapshots), with the
pending-maintenance answer added in its documented shape
(`PendingMaintenanceActions[].PendingMaintenanceActionDetails[]`) where the
stand-in has none; Google's `instances.get` and Azure's server and
firewall rules as recorded answers. No real account is asked.
"""
import dataclasses
import datetime
from types import SimpleNamespace

import pytest

from migkit import cloud_readiness as cr
from migkit import cloudapi

moto = pytest.importorskip("moto")

NOW = datetime.datetime(2026, 10, 16, 3, 0, tzinfo=datetime.timezone.utc)


@pytest.fixture
def aws(monkeypatch):
    for k, v in (("AWS_ACCESS_KEY_ID", "testing"),
                 ("AWS_SECRET_ACCESS_KEY", "CHANGE_ME"),
                 ("AWS_DEFAULT_REGION", "us-east-1")):
        monkeypatch.setenv(k, v)
    with moto.mock_aws():
        import boto3
        yield boto3.client("ec2"), boto3.client("rds")


def _instance(ec2, rds, name, cidr, port_from="10.0.0.0/24", snapshot=True):
    vpc = ec2.create_vpc(CidrBlock=cidr)["Vpc"]["VpcId"]
    sub = ec2.create_subnet(VpcId=vpc, CidrBlock=cidr.split("/")[0] + "/28",
                            AvailabilityZone="us-east-1a")["Subnet"]
    sg = ec2.create_security_group(GroupName=name, Description="db",
                                   VpcId=vpc)["GroupId"]
    ec2.authorize_security_group_ingress(GroupId=sg, IpPermissions=[{
        "IpProtocol": "tcp", "FromPort": 5432, "ToPort": 5432,
        "IpRanges": [{"CidrIp": port_from}]}])
    rds.create_db_subnet_group(DBSubnetGroupName=name,
                               DBSubnetGroupDescription="db",
                               SubnetIds=[sub["SubnetId"]])
    rds.create_db_instance(DBInstanceIdentifier=name, Engine="postgres",
                           DBInstanceClass="db.t3.micro", MasterUsername="u",
                           MasterUserPassword="CHANGE_ME1", Port=5432,
                           AllocatedStorage=20, DBSubnetGroupName=name,
                           VpcSecurityGroupIds=[sg])
    if snapshot:
        rds.create_db_snapshot(DBInstanceIdentifier=name,
                               DBSnapshotIdentifier=name + "-before")
    return vpc


def _pending(monkeypatch, details):
    real = cloudapi.Aws.client

    class Rds:
        def __init__(self, c):
            self.c = c

        def __getattr__(self, k):
            return getattr(self.c, k)

        def describe_pending_maintenance_actions(self, ResourceIdentifier):
            return {"PendingMaintenanceActions": [{
                "ResourceIdentifier": ResourceIdentifier,
                "PendingMaintenanceActionDetails": details}]}

    def client(self, service=None):
        got = real(self, service)
        return Rds(got) if (service or "rds") == "rds" else got
    monkeypatch.setattr(cloudapi.Aws, "client", client)


def _net(name):
    return cloudapi.network(cloudapi.adapter("aws", {
        "product": "rds", "id": name, "region": "us-east-1"}))


def _fails(gaps):
    return {what: (detail, fix) for level, what, detail, fix in gaps
            if level == "fail"}


def test_a_clean_pair_passes(aws, monkeypatch):
    _pending(monkeypatch, [])
    _instance(*aws, "src", "10.0.0.0/25")
    _instance(*aws, "dst", "10.0.0.128/25")
    src, dst = _net("src"), _net("dst")
    assert src.cidrs == ("10.0.0.0/25",) and src.port == 5432
    assert src.subnets[0][1] > 0 and src.rules[0][1:] == ("10.0.0.0/24",
                                                         5432, 5432)
    assert any(p.ready for p in src.restore_points)
    ec2 = aws[0]
    pcx = ec2.create_vpc_peering_connection(VpcId=src.network,
                                            PeerVpcId=dst.network)
    ec2.accept_vpc_peering_connection(VpcPeeringConnectionId=pcx[
        "VpcPeeringConnection"]["VpcPeeringConnectionId"])
    src = _net("src")
    assert dst.network in src.peers
    got = cr.audit(src, dst, {"source": ("10.0.0.4",),
                              "target": ("10.0.0.4",)}, NOW)
    assert [g[0] for g in got] == ["pass", "pass"], got


def test_overlapping_networks_are_a_blocker(aws, monkeypatch):
    _pending(monkeypatch, [])
    _instance(*aws, "src", "10.0.0.0/25")
    _instance(*aws, "dst", "10.0.0.0/25")
    got = _fails(cr.audit(_net("src"), _net("dst"),
                          {"source": ("10.0.0.4",), "target": ("10.0.0.4",)},
                          NOW))
    assert "overlap" in got["the two networks"][0]
    assert "range overlaps none" in got["the two networks"][1]


def test_a_full_subnet_a_closed_group_and_no_restore_point(aws, monkeypatch):
    _pending(monkeypatch, [])
    _instance(*aws, "src", "10.0.0.0/25", port_from="10.0.0.0/25",
              snapshot=False)
    # the stand-in keeps an automated snapshot of every instance: none kept
    net = dataclasses.replace(_net("src"), subnets=(("subnet-full", 0),),
                              restore_points=())
    got = _fails(cr.audit(net, None, {"source": ("192.0.2.10",)}, NOW))
    assert "0 free" in got["the source's subnets"][0]
    assert "add a subnet" in got["the source's subnets"][1]
    assert "nothing admits port 5432 from 192.0.2.10" in \
        got["the source's security groups"][0]
    assert "inbound rule for port 5432 from 192.0.2.10/32" in \
        got["the source's security groups"][1]
    assert "take a restore point" in got["the source's restore point"][1]


def test_pending_maintenance_inside_the_move_is_a_blocker(aws, monkeypatch):
    _pending(monkeypatch, [{"Action": "system-update",
                            "CurrentApplyDate": NOW + datetime.timedelta(
                                hours=1)}])
    _instance(*aws, "src", "10.0.0.0/25")
    net, here = _net("src"), {"source": ("10.0.0.4",)}
    inside = _fails(cr.audit(net, None, here, NOW,
                             until=NOW + datetime.timedelta(hours=2)))
    assert "system-update" in inside["the source's pending maintenance"][0]
    assert "defer it" in inside["the source's pending maintenance"][1]
    after = _fails(cr.audit(net, None, here, NOW,
                            until=NOW + datetime.timedelta(minutes=30)))
    assert "the source's pending maintenance" not in after
    # the move's length not yet measured: it counts
    assert "the source's pending maintenance" in _fails(
        cr.audit(net, None, here, NOW))


def test_google_and_azure_answers_are_audited():
    gcp = SimpleNamespace(provider="gcp", id="pg1", _project=lambda: "p",
                          restore_points=lambda: [],
                          _call=lambda path, **kw: {
                              "settings": {
                                  "ipConfiguration": {
                                      "ipv4Enabled": True,
                                      "authorizedNetworks": [
                                          {"value": "203.0.113.0/24"}]},
                                  "maintenanceWindow": {"day": 7,
                                                        "hour": 2}},
                              "scheduledMaintenance": {
                                  "startTime": "2026-10-16T04:00:00Z"}})
    net = cloudapi.network(gcp)
    net.port = 5432
    got = _fails(cr.audit(net, None, {"source": ("198.51.100.7",)}, NOW))
    assert "2026-10-16T04:00:00" in got["the source's pending maintenance"][0]
    assert "198.51.100.7" in got["the source's security groups"][0]
    assert "no backup" in got["the source's restore point"][0]
    az = SimpleNamespace(
        provider="azure", id="fs1", _rg=lambda: "rg",
        restore_points=lambda: [],
        _server=lambda: SimpleNamespace(
            network=SimpleNamespace(delegated_subnet_resource_id=None),
            maintenance_window=SimpleNamespace(custom_window="Disabled")),
        _op=lambda what, *a: [SimpleNamespace(start_ip_address="10.0.0.0",
                                              end_ip_address="10.0.0.255")])
    net = cloudapi.network(az)
    net.port = 5432
    got = cr.audit(net, None, {"source": ("10.0.0.4",)}, NOW)
    assert not _fails(got) and any("not read" in g[2] for g in got)


def test_assess_says_the_cloud_side(monkeypatch):
    from migkit import cutover, egress, machine, managed, ready
    net = cloudapi.Network("aws", "src", network="vpc-1",
                           cidrs=("10.0.0.0/25",), subnets=(("s", 0),),
                           rules=(), port=5432, pending=(),
                           restore_points=(), restore_kind="snapshot")
    monkeypatch.setattr(managed, "api_conf", lambda h, e, w: (
        ("aws", {}) if w == "src" else (None, "owner's")))
    monkeypatch.setattr(managed, "adapter", lambda h, e, w: object())
    monkeypatch.setattr(cloudapi, "network", lambda a: net)
    monkeypatch.setattr(cr, "_here", lambda h, p: ("10.0.0.4",))
    for name in ("_servers", "_managed", "_target", "_resources", "_case",
                 "_sap_licence", "_goal_checks", "_rehearsed", "by_hand"):
        monkeypatch.setattr(ready, name, lambda *a, **k: [])
    monkeypatch.setattr(ready, "_dbs", lambda h, e, d: [])
    monkeypatch.setattr(cutover, "summary", lambda *a: ("pass", "ok"))
    monkeypatch.delenv("MIGKIT_EGRESS", raising=False)
    hop = SimpleNamespace(name="h", options={},
                          source=SimpleNamespace(host="10.0.0.5", port=5432),
                          target=SimpleNamespace(host="10.0.0.6", port=5432))
    got = {i.what: i for i in ready.items(hop, None, assessed=[])}
    assert got["the source's subnets"].level == "fail"
    assert got["the source's security groups"].fix.startswith(
        "add an inbound rule")
    monkeypatch.setenv("MIGKIT_EGRESS", "none")
    egress._SAID.clear()
    shut = {i.what: i for i in ready.items(hop, None, assessed=[])}
    assert "not read" in shut["the cloud side"].detail


def test_each_provider_cell_is_declared_built_and_unproved():
    for who in ("aws", "gcp", "azure"):
        for part in ("sign-in", "forwarder", "readiness"):
            assert cr.UNPROVED[f"managed.{part}.{who}"].startswith(
                "built; not run against a real")
