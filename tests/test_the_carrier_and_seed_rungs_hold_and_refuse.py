"""The rungs a seed's carrier, its far-end load, an object's copy and its
proof, a SQL Server backup seed and a provider's own copy are chosen by,
each reached under `watched()` on both its branches on fakes (backlog
AUD-Z.rungs-outside-owed): a carrier is reached only the way the hop
gives what it needs, a load reads only where the sealed seed is, a copy
is held to the strongest proof its source gives, and a provider's copy is
refused in the words of the first thing the hop or the source lacks."""
import pytest

from migkit import backupseed, carriers, cloudseed, decide
from tests.rung_branches import HOLDS, REFUSES, branch, watched, why
from tests.test_the_case_between_two_servers_is_held_first import (
    _backup_facts, _hop)

#: read by `test_every_decide_rung_is_reached` as it is written
PROVES_RUNGS = [
    ("carriers", "rungs", "disk", "holds"),
    ("carriers", "rungs", "disk", "refuses"),
    ("carriers", "rungs", "snowball", "holds"),
    ("carriers", "rungs", "snowball", "refuses"),
    ("carriers", "rungs", "databox share", "holds"),
    ("carriers", "rungs", "databox share", "refuses"),
    ("carriers", "rungs", "databox blob", "holds"),
    ("carriers", "rungs", "databox blob", "refuses"),
    ("carriers", "rungs", "transfer appliance", "holds"),
    ("carriers", "rungs", "transfer appliance", "refuses"),
    ("carriers", "for_export", "<r.name>", "holds"),
    ("carriers", "for_export", "<r.name>", "refuses"),
    ("carriers", "_load_rungs", "object store", "holds"),
    ("carriers", "_load_rungs", "object store", "refuses"),
    ("carriers", "_load_rungs", "attached", "holds"),
    ("carriers", "_load_rungs", "attached", "refuses"),
    ("carriers", "SOURCE_PROOFS", "source checksum", "holds"),
    ("carriers", "SOURCE_PROOFS", "source checksum", "refuses"),
    ("carriers", "SOURCE_PROOFS", "source digest", "holds"),
    ("carriers", "SOURCE_PROOFS", "source digest", "refuses"),
    ("carriers", "SOURCE_PROOFS", "parts reported", "holds"),
    ("carriers", "FILE_WAYS", "server side", "holds"),
    ("carriers", "FILE_WAYS", "server side", "refuses"),
    ("carriers", "FILE_WAYS", "ranged parts", "holds"),
    ("carriers", "FILE_WAYS", "ranged parts", "refuses"),
    ("carriers", "FILE_WAYS", "one stream", "holds"),
    ("backupseed", "rungs", "<RUNG_NAME>", "holds"),
    ("backupseed", "rungs", "<RUNG_NAME>", "refuses"),
    ("cloudseed", "rungs", "cloud-aws-clone", "holds"),
    ("cloudseed", "rungs", "cloud-aws-clone", "refuses"),
    ("cloudseed", "rungs", "cloud-aws-snapshot", "holds"),
    ("cloudseed", "rungs", "cloud-aws-snapshot", "refuses"),
    ("cloudseed", "rungs", "cloud-azure-restore", "holds"),
    ("cloudseed", "rungs", "cloud-azure-restore", "refuses"),
    ("cloudseed", "rungs", "cloud-google-clone", "holds"),
    ("cloudseed", "rungs", "cloud-google-clone", "refuses"),
    ("cloudseed", "rungs", "<f'cloud-{name}-clone'>", "holds"),
    ("cloudseed", "rungs", "<f'cloud-{name}-clone'>", "refuses"),
]

SHARE = "/mnt/appliance_BlockBlob/seeds"

#: (rung, the hop's ship: facts it holds on, the change refusing it, the
#: reason)
CARRIERS = [
    ("disk", {"kind": "disk", "path": "/mnt/disk"}, {"path": None},
     "the hop gives no path"),
    ("snowball", {"kind": "snowball", "endpoint": "https://10.0.0.5:8443",
                  "bucket": "seed"}, {"bucket": None},
     "the hop gives no endpoint and bucket"),
    ("databox share", {"kind": "databox", "path": SHARE},
     {"path": "/mnt/appliance_PageBlob/seeds"},
     "the page blob share takes only files"),
    ("databox blob", {"kind": "databox", "endpoint": "https://10.0.0.6",
                      "container": "seed"}, {"container": None},
     "the hop gives no endpoint and container"),
    ("transfer appliance", {"kind": "transfer_appliance",
                            "path": "/mnt/ta"}, {"kind": "disk"},
     "the hop names another carrier"),
]


def _ship(**given):
    base = {"kind": "", "path": None, "endpoint": None, "bucket": None,
            "container": None}
    base.update(given)
    return base


@pytest.mark.parametrize("name,ok,change,said", CARRIERS)
def test_a_carrier_is_reached_only_the_way_the_hop_gives_it(name, ok,
                                                            change, said):
    with watched() as seen:
        kept, _ = decide.prune(carriers.rungs(), _ship(**ok))
        assert name in [r.name for r in kept]
        kept, _ = decide.prune(carriers.rungs(), _ship(**dict(ok, **change)))
    assert name not in [r.name for r in kept]
    assert branch(seen, name) == [HOLDS, REFUSES]
    assert said in why(seen, name), why(seen, name)


def test_the_carrier_the_hop_names_is_climbed_and_said(tmp_path):
    with watched() as seen:
        got = carriers.for_export({"carrier": "disk", "path": str(tmp_path)},
                                  prove=False)
        with pytest.raises(SystemExit) as stop:
            carriers.for_export({"carrier": "disk"}, prove=False)
    assert got.rung.name == "disk" and got.kind == "disk"
    assert branch(seen, "disk") == [HOLDS, REFUSES]
    assert "the hop gives no path" in str(stop.value)
    assert why(seen, "disk") in str(stop.value)


def _load(**given):
    sealed = given.pop("sealed", {"load_from", "attached"})
    base = {"load_from": "s3://seed/app", "attached": "/mnt/disk",
            "sealed": lambda where: "seed.json" if where in sealed else None,
            "said": lambda where: where}
    base.update(given)
    return base


@pytest.mark.parametrize("name,change,said", [
    ("object store", {"load_from": None}, "the hop gives no load_from"),
    ("object store", {"sealed": {"attached"}}, "no sealed seed is at"
                                                " load_from yet"),
    ("attached", {"attached": None}, "an endpoint this machine does not"
                                     " have"),
    ("attached", {"sealed": {"load_from"}}, "no sealed seed is at attached"),
])
def test_the_far_end_reads_the_seed_only_where_it_is_sealed(name, change,
                                                            said):
    with watched() as seen:
        kept, _ = decide.prune(carriers._load_rungs(), _load())
        assert [r.name for r in kept] == ["object store", "attached"]
        decide.prune(carriers._load_rungs(), _load(**change))
    assert branch(seen, name) == [HOLDS, REFUSES]
    assert said in why(seen, name), why(seen, name)


def test_a_copy_is_held_to_the_strongest_proof_its_source_gives():
    small = carriers.S3_COPY_ONE
    with watched() as seen:
        kept, _ = decide.prune(carriers.SOURCE_PROOFS, {
            "checksum": "crc64nvme:abc", "plain etag": True, "size": small})
        assert [r.name for r in kept] == ["source checksum", "source digest",
                                          "parts reported"]
        kept, _ = decide.prune(carriers.SOURCE_PROOFS, {
            "checksum": "", "plain etag": True, "size": small + 1})
        assert [r.name for r in kept] == ["parts reported"]
    assert branch(seen, "source checksum") == [HOLDS, REFUSES]
    assert "a whole-object checksum on the source" in why(seen,
                                                          "source checksum")
    assert branch(seen, "source digest") == [HOLDS, REFUSES]
    assert "an object a single copy can carry" in why(seen, "source digest")
    assert branch(seen, "parts reported") == [HOLDS]


def test_a_file_crosses_this_machine_only_where_the_store_cannot_copy_it():
    with watched() as seen:
        kept, _ = decide.prune(carriers.FILE_WAYS, {
            "same store": True, "ranged": True, "parts": True})
        assert [r.name for r in kept] == ["server side", "ranged parts",
                                          "one stream"]
        kept, _ = decide.prune(carriers.FILE_WAYS, {
            "same store": False, "ranged": True, "parts": False})
    assert [r.name for r in kept] == ["one stream"]
    assert branch(seen, "server side") == [HOLDS, REFUSES]
    assert "one object store on both sides" in why(seen, "server side")
    assert branch(seen, "ranged parts") == [HOLDS, REFUSES]
    assert "a store taking parts" in why(seen, "ranged parts")
    assert branch(seen, "one stream") == [HOLDS]


def test_sql_servers_own_backup_is_held_and_refused_by_its_own_needs():
    rung = backupseed.rungs()[0]
    assert rung.name == backupseed.RUNG_NAME and rung.engine == "mssql"
    hop = _hop("mssql", options={"physical": {
        "backup": "/var/opt/mssql/backup"}})
    with watched() as seen:
        assert rung.refused(_backup_facts(hop)) is None
        got = rung.refused(_backup_facts(
            hop, dst_databases={"master", "app"}))
    assert branch(seen, backupseed.RUNG_NAME) == [HOLDS, REFUSES]
    assert "already holds app" in got
    assert why(seen, backupseed.RUNG_NAME) == got


class _Facts(dict):
    def __init__(self, hop, mode="full", **kw):
        super().__init__(kw)
        self.hop, self.mode = hop, mode


class _CloudHop:
    goal = ""


def _cloud(provider, **options):
    opts = {"cloud": provider, "source_id": "app-db", "target_id": "app-new",
            "instance_class": "db.r6g.large", "subscription": "CHANGE_ME",
            "resource_group": "rg", "project": "p", "region": "r1"}
    opts.update(options)
    return _Facts(_CloudHop(), options=opts, covers=True, sdk=True,
                  source={"kind": "cluster", "engine": "aurora-postgresql"},
                  source_cloud="", target_exists=False, our_seed=False)


CLOUDS = [
    ("cloud-aws-clone", "aws", {"target_region": "r2"},
     "a clone stays in its source's region"),
    ("cloud-aws-snapshot", "aws", {"way": "clone"},
     "the hop asks for the clone way"),
    ("cloud-azure-restore", "azure", {"resource_group": ""},
     "the hop's physical option needs resource_group"),
    ("cloud-google-clone", "google", {"project": ""},
     "the hop's physical option needs project"),
    ("cloud-tencent-clone", "tencent", {"region": ""},
     "the hop's physical option needs region"),
    ("cloud-alibaba-clone", "alibaba", {"cloud": "aws"},
     "the hop does not ask for alibaba's copy"),
]


@pytest.mark.parametrize("name,cloud,change,said", CLOUDS)
def test_a_providers_copy_holds_where_the_hop_and_source_allow_it(
        name, cloud, change, said):
    rung = next(r for r in cloudseed.rungs()
                if r.name == name and r.engine == "postgres")
    with watched() as seen:
        assert rung.refused(_cloud(cloud)) is None
        got = rung.refused(_cloud(cloud, **change))
    assert branch(seen, name) == [HOLDS, REFUSES]
    assert got and said in got and why(seen, name) == got


def test_a_target_that_exists_refuses_every_providers_copy():
    for rung in (r for r in cloudseed.rungs() if r.engine == "postgres"):
        facts = _cloud(rung.cloud)
        facts["target_exists"] = True
        with watched() as seen:
            got = rung.refused(facts)
        assert branch(seen, rung.name) == [REFUSES]
        assert "app-new exists already" in got, (rung.name, got)
