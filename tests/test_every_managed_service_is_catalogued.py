"""Every managed database service, any provider, either end (the owner,
2026-10-01: "any <-> any from the start"): the catalogue
(`migkit/data/managed.yaml`) held to its schema and its sources, and every
pair of its services held to a plan - the pair named, the engine it runs
through (the family's where both are one, the pair engine where they are
not), what following the source needs in its provider's words, what the
target's admin can and cannot do, the way the source's writes are held
(the freeze ladder over what its admin gives, with stand-ins for the
servers), and the restore points each side offers. The engines migkit
does not have are listed, not planned.
"""
import re

import pytest

from migkit import clouds, decide, freeze, managed
from migkit.engines import NAMES

#: what this module proves, item by engine (`capabilities.proofs`)
PROVES = [
    ("managed.catalogued", ("postgres", "mysql", "mongodb", "mssql", "redis",
                            "kafka", "clickhouse", "dynamodb", "oracle",
                            "db2", "opensearch", "cassandra", "redshift",
                            "bigquery", "kinesis", "pubsub", "cockroachdb",
                            "dsql", "cosmosdb", "teradata")),
]

#: every provider the owner named, and the vendors'
PROVIDERS = ("aws", "gcp", "azure", "tencent", "alibaba", "huawei", "oracle",
             "ibm")
#: the services that are engines migkit lacks, by a word of their name
DISTINCT = ("spanner", "bigtable", "firestore", "gaussdb", "oceanbase",
            "keyspaces", "neptune", "timestream", "lindorm", "geminidb")
#: the admin's facts each family's ladder reads (`managed.stand_in`)
ADMIN = {"postgres": ("superuser", "role", "alter_system", "createrole",
                      "signal_backend", "owns_databases", "subscribe",
                      "replication"),
         "mysql": ("superuser", "role", "super", "system_variables_admin",
                   "connection_admin", "kill", "grant_option",
                   "read_only_by_api", "binlog_retention"),
         "mongodb": ("superuser", "role", "version", "commands_missing",
                     "user_roles", "change_streams"),
         "redis": ("role", "version", "acl"),
         "mssql": ("role", "alter", "control", "kill", "availability_group"),
         "kafka": ("role", "authorizer")}


def _all():
    return list(managed.services().values())


def test_the_catalogue_names_its_sources():
    text = managed.FILE.read_text()
    head = text.split("\nservices:")[0]
    assert "Sources" in head, head[:400]
    urls = re.findall(r"https?://\S+", head)
    assert len(urls) >= 20, urls
    assert "@@" not in text, "a placeholder was left in the catalogue"
    assert managed.catalogue().get("version") == 1


def test_every_provider_the_owner_named_is_there_with_its_services():
    have = managed.providers()
    for p in PROVIDERS:
        assert p in have and have[p].get("label"), p
        assert any(s.provider == p for s in _all()), p
        assert have[p].get("api") == p, (p, have[p])
    vendors = [p for p, v in have.items() if v.get("api") == "none"]
    assert len(vendors) >= 10, vendors
    for p in vendors:
        assert any(s.provider == p for s in _all()), p


def test_every_entry_keeps_the_schema():
    seen = set()
    for s in _all():
        assert s.provider in managed.providers(), s.id
        assert s.label and s.where, s.id
        assert s.family in NAMES or s.family == "none", (s.id, s.family)
        assert bool(s.distinct) == (s.family == "none"), s.id
        assert (s.where, s.family) not in seen or s.distinct, s.id
        seen.add((s.where, s.family))
        for p in s.hosts:
            re.compile(p)
        for template in (s.change.get("words"), s.change.get("cli"),
                         s.admin.get("replication"),
                         s.admin.get("binlog_retention"),
                         s.admin.get("kill")):
            for name in re.findall(r"\{([a-z_]+)\}", str(template or "")):
                assert name in managed.PLACEHOLDERS, (s.id, template)
        if s.distinct:
            continue
        for key in ADMIN.get(s.family, ("role",)):
            assert key in s.admin, (s.id, key)
        restore = (s.api or {}).get("restore_point")
        assert restore in ("snapshot", "backup", "point in time", ""), \
            (s.id, restore)


def test_every_product_is_one_the_providers_api_layer_drives():
    from migkit import cloudapi
    for s in _all():
        api = managed.providers()[s.provider].get("api")
        if not s.product or api in (None, "none"):
            continue
        assert s.product in cloudapi.ADAPTERS[api].PRODUCTS, (s.id,
                                                              s.product)
        how = (s.api or {}).get("restore_point")
        if not how:
            continue
        # the plan says who takes it: the provider's API through migkit
        # where the layer drives it, its console where it does not
        said = managed._restore_words(s)
        if cloudapi.offered(api, s.product, "restore_point"):
            assert "API" in said, (s.id, said)
        else:
            assert s.console in said and "API" not in said, (s.id, said)


def test_the_engines_migkit_lacks_are_listed_not_planned():
    lacking = " ".join(f"{s.id} {s.label}" for s in managed.distinct()
                       ).lower()
    for word in DISTINCT:
        assert word in lacking, word
    planned = {k for pair in managed.matrix() for k in pair}
    assert not planned & {s.id for s in managed.distinct()}


def test_the_clouds_own_signs_name_their_service():
    class Pg:
        CANON_ENGINE = "postgres"

        def __init__(self, role):
            self.role = role

        def _psql(self, side, db, sql):
            assert f"'{self.role}'" in sql, sql
            return f"{self.role}\x1ffalse"
    from migkit.config import Endpoint
    ep = Endpoint(host="10.0.0.9")
    for role, (cloud, where) in managed.signs("pg_roles").items():
        got = clouds.of(Pg(role), "src", ep)
        assert got.cloud == clouds.PG_SIGNS.get(role, (cloud,))[0], role


def test_every_pair_of_one_engine_gets_its_plan():
    plans = managed.matrix()
    usable = [s for s in _all() if not s.distinct]
    assert len(plans) == len(usable) ** 2
    same = 0
    for (a, b), p in plans.items():
        src, dst = managed.get(a), managed.get(b)
        assert src.label in p.pair and dst.label in p.pair, p.pair
        assert p.source and p.target, (a, b)
        assert p.freeze, (a, b)
        assert p.restore["source"] and p.restore["target"], (a, b)
        if src.family == dst.family:
            same += 1
            assert p.route == src.family, (a, b, p.route)
        else:
            assert p.route == "hetero", (a, b, p.route)
    assert same > len(usable), same


def test_every_source_is_held_the_way_its_admin_allows():
    for s in (s for s in _all() if not s.distinct):
        way, why = managed.freeze_way(s)
        if s.family not in freeze.HOLDS:
            assert way is None and why, s.id
            assert "operator" in managed.plan(s, s).freeze, s.id
            continue
        if way is None:
            # the ladder says why, in the words of what the admin lacks
            assert why, s.id
            assert "operator" in managed.plan(s, s).freeze, s.id
            continue
        assert way in freeze.SCOPE, (s.id, way)
        # the way stands on what the admin gives: a server whose admin
        # gives none of it is refused that way
        rungs = freeze.HOLDS[s.family](None, managed._Stub(s.family)).rungs()
        chosen = next(r for r in rungs if r.name == way)
        nothing = decide.Facts({k: _nothing(k, v) for k, v in
                                managed.stand_in(s).items()})
        got = decide.climb("t", rungs, nothing, prove=False)
        if chosen.needs or chosen.changes:
            assert got.rung is None or got.rung.name != way, (s.id, way)


def _nothing(key, value):
    """A fact as a server whose admin may do nothing answers it."""
    from migkit.decide import IMPOSSIBLE, Stand
    if key in ("others", "roles elsewhere"):
        return ["another"]
    if key == "owner roles":
        return True
    if key == "kill":
        return None
    if key in ("commands", "actions"):
        return []
    if key == "version":
        return (0, 0)
    if key == "cache":
        return None
    if key.startswith("change "):
        return Stand(IMPOSSIBLE, "nothing may be changed")
    if isinstance(value, bool):
        return False
    return value


def _known(provider, where, family):
    got = managed.find(provider, where, family)
    assert got is not None, (provider, where, family)
    return got


def test_the_pairs_the_real_runs_take_are_planned_as_their_admins_allow():
    tencent = _known("tencent", "tencentdb", "postgres")
    for src in (_known("aws", "rds", "postgres"),
                _known("aws", "aurora", "postgres")):
        p = managed.plan(src, tencent)
        assert p.route == "postgres"
        assert "rds.logical_replication" in " ".join(p.source), p.source
        # the master user owns what it makes and is no superuser: its
        # databases' right to connect is its to take
        assert p.freeze_way == "pg_database", (src.id, p.freeze)
        assert "snapshot" in p.restore["source"]
    my = managed.plan(_known("aws", "rds", "mysql"),
                      _known("tencent", "tencentdb", "mysql"))
    assert my.route == "mysql" and my.freeze_way == "my_database"
    assert "param_read_only" in my.offer, my.offer
    docs = managed.plan(_known("aws", "documentdb", "mongodb"),
                        _known("tencent", "tencentdb", "mongodb"))
    assert docs.route == "mongodb"
    assert docs.freeze_way == "mongo_database", docs.freeze
    # and back the other way, and across engines
    back = managed.plan(tencent, _known("aws", "rds", "postgres"))
    assert back.route == "postgres" and back.freeze
    across = managed.plan(_known("aws", "rds", "mysql"), tencent)
    assert across.route == "hetero"
    # the pair engine is the one a hop of two engines runs through
    from migkit.engines import ALIASES
    assert "hetero" in NAMES and ALIASES.get("hetero", "hetero") == "hetero"


@pytest.mark.parametrize("provider,where,family", [
    ("aws", "rds", "postgres"), ("aws", "aurora", "mysql"),
    ("gcp", "cloud-sql", "postgres"), ("azure", "flexible", "mysql"),
    ("tencent", "tencentdb", "postgres"), ("alibaba", "apsaradb", "mysql")])
def test_a_setting_on_a_cloud_migkit_already_spoke_to_reads_as_before(
        provider, where, family):
    got = managed.spell(family, clouds.Where(provider, where), "log_bin",
                        "ON", True, "the source")
    assert got is not None and got[0] == [], got
    assert any("log_bin" in line and "ON" in line for line in got[1]), got
