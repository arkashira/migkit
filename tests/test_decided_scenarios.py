from dataclasses import replace
from types import SimpleNamespace

import pytest

from migkit import cloudapi, decide, scenarios, tableengine
from migkit.cloudapi import Instance
from migkit.engines.base import Engine as BaseEngine
from migkit.engines.mssql import MSSQLEngine
from migkit.engines.mysql import MySQLEngine
from migkit.engines.postgres import PostgresEngine


UPGRADE = next(op for op in cloudapi.OPERATIONS["aws"]
               if op.name == "aws-major-upgrade")
NEVER = (("an eligible class", lambda s, d: False,
          "the provider does not offer it between these classes"),)


def _change(table, a, b, exists):
    return tableengine.Change(table, a, b, "", there=a if exists else "")


def _kw(kw):
    return {("observed_traits" if k == "postgres_traits" else k): v
            for k, v in kw.items()}


def storage_case(table, a, b, exists=False, **kw):
    return tableengine.decided(_change(table, a, b, exists), **_kw(kw))


def storage_alter(table, a, b, exists=False, family="postgres", **kw):
    return tableengine.access_method_alter(_change(table, a, b, exists),
                                           **_kw(kw))


def storage_definition(ddl, table, a, b, *, hop, eng, db, **kw):
    return tableengine.definition(hop, eng, db, table, ddl, **_kw(kw))


def managed():
    src = Instance("aws", "rds", "source", engine="postgres", version="15",
                   klass="db.large", upgrades=frozenset({"16"}),
                   status="available", storage_gb=10,
                   maintenance="sun:03:00-sun:04:00")
    dst = replace(src, id="target", version="16")
    offer = {"name": "provider-upgrade", "said": "the service's own in-place upgrade",
             "checks": UPGRADE.checks,
             "execute": lambda: "completed", "startup_seconds": 60,
             "startup_source": "documented startup",
             "runs": [{"bytes": 1000, "seconds": 70}],
             "duration_source": "on the rehearsal r", "truth": "predicted"}
    return decide.Facts({"source": src, "target": dst, "api_reachable": True,
                         "offers": (offer,), "size_bytes": 1000})


@pytest.mark.parametrize("label", ["EC2 to RDS", "RDS to Aurora",
                                    "Azure single to flexible server",
                                    "Cloud SQL in-place major upgrade"])
def test_managed_paths_need_provider_eligibility_not_service_names(label):
    facts = managed()
    facts["offers"][0]["said"] = label
    got = scenarios.managed_case(facts)
    assert got.choice.path == "provider-upgrade"
    assert got.choice.cost().mean == 70
    assert got.choice.cost().truth == "predicted"
    assert label in got.words()
    assert "migkit's full verify" in got.words()
    facts["offers"][0]["checks"] = NEVER
    assert scenarios.managed_case(facts).choice.path is None


def test_managed_unreachable_api_refuses_with_the_error():
    facts = managed()
    facts["api_reachable"] = False
    facts["api_error"] = "no credentials"
    got = scenarios.managed_case(facts)
    assert got.choice.path is None
    assert "was not reached: no credentials" in got.words()


@pytest.mark.parametrize("change,reason", [
    (lambda f: f.update(size_bytes=None), "does not say the measured size"),
    (lambda f: f.update(target=replace(f["target"], klass="")), "target's klass"),
    (lambda f: f.update(source=replace(f["source"], version="")), "source's version"),
    (lambda f: f.update(target=replace(f["target"], status="")), "target's status"),
    (lambda f: f.update(target=replace(f["target"], status="modifying")),
     "the target is modifying, not available"),
    (lambda f: f.update(source=replace(f["source"], status="stopped")),
     "the source is stopped, not available"),
    (lambda f: f.update(target=replace(f["target"], storage_gb=None)),
     "does not say the target's storage"),
    (lambda f: f.update(size_bytes=11 * 1024 ** 3), "the target holds 10 GiB"),
    (lambda f: f.update(target=replace(f["target"], maintenance="")),
     "does not say the target's maintenance window"),
])
def test_managed_gates_refuse_with_their_reason(change, reason):
    facts = managed()
    change(facts)
    result = scenarios.managed_case(facts)
    assert result.choice.path is None
    assert reason in result.words()


@pytest.mark.parametrize("change", [
    lambda f: f.update(size_bytes=None),
    lambda f: f.update(target=replace(f["target"], storage_gb=None)),
    lambda f: f.update(target=replace(f["target"], maintenance="")),
    lambda f: f.update(target=replace(f["target"], status="")),
])
def test_a_fact_the_api_does_not_give_is_said_not_yet_never_assumed(change):
    facts = managed()
    change(facts)
    assert "not yet: the provider's API does not say" in scenarios.managed_case(facts).words()


def test_managed_case_says_the_window_it_read():
    got = scenarios.managed_case(managed())
    assert "maintenance window: sun:03:00-sun:04:00" in got.words()


def test_managed_size_changes_cost_and_failed_verify_is_not_success():
    facts = managed()
    facts["size_bytes"] = 10000
    assert scenarios.managed_case(facts).choice.cost().mean == 160
    calls = []
    facts["offers"][0]["execute"] = lambda: calls.append("execute")
    def verify():
        calls.append("verify")
        return False
    with pytest.raises(ValueError, match="full verify did not pass"):
        scenarios.run_managed_case(facts, verify)
    assert calls == ["execute", "verify"]
    result, proved = scenarios.run_managed_case(managed(), lambda: True)
    assert result == "completed"
    assert proved.choice.path == "provider-upgrade"


def test_no_provider_offer_is_no_managed_path():
    facts = managed()
    facts["offers"] = ()
    assert scenarios.managed_case(facts).choice.path is None


def hosts():
    a = {"architecture": "x86_64", "os": "Linux", "byte_order": "little",
         "collation": "utf8mb4_bin", "case_sensitivity": "0", "float_rendering": "1.2345678901234567"}
    return {"src": a, "dst": dict(a, architecture="aarch64")}


@pytest.mark.parametrize("os_pair", [("Linux", "Linux"), ("Windows", "Linux")])
def test_architecture_and_os_changes_choose_logical(os_pair):
    facts = hosts()
    facts["src"]["os"], facts["dst"]["os"] = os_pair
    got = scenarios.architecture_case(facts)
    assert got.choice.path == "architecture-logical"
    assert "x86_64" in got.words() and "aarch64" in got.words()
    assert "byte_order" in got.words() and "collation" in got.words()
    assert "same architecture, OS and byte order" in got.choice.reason


@pytest.mark.parametrize("key", ["collation", "case_sensitivity", "float_rendering", "float_setting"])
def test_architecture_semantics_must_be_carried_by_schema(key):
    facts = hosts()
    facts["dst"][key] = "different"
    refused = scenarios.architecture_case(facts)
    assert refused.choice.path is None
    assert key in refused.words()
    assert scenarios.architecture_case(facts, schema_carries=(key,)).choice.path == "architecture-logical"


def test_unread_byte_order_is_not_assumed_from_cpu_name():
    facts = hosts()
    del facts["dst"]["byte_order"]
    got = scenarios.architecture_case(facts)
    assert got.choice.path is None
    assert "unread server facts: byte_order" in got.words()


@pytest.mark.parametrize("source", ["myisam", "aria", "memory"])
def test_storage_change_reuses_schema_definition(source, monkeypatch):
    got = storage_case("items", source, "innodb", supported=("innodb",),
                                 accepted=True, orphan_rows=0)
    assert got.choice.path == "definition"
    for fact in ("transactions", "foreign_keys", "fulltext", "locks", "holds"):
        assert fact in got.words()
    change = tableengine.Change("items", source, "innodb", "observed")
    monkeypatch.setattr(tableengine, "plan", lambda hop, eng, db: SimpleNamespace(of=lambda table: change))
    called = []
    def schema(hop, eng, db, table, ddl):
        called.append((hop, eng, db, table))
        return tableengine.rewrite(ddl, change)
    monkeypatch.setattr(tableengine, "schema", schema)
    ddl = storage_definition(
        f"CREATE TABLE `items` (`id` int) ENGINE={source}"
        + (" PACK_KEYS=1" if source != "memory" else ""),
        "items", source, "innodb", hop="hop", eng="engine", db="db",
        supported=("innodb",), accepted=True, orphan_rows=0)
    assert called == [("hop", "engine", "db", "items")]
    assert "ENGINE=innodb" in ddl
    assert "PACK_KEYS" not in ddl


def test_existing_target_is_changed_in_place():
    got = storage_case("items", "myisam", "innodb", exists=True,
                                 supported=("innodb",), accepted=True, orphan_rows=0)
    assert got.choice.path == "in place"


def test_a_planned_change_on_an_existing_table_is_no_definition(monkeypatch):
    change = tableengine.Change("items", "myisam", "innodb", "observed", there="myisam")
    monkeypatch.setattr(tableengine, "plan", lambda hop, eng, db: SimpleNamespace(of=lambda table: change))
    with pytest.raises(ValueError, match="converted in place"):
        storage_definition("CREATE TABLE t (id int)", "items", "myisam", "innodb",
                           hop="hop", eng="engine", db="db",
                           supported=("innodb",), accepted=True, orphan_rows=0)
    monkeypatch.setattr(tableengine, "plan", lambda hop, eng, db: SimpleNamespace(of=lambda table: None))
    with pytest.raises(ValueError, match="plans no storage change"):
        storage_definition("CREATE TABLE t (id int)", "items", "myisam", "innodb",
                           hop="hop", eng="engine", db="db",
                           supported=("innodb",), accepted=True, orphan_rows=0)


@pytest.mark.parametrize("target", ["aria", "memory"])
def test_storage_transaction_or_durability_loss_is_refused(target):
    got = storage_case("items", "innodb", target, supported=(target,), accepted=True)
    assert got.choice.path is None
    assert "lost transactions" in got.words()


@pytest.mark.parametrize("orphans", [None, 3])
def test_new_foreign_keys_require_zero_orphans(orphans):
    got = storage_case("items", "myisam", "innodb", supported=("innodb",),
                                 accepted=True, orphan_rows=orphans)
    assert got.choice.path is None
    assert "zero rows refused" in got.words()


def test_postgres_columnar_is_not_assumed_to_have_heap_semantics():
    got = storage_case("public.items", "heap", "columnar", family="postgres",
                                 supported=("heap", "columnar"), accepted=True)
    assert got.choice.path is None
    assert "known storage semantics" in got.words()
    traits = tableengine.Traits("heap", True, True, "row", "postgres")
    got = storage_case("public.items", "heap", "columnar", family="postgres",
                                 supported=("columnar",), accepted=True, orphan_rows=0,
                                 postgres_traits={"heap": traits, "columnar": traits})
    assert got.choice.path is None
    assert "ALTER TABLE SET ACCESS METHOD support" in got.words()
    facts = dict(family="postgres", exists=True, supported=("columnar",),
                 accepted=True, orphan_rows=0, alter_access_method=True,
                 postgres_traits={"heap": traits, "columnar": traits})
    assert storage_case("public.items", "heap", "columnar", **facts).choice.path == "in place"
    assert storage_alter("public.items", "heap", "columnar", **facts) == (
        'ALTER TABLE "public"."items" SET ACCESS METHOD "columnar"')


def test_storage_reader_reads_both_servers_and_scope():
    class Engine(MySQLEngine):
        CANON_ENGINE = "mysql"
        def _d(self, side, db):
            return db
        def _q(self, side, sql, args):
            assert "information_schema.tables" in sql
            return [("items", "MyISAM" if side == "src" else "InnoDB"), ("ignored", "MEMORY")]
        def in_scope(self, db, table):
            return table == "items"
    assert tableengine.observed(Engine.__new__(Engine), "db") == {"src": {"items": "myisam"},
                                                            "dst": {"items": "innodb"}}


def test_mysql_architecture_reader_reads_both_sides():
    calls = []
    class Engine(MySQLEngine):
        CANON_ENGINE = "mysql"
        def _q(self, side, sql):
            calls.append(side)
            if "variables_info" in sql:
                return [("collation_server", "utf8mb4_bin", "COMPILED"),
                        ("lower_case_table_names", "0", "COMPILED")]
            return [("1.2345678901234567", "false")]
    facts = scenarios.architecture_facts(Engine.__new__(Engine), "db", lambda side: hosts()[side])
    assert calls == ["src", "src", "dst", "dst"]
    assert facts["src"]["collation"] == "utf8mb4_bin"
    assert scenarios.architecture_case(facts).choice.path == "architecture-logical"


@pytest.mark.parametrize("engine", ["postgres", "mssql"])
def test_other_architecture_readers_keep_engine_collations(monkeypatch, engine):
    from migkit import upgrade
    calls = []
    def pg(e, side, db, sql):
        calls.append(side)
        if "pg_settings" in sql:
            return [("lc_collate", "C"), ("lc_ctype", "C"), ("extra_float_digits", "3")]
        return [("1.2345678901234567", "false")]
    monkeypatch.setattr(upgrade, "_pg", pg)
    monkeypatch.setattr(upgrade, "pg_collations", lambda e, side, db: {"C": "1"})
    class Engine(PostgresEngine if engine == "postgres" else MSSQLEngine):
        CANON_ENGINE = engine
        def _cmd(self, side, db, sql):
            calls.append(side)
            if "databasepropertyex" in sql:
                return [("SQL_Latin1_General_CP1_CI_AS", 0)]
            return [("1.2345678901234567", "false")]
    facts = scenarios.architecture_facts(Engine.__new__(Engine), "db", lambda side: hosts()[side])
    assert calls == ["src", "src", "dst", "dst"]
    expected = ("C", {"C": "1"}) if engine == "postgres" else "SQL_Latin1_General_CP1_CI_AS"
    assert facts["src"]["collation"] == expected
    assert facts["dst"]["collation"] == expected
    assert scenarios.architecture_case(facts).choice.path == "architecture-logical"


def test_postgres_storage_reader_does_not_call_heap_memory(monkeypatch):
    from migkit import upgrade
    calls = []
    def pg(e, side, db, sql):
        calls.append(side)
        assert "pg_am" in sql
        return [("public.items", "heap" if side == "src" else "columnar")]
    monkeypatch.setattr(upgrade, "_pg", pg)
    eng = PostgresEngine.__new__(PostgresEngine)
    eng.in_scope = lambda db, table: True
    assert tableengine.observed(eng, "db") == {
        "src": {"public.items": "heap"}, "dst": {"public.items": "columnar"}}
    assert calls == ["src", "dst"]


def test_managed_facts_use_existing_cloud_owners(monkeypatch):
    from migkit import clouds
    src, dst = managed()["source"], managed()["target"]
    calls = []
    def location(eng, side, ep):
        calls.append(side)
        return clouds.Where("aws", "rds")
    monkeypatch.setattr(clouds, "of", location)
    hop = SimpleNamespace(source=object(), target=object())
    facts = scenarios.managed_facts(hop, object(),
                                    SimpleNamespace(instance=lambda: src),
                                    SimpleNamespace(instance=lambda: dst),
                                    managed()["offers"], 1000)
    assert calls == ["src", "dst"]
    assert facts["api_reachable"] is True
    assert facts["target"].maintenance == dst.maintenance
    assert scenarios.managed_case(facts).choice.path == "provider-upgrade"


def test_an_api_that_does_not_answer_is_a_fact_not_an_exception(monkeypatch):
    from migkit import clouds
    monkeypatch.setattr(clouds, "of", lambda eng, side, ep: clouds.Where())
    def down():
        raise SystemExit("the provider refused the credentials")
    hop = SimpleNamespace(source=object(), target=object())
    facts = scenarios.managed_facts(hop, object(),
                                    SimpleNamespace(instance=down),
                                    SimpleNamespace(instance=lambda: None),
                                    managed()["offers"], 1000)
    assert facts["api_reachable"] is False
    assert facts["api_error"] == "the provider refused the credentials"
    got = scenarios.managed_case(facts)
    assert got.choice.path is None
    assert "the provider refused the credentials" in got.words()


def test_the_plan_says_the_case_from_what_the_providers_state(monkeypatch):
    from migkit import clouds, managed as catalogue
    facts = managed()
    monkeypatch.setattr(clouds, "of", lambda eng, side, ep: clouds.Where("aws", "rds"))
    monkeypatch.setattr(catalogue, "api_conf", lambda hop, eng, which: ("aws", {}))
    adapter = SimpleNamespace(instance=lambda: facts["source"])
    monkeypatch.setattr(catalogue, "adapter", lambda hop, eng, which: adapter)
    eng = SimpleNamespace(table_sizes=lambda db, quick=False: {
        "a": {"bytes": 600}, "b": {"bytes": 400}})
    hop = SimpleNamespace(source=object(), target=object())
    lines = scenarios.plan_lines(hop, eng, "db", offers=facts["offers"])
    text = "\n".join(lines)
    assert "managed service change" in text
    assert "the service's own in-place upgrade" in text
    assert "source: db.large, version 15, available, 10 GiB" in text
    assert "window sun:03:00-sun:04:00" in text
    none = "\n".join(scenarios.plan_lines(hop, eng, "db"))
    assert "no eligible provider operation was read" in none


def test_the_plan_asks_nothing_of_a_side_with_no_provider_api(monkeypatch):
    from migkit import managed as catalogue
    monkeypatch.setattr(catalogue, "api_conf",
                        lambda hop, eng, which: (None, "run by its owner"))
    def adapter(*a):
        raise AssertionError("no provider API should be asked")
    monkeypatch.setattr(catalogue, "adapter", adapter)
    assert scenarios.plan_lines(object(), object(), "db") == []


def test_unsupported_target_storage_and_fulltext_loss_refuse():
    got = storage_case("items", "aria", "innodb", accepted=True, orphan_rows=0)
    assert got.choice.path is None
    assert "supported storage method" in got.words()
    got = storage_case("items", "myisam", "memory", accepted=True,
                                 supported=("memory",), used_features=("fulltext",))
    assert got.choice.path is None


def test_size_ranks_only_safe_managed_paths():
    facts = managed()
    first = facts["offers"][0]
    second = dict(first, name="provider-blue-green", said="the service's blue/green path",
                  startup_seconds=120, runs=[{"bytes": 1000, "seconds": 121}])
    facts["offers"] = (first, second)
    assert scenarios.managed_case(facts).choice.path == first["name"]
    facts["size_bytes"] = 100000
    assert scenarios.managed_case(facts).choice.path == second["name"]
    second["checks"] = NEVER
    assert scenarios.managed_case(facts).choice.path == first["name"]



def test_postgres_access_method_reader_uses_the_existing_text_protocol():
    calls = []
    class Engine(PostgresEngine):
        CANON_ENGINE = "postgres"
        def in_scope(self, db, table):
            return True
        def _psql(self, side, db, sql):
            calls.append(side)
            assert "c.relname || chr(31) || a.amname" in sql
            return "public.items\x1f" + ("heap" if side == "src" else "columnar")
    assert tableengine.observed(Engine.__new__(Engine), "db") == {
        "src": {"public.items": "heap"}, "dst": {"public.items": "columnar"}}
    assert calls == ["src", "dst"]


def test_the_definition_decides_the_change_the_schema_step_plans(monkeypatch):
    change = tableengine.Change("items", "myisam", "aria", "observed")
    monkeypatch.setattr(tableengine, "plan", lambda hop, eng, db: SimpleNamespace(of=lambda table: change))
    with pytest.raises(ValueError, match="supported storage method"):
        storage_definition("CREATE TABLE items (id int) ENGINE=MyISAM", "items", "myisam", "innodb",
                                     hop=object(), eng=object(), db="db", supported=("innodb",),
                                     accepted=True, orphan_rows=0)


def test_byte_order_difference_is_said_and_pages_refused():
    facts = hosts()
    facts["dst"]["byte_order"] = "big"
    got = scenarios.architecture_case(facts)
    assert got.choice.path == "architecture-logical"
    assert "byte_order: 'little' to 'big'" in got.words()


<<<<<<< HEAD
class _Unread(BaseEngine):
    CANON_ENGINE = "redis"


def test_an_engine_without_semantic_readers_is_not_yet_never_an_error():
    eng = _Unread.__new__(_Unread)
    facts = scenarios.architecture_facts(eng, "db", lambda side: hosts()[side])
    got = scenarios.architecture_case(facts)
    assert got.choice.path is None
    assert "not yet" in got.choice.reason
    assert "P0-SCENARIOS.arch" in got.choice.reason


def test_an_engine_without_a_storage_reader_is_not_yet_never_an_error():
    eng = _Unread.__new__(_Unread)
    got = tableengine.observed(eng, "db")
    assert got["src"] is None and got["dst"] is None
    assert "P0-SCENARIOS.engine" in got["not_read"]


@pytest.mark.parametrize("family", ["redis", "nosuchengine"])
def test_a_storage_change_on_an_unread_engine_is_not_yet(family):
    got = storage_case("items", "a", "b", family=family, supported=("b",),
                       accepted=True, orphan_rows=0)
    assert got.choice.path is None
    assert "not yet" in got.choice.reason
    assert "P0-SCENARIOS.engine" in got.choice.reason


def test_the_storage_needs_read_the_facts_not_the_arguments():
    change = tableengine.Change("items", "myisam", "innodb", "", there="")
    rungs = decide.fenced(tableengine.ways(), *tableengine.storage_needs())
    facts = decide.Facts({
        "there": False, "source": "myisam", "target": "innodb",
        "source_traits": tableengine.TRAITS["myisam"],
        "target_traits": tableengine.TRAITS["innodb"],
        "supported": ("innodb",), "accepted": True, "orphan_rows": 0,
        "used_features": (), "alter_access_method": False})
    assert decide.climb("t", rungs, facts, need=("engine changed",)).path \
        == "definition"
    facts["orphan_rows"] = 3
    assert decide.climb("t", rungs, facts, need=("engine changed",)).path \
        is None
    assert change.table == "items"
||||||| 580cd213
=======
def test_the_server_says_its_build_and_the_byte_order_follows():
    class My:
        CANON_ENGINE = "mysql"
        def _q(self, side, sql):
            return [("aarch64" if side == "dst" else "x86_64", "Linux")]
    assert scenarios.server_platform(My(), "db", "dst") == {
        "architecture": "aarch64", "os": "linux", "byte_order": "little"}

    class Ms:
        CANON_ENGINE = "mssql"
        def _cmd(self, side, db, sql):
            return [("Microsoft SQL Server 2022 (RTM) (X64) ... on Windows"
                     " Server 2022 Datacenter",)]
    assert scenarios.server_platform(Ms(), "db", "src") == {
        "architecture": "x86_64", "os": "windows", "byte_order": "little"}


def test_an_unread_build_falls_to_the_instance_class_or_stays_unknown():
    class Mute:
        CANON_ENGINE = "mysql"
        def _q(self, side, sql):
            raise RuntimeError("denied")
    arm = Instance("aws", "rds", "db-1", klass="db.r7g.large")
    assert scenarios.server_platform(Mute(), "db", "src", arm)[
        "architecture"] == "aarch64"
    assert scenarios.server_platform(Mute(), "db", "src") == {
        "architecture": None, "os": None, "byte_order": None}


def test_a_target_that_follows_another_server_is_refused():
    facts = hosts()
    facts["dst"]["replica_of"] = "db-primary"
    got = scenarios.architecture_case(facts)
    assert got.choice.path is None
    assert "replica of db-primary" in got.words()


def test_a_provider_operation_is_held_until_it_is_known_to_move_instances():
    facts = hosts()
    facts["offers"] = ({"name": "op", "said": "the provider's switch",
                        "checks": ()},)
    got = scenarios.architecture_case(facts)
    assert got.choice.path == "architecture-logical"
    assert "not yet: whether the provider's switch places the data" \
        in got.choice.reason


def test_the_topology_change_is_said():
    facts = hosts()
    facts["src"].update(product="rds", replicas=0)
    facts["dst"].update(product="aurora", replicas=2)
    words = scenarios.architecture_case(facts).words()
    assert "topology: rds, alone to aurora with 2 replicas" in words


def test_the_plan_carries_the_architecture_case_and_records_it(monkeypatch):
    from migkit import managed as catalogue
    monkeypatch.setattr(catalogue, "api_conf",
                        lambda hop, eng, which: (None, "run by its owner"))
    recorded = []
    monkeypatch.setattr(decide, "record",
                        lambda hop, what, *a, **k: recorded.append(what))
    monkeypatch.setattr(scenarios, "architecture_facts",
                        lambda eng, db, reader: decide.Facts(hosts()))
    eng = SimpleNamespace(CANON_ENGINE="mysql")
    lines = scenarios.plan_lines(object(), eng, "db")
    assert len(lines) == 1
    assert "hardware / OS / architecture change" in lines[0]
    assert "a logical move across hardware or OS" in lines[0]
    assert recorded and recorded[0].path == "architecture-logical"
    same = hosts()
    same["dst"] = dict(same["src"])
    monkeypatch.setattr(scenarios, "architecture_facts",
                        lambda eng, db, reader: decide.Facts(same))
    assert scenarios.plan_lines(object(), eng, "db") == []
>>>>>>> worktree-agent-a5f0e055450860533
