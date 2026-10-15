from dataclasses import replace
from types import SimpleNamespace

import pytest

from migkit import decide, scenarios, tableengine
from migkit.cloudapi import Instance
from migkit.cloud_readiness import Finding


def managed():
    src = Instance("aws", "rds", "source", version="15", klass="db.large")
    dst = replace(src, id="target", version="16")
    offer = {"name": "provider-upgrade", "said": "the service's own in-place upgrade",
             "eligible": ((src.klass, dst.klass, "15", "16"),),
             "execute": lambda: "completed", "startup_seconds": 60,
             "bytes_per_second": 100, "duration_source": "provider duration and rehearsal"}
    return decide.Facts({"source": src, "target": dst, "api_reachable": True,
                         "readiness": [], "offers": (offer,), "size_bytes": 1000})


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
    facts["offers"][0]["eligible"] = ()
    assert scenarios.managed_case(facts).choice.path is None


@pytest.mark.parametrize("key,value,reason", [
    ("api_reachable", False, "reachable provider API"),
    ("size_bytes", None, "measured size"),
    ("readiness", [Finding("fail", "network", "target", "blocked")], "readiness"),
    ("target", Instance("aws", "rds", "target"), "instance classes"),
])
def test_managed_missing_facts_refuse(key, value, reason):
    facts = managed()
    facts[key] = value
    result = scenarios.managed_case(facts)
    assert result.choice.path is None
    assert reason in result.words()


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
    got = scenarios.storage_case("items", source, "innodb", supported=("innodb",),
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
    ddl = scenarios.storage_definition(
        f"CREATE TABLE `items` (`id` int) ENGINE={source}"
        + (" PACK_KEYS=1" if source != "memory" else ""),
        "items", source, "innodb", hop="hop", eng="engine", db="db",
        supported=("innodb",), accepted=True, orphan_rows=0)
    assert called == [("hop", "engine", "db", "items")]
    assert "ENGINE=innodb" in ddl
    assert "PACK_KEYS" not in ddl


def test_existing_target_is_changed_in_place():
    got = scenarios.storage_case("items", "myisam", "innodb", exists=True,
                                 supported=("innodb",), accepted=True, orphan_rows=0)
    assert got.choice.path == "in place"
    with pytest.raises(ValueError, match="converted in place"):
        scenarios.storage_definition("CREATE TABLE t (id int)", "items", "myisam", "innodb",
                                     hop="hop", eng="engine", db="db", exists=True,
                                     supported=("innodb",), accepted=True, orphan_rows=0)


@pytest.mark.parametrize("target", ["aria", "memory"])
def test_storage_transaction_or_durability_loss_is_refused(target):
    got = scenarios.storage_case("items", "innodb", target, supported=(target,), accepted=True)
    assert got.choice.path is None
    assert "lost transactions" in got.words()


@pytest.mark.parametrize("orphans", [None, 3])
def test_new_foreign_keys_require_zero_orphans(orphans):
    got = scenarios.storage_case("items", "myisam", "innodb", supported=("innodb",),
                                 accepted=True, orphan_rows=orphans)
    assert got.choice.path is None
    assert "zero rows refused" in got.words()


def test_postgres_columnar_is_not_assumed_to_have_heap_semantics():
    got = scenarios.storage_case("public.items", "heap", "columnar", family="postgres",
                                 supported=("heap", "columnar"), accepted=True)
    assert got.choice.path is None
    assert "known storage semantics" in got.words()
    traits = tableengine.Traits("heap", True, True, "row", "postgres")
    got = scenarios.storage_case("public.items", "heap", "columnar", family="postgres",
                                 supported=("columnar",), accepted=True, orphan_rows=0,
                                 postgres_traits={"heap": traits, "columnar": traits})
    assert got.choice.path is None
    assert "ALTER TABLE SET ACCESS METHOD support" in got.words()
    facts = dict(family="postgres", exists=True, supported=("columnar",),
                 accepted=True, orphan_rows=0, alter_access_method=True,
                 postgres_traits={"heap": traits, "columnar": traits})
    assert scenarios.storage_case("public.items", "heap", "columnar", **facts).choice.path == "in place"
    assert scenarios.storage_alter("public.items", "heap", "columnar", **facts) == (
        'ALTER TABLE "public"."items" SET ACCESS METHOD "columnar"')


def test_storage_reader_reads_both_servers_and_scope():
    class Engine:
        CANON_ENGINE = "mysql"
        def _d(self, side, db):
            return db
        def _q(self, side, sql, args):
            assert "information_schema.tables" in sql
            return [("items", "MyISAM" if side == "src" else "InnoDB"), ("ignored", "MEMORY")]
        def in_scope(self, db, table):
            return table == "items"
    assert scenarios.storage_facts(Engine(), "db") == {"src": {"items": "myisam"},
                                                            "dst": {"items": "innodb"}}


def test_mysql_architecture_reader_reads_both_sides():
    calls = []
    class Engine:
        CANON_ENGINE = "mysql"
        def _q(self, side, sql):
            calls.append(side)
            if "variables_info" in sql:
                return [("collation_server", "utf8mb4_bin", "COMPILED"),
                        ("lower_case_table_names", "0", "COMPILED")]
            return [("1.2345678901234567", "false")]
    facts = scenarios.architecture_facts(Engine(), "db", lambda side: hosts()[side])
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
    class Engine:
        CANON_ENGINE = engine
        def _cmd(self, side, db, sql):
            calls.append(side)
            if "databasepropertyex" in sql:
                return [("SQL_Latin1_General_CP1_CI_AS", 0)]
            return [("1.2345678901234567", "false")]
    facts = scenarios.architecture_facts(Engine(), "db", lambda side: hosts()[side])
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
    eng = SimpleNamespace(CANON_ENGINE="postgres", in_scope=lambda db, table: True)
    assert scenarios.storage_facts(eng, "db") == {
        "src": {"public.items": "heap"}, "dst": {"public.items": "columnar"}}
    assert calls == ["src", "dst"]


def test_managed_facts_use_existing_cloud_owners(monkeypatch):
    from migkit import clouds, cloud_readiness
    src, dst = managed()["source"], managed()["target"]
    calls = []
    def location(eng, side, ep):
        calls.append(side)
        return clouds.Where("aws", "rds")
    monkeypatch.setattr(clouds, "of", location)
    def audit(a, b):
        assert a["klass"] == src.klass
        assert b["version"] == dst.version
        return []
    monkeypatch.setattr(cloud_readiness, "audit", audit)
    hop = SimpleNamespace(source=object(), target=object())
    facts = scenarios.managed_facts(hop, object(),
                                    SimpleNamespace(instance=lambda: src),
                                    SimpleNamespace(instance=lambda: dst),
                                    managed()["offers"], 1000)
    assert calls == ["src", "dst"]
    assert scenarios.managed_case(facts).choice.path == "provider-upgrade"


def test_unsupported_target_storage_and_fulltext_loss_refuse():
    got = scenarios.storage_case("items", "aria", "innodb", accepted=True, orphan_rows=0)
    assert got.choice.path is None
    assert "supported storage method" in got.words()
    got = scenarios.storage_case("items", "myisam", "memory", accepted=True,
                                 supported=("memory",), used_features=("fulltext",))
    assert got.choice.path is None


def test_size_ranks_only_safe_managed_paths():
    facts = managed()
    first = facts["offers"][0]
    second = dict(first, name="provider-blue-green", said="the service's blue/green path",
                  startup_seconds=120, bytes_per_second=1000)
    facts["offers"] = (first, second)
    assert scenarios.managed_case(facts).choice.path == first["name"]
    facts["size_bytes"] = 100000
    assert scenarios.managed_case(facts).choice.path == second["name"]
    second["eligible"] = ()
    assert scenarios.managed_case(facts).choice.path == first["name"]



def test_postgres_access_method_reader_uses_the_existing_text_protocol():
    calls = []
    class Engine:
        CANON_ENGINE = "postgres"
        def in_scope(self, db, table):
            return True
        def _psql(self, side, db, sql):
            calls.append(side)
            assert "c.relname || chr(31) || a.amname" in sql
            return "public.items\x1f" + ("heap" if side == "src" else "columnar")
    assert scenarios.storage_facts(Engine(), "db") == {
        "src": {"public.items": "heap"}, "dst": {"public.items": "columnar"}}
    assert calls == ["src", "dst"]


def test_storage_definition_refuses_a_different_schema_plan(monkeypatch):
    change = tableengine.Change("items", "myisam", "aria", "observed")
    monkeypatch.setattr(tableengine, "plan", lambda hop, eng, db: SimpleNamespace(of=lambda table: change))
    with pytest.raises(ValueError, match="differs from the schema step"):
        scenarios.storage_definition("CREATE TABLE items (id int) ENGINE=MyISAM", "items", "myisam", "innodb",
                                     hop=object(), eng=object(), db="db", supported=("innodb",),
                                     accepted=True, orphan_rows=0)


def test_byte_order_difference_is_said_and_pages_refused():
    facts = hosts()
    facts["dst"]["byte_order"] = "big"
    got = scenarios.architecture_case(facts)
    assert got.choice.path == "architecture-logical"
    assert "byte_order: 'little' to 'big'" in got.words()
