import pytest

from migkit.grants import (
    ObjectGrant,
    ParityReport,
    PasswordEquivalenceStatus,
    Role,
    RoleMembership,
    SequenceDependency,
    SequenceGrant,
    UsersGrantsParityOrchestrator,
    check_password_hash_equivalence,
)


def test_password_hash_equivalence():
    res = check_password_hash_equivalence(
        "SCRAM-SHA-256$4096:salt$stored:server",
        "SCRAM-SHA-256$4096:salt$stored:server",
        "app_user",
        "app_user",
    )
    assert res.status == PasswordEquivalenceStatus.MATCH
    assert res.equivalent is True

    res = check_password_hash_equivalence(
        "md5c4ca4238a0b923820dcc509a6f75849b",
        "md5c4ca4238a0b923820dcc509a6f75849b",
        "app_user",
        "app_user",
    )
    assert res.status == PasswordEquivalenceStatus.MATCH
    assert res.equivalent is True

    res = check_password_hash_equivalence(
        "*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19",
        "*2470c0c06dee42fd1618bb99005adca2ec9d1e19",
        "app_user",
        "app_user",
    )
    assert res.status == PasswordEquivalenceStatus.MATCH
    assert res.equivalent is True

    res = check_password_hash_equivalence(
        "md5c4ca4238a0b923820dcc509a6f75849b",
        "md5eccbc87e4b5ce2fe28308fd9f2a7baf3",
        "app_user",
        "app_user",
    )
    assert res.status == PasswordEquivalenceStatus.MISMATCH
    assert res.equivalent is False

    res = check_password_hash_equivalence(
        "SCRAM-SHA-256$4096:salt$stored:server",
        None,
        "app_user",
        "app_user",
    )
    assert res.status == PasswordEquivalenceStatus.SOURCE_ONLY
    assert res.equivalent is False

    res = check_password_hash_equivalence(
        None,
        "md5c4ca4238a0b923820dcc509a6f75849b",
        "app_user",
        "app_user",
    )
    assert res.status == PasswordEquivalenceStatus.TARGET_ONLY
    assert res.equivalent is False

    res = check_password_hash_equivalence(
        "SCRAM-SHA-256$4096:salt$stored:server",
        "md5c4ca4238a0b923820dcc509a6f75849b",
        "app_user",
        "app_user",
    )
    assert res.status == PasswordEquivalenceStatus.ALGORITHM_MISMATCH
    assert res.equivalent is False


def test_extract_catalog_roles_and_attributes():
    orchestrator = UsersGrantsParityOrchestrator(engine="postgres")
    src_roles = {
        "admin": Role(name="admin", superuser=True, login=True),
        "app_user": Role(
            name="app_user",
            login=True,
            password_hash="SCRAM-SHA-256$4096:s$k:v",
            member_of=["app_readers"],
        ),
        "app_readers": Role(name="app_readers", login=False),
    }
    dst_roles = {
        "admin": Role(name="admin", superuser=True, login=True),
        "app_user": Role(
            name="app_user",
            login=False,
            password_hash="SCRAM-SHA-256$4096:s$k:v",
            member_of=[],
        ),
    }

    diff = orchestrator.audit_roles_parity(src_roles, dst_roles)
    assert len(diff.missing_roles) == 1
    assert diff.missing_roles[0].name == "app_readers"
    assert "app_user" in diff.attribute_drifts
    assert diff.attribute_drifts["app_user"]["login"] == (True, False)
    assert len(diff.membership_drifts) == 1
    assert diff.membership_drifts[0]["role"] == "app_user"
    assert diff.membership_drifts[0]["missing"] == ["app_readers"]


def test_audit_object_permissions_crud():
    orchestrator = UsersGrantsParityOrchestrator(engine="postgres")

    src_grants = [
        ObjectGrant("app_user", "public", "orders", "SELECT"),
        ObjectGrant("app_user", "public", "orders", "INSERT"),
        ObjectGrant("app_user", "public", "orders", "UPDATE"),
        ObjectGrant("app_user", "public", "orders", "DELETE"),
        ObjectGrant("readonly", "public", "orders", "SELECT"),
    ]
    dst_grants = [
        ObjectGrant("app_user", "public", "orders", "SELECT"),
        ObjectGrant("extra_user", "public", "orders", "SELECT"),
    ]

    diff = orchestrator.audit_object_permissions(src_grants, dst_grants)
    assert diff.matched_grants == 1
    assert len(diff.missing_grants) == 4
    missing_privs = {
        (g.grantee, g.schema, g.object_name, g.privilege)
        for g in diff.missing_grants
    }
    assert ("app_user", "public", "orders", "INSERT") in missing_privs
    assert ("app_user", "public", "orders", "UPDATE") in missing_privs
    assert ("app_user", "public", "orders", "DELETE") in missing_privs
    assert ("readonly", "public", "orders", "SELECT") in missing_privs

    assert len(diff.extra_grants) == 1
    assert diff.extra_grants[0].grantee == "extra_user"


def test_audit_sequence_permissions_separation():
    orchestrator = UsersGrantsParityOrchestrator(engine="postgres")

    src_seq_grants = [
        SequenceGrant("app_user", "public", "orders_id_seq", "USAGE"),
        SequenceGrant("app_user", "public", "orders_id_seq", "UPDATE"),
    ]
    dst_seq_grants = []

    table_grants = [
        ObjectGrant("app_user", "public", "orders", "INSERT"),
    ]
    seq_deps = [
        SequenceDependency("public", "orders_id_seq", "public", "orders", "id"),
    ]

    diff = orchestrator.audit_sequence_permissions(
        src_seq_grants,
        dst_seq_grants,
        table_grants=table_grants,
        seq_dependencies=seq_deps,
    )

    assert len(diff.missing_grants) == 2
    assert len(diff.missing_dependency_grants) == 1
    dep = diff.missing_dependency_grants[0]
    assert dep.grantee == "app_user"
    assert dep.sequence_name == "orders_id_seq"
    assert dep.privilege == "USAGE"
    assert dep.bound_table == "public.orders"
    assert dep.bound_column == "id"

    sep = diff.table_sequence_separation
    assert "public.orders" in sep["tables_with_sequences"]
    assert sep["sequence_count"] == 1
    assert sep["table_count"] == 1


def test_field_ops12_reproducer_incident():
    orchestrator = UsersGrantsParityOrchestrator(engine="postgres")

    src_roles = {f"app_user_{i}": Role(name=f"app_user_{i}", login=True) for i in range(38)}
    dst_roles = {}

    src_table_grants = []
    src_seq_grants = []
    seq_deps = []

    for i in range(38):
        role = f"app_user_{i}"
        for tbl_idx in range(10):
            tbl = f"tbl_{tbl_idx}"
            seq = f"{tbl}_id_seq"
            for priv in ("SELECT", "INSERT", "UPDATE", "DELETE"):
                src_table_grants.append(ObjectGrant(role, "public", tbl, priv))
            src_seq_grants.append(SequenceGrant(role, "public", seq, "USAGE"))
            src_seq_grants.append(SequenceGrant(role, "public", seq, "UPDATE"))
            if i == 0:
                seq_deps.append(SequenceDependency("public", seq, "public", tbl, "id"))

    dst_table_grants = []
    dst_seq_grants = []

    report = orchestrator.run_parity_audit(
        src_roles=src_roles,
        dst_roles=dst_roles,
        src_object_grants=src_table_grants,
        dst_object_grants=dst_table_grants,
        src_sequence_grants=src_seq_grants,
        dst_sequence_grants=dst_seq_grants,
        sequence_dependencies=seq_deps,
    )

    assert len(report.roles_diff.missing_roles) == 38
    assert len(report.object_grants_diff.missing_grants) == 38 * 40
    assert len(report.sequence_grants_diff.missing_grants) == 38 * 20
    assert len(report.sequence_grants_diff.missing_dependency_grants) == 38 * 10
    assert len(report.grant_scripts) > 0
    assert len(report.revoke_scripts) > 0

    terse = report.format_terse()
    assert "ACTION: 38 missing roles on target" in terse
    assert "ACTION: sequence permissions missing on target" in terse
    assert "permission denied for sequence" in terse


def test_generate_idempotent_target_grant_and_revoke_ddl():
    orchestrator = UsersGrantsParityOrchestrator(engine="postgres")

    src_roles = {
        "app user": Role(name="app user", login=True),
    }
    dst_roles = {}

    src_table_grants = [
        ObjectGrant("app user", "public", "order items", "SELECT"),
        ObjectGrant("app user", "public", "order items", "INSERT"),
    ]
    dst_table_grants = []

    src_seq_grants = [
        SequenceGrant("app user", "public", "order items_id_seq", "USAGE"),
    ]
    dst_seq_grants = []

    report = orchestrator.run_parity_audit(
        src_roles=src_roles,
        dst_roles=dst_roles,
        src_object_grants=src_table_grants,
        dst_object_grants=dst_table_grants,
        src_sequence_grants=src_seq_grants,
        dst_sequence_grants=dst_seq_grants,
    )

    grants = report.grant_scripts
    revokes = report.revoke_scripts

    assert any('CREATE ROLE "app user"' in s for s in grants)
    assert any('GRANT SELECT, INSERT ON TABLE "public"."order items" TO "app user";' in s for s in grants)
    assert any('GRANT USAGE ON SEQUENCE "public"."order items_id_seq" TO "app user";' in s for s in grants)

    assert any('REVOKE USAGE ON SEQUENCE "public"."order items_id_seq" FROM "app user";' in s for s in revokes)
    assert any('REVOKE SELECT, INSERT ON TABLE "public"."order items" FROM "app user";' in s for s in revokes)
    assert any('DROP ROLE IF EXISTS "app user";' in s for s in revokes)

    valid, errors = orchestrator.verify_ddl(grants, revokes)
    assert valid is True
    assert errors == []


def test_mysql_engine_parity_generation():
    orchestrator = UsersGrantsParityOrchestrator(engine="mysql")

    src_roles = {
        "app_user@%": Role(name="app_user@%", login=True),
    }
    dst_roles = {}

    src_table_grants = [
        ObjectGrant("app_user@%", "appdb", "orders", "SELECT"),
        ObjectGrant("app_user@%", "appdb", "orders", "INSERT"),
        ObjectGrant("app_user@%", "appdb", "orders", "UPDATE"),
        ObjectGrant("app_user@%", "appdb", "orders", "DELETE"),
    ]
    dst_table_grants = []

    report = orchestrator.run_parity_audit(
        src_roles=src_roles,
        dst_roles=dst_roles,
        src_object_grants=src_table_grants,
        dst_object_grants=dst_table_grants,
        src_sequence_grants=[],
        dst_sequence_grants=[],
    )

    grants = report.grant_scripts
    revokes = report.revoke_scripts

    assert any("CREATE USER IF NOT EXISTS 'app_user'@'%'" in s for s in grants)
    assert any("GRANT SELECT, INSERT, UPDATE, DELETE ON `appdb`.`orders` TO 'app_user'@'%';" in s for s in grants)
    assert any("REVOKE SELECT, INSERT, UPDATE, DELETE ON `appdb`.`orders` FROM 'app_user'@'%';" in s for s in revokes)

    valid, errors = orchestrator.verify_ddl(grants, revokes)
    assert valid is True
    assert errors == []


def test_passwordless_user_locking_and_hashes():
    orchestrator = UsersGrantsParityOrchestrator(engine="mysql")
    src_roles = {
        "locked_user@%": Role(name="locked_user@%", login=True),
        "hashed_user@%": Role(
            name="hashed_user@%",
            login=True,
            password_hash="*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19",
        ),
        "sha2_user@%": Role(
            name="sha2_user@%",
            login=True,
            password_hash="$caching_sha2_password$somehash",
        ),
    }
    dst_roles = {}
    report = orchestrator.run_parity_audit(
        src_roles=src_roles,
        dst_roles=dst_roles,
        src_object_grants=[],
        dst_object_grants=[],
    )
    grants = report.grant_scripts
    assert any("CREATE USER IF NOT EXISTS 'locked_user'@'%' ACCOUNT LOCK;" in s for s in grants)
    assert any("CREATE USER IF NOT EXISTS 'hashed_user'@'%' IDENTIFIED WITH mysql_native_password AS '*2470C0C06DEE42FD1618BB99005ADCA2EC9D1E19';" in s for s in grants)
    assert any("CREATE USER IF NOT EXISTS 'sha2_user'@'%' IDENTIFIED WITH caching_sha2_password AS '$caching_sha2_password$somehash';" in s for s in grants)


def test_audit_grants_parity_orchestration():
    from migkit import grants

    class FakeEngine:
        CANON_ENGINE = "postgres"

        def _psql(self, side, db, sql):
            if "pg_roles" in sql and "pg_auth_members" not in sql:
                return "admin|t|t|secret\napp_user|f|t|\n"
            if "pg_auth_members" in sql:
                return "admin|app_user\n"
            if "role_table_grants" in sql:
                return "app_user|public|orders|SELECT|NO\n"
            if "role_usage_grants" in sql:
                return "app_user|public|orders_id_seq|USAGE|NO\n"
            if "pg_depend" in sql:
                return "public|orders_id_seq|public|orders|id\n"
            return ""

    src = FakeEngine()
    dst = FakeEngine()
    rep = grants.audit_grants_parity(src, dst, db="testdb")
    assert rep.summary["missing_roles"] == 0
    assert rep.summary["missing_table_grants"] == 0


PROVES_RUNGS = [
    ("grants", "grants_parity_rungs", "role_and_membership_parity", "holds"),
    ("grants", "grants_parity_rungs", "role_and_membership_parity", "refuses"),
    ("grants", "grants_parity_rungs", "object_crud_permission_parity", "holds"),
    ("grants", "grants_parity_rungs", "object_crud_permission_parity", "refuses"),
    ("grants", "grants_parity_rungs", "sequence_usage_update_parity", "holds"),
    ("grants", "grants_parity_rungs", "sequence_usage_update_parity", "refuses"),
    ("grants", "grants_parity_rungs", "password_hash_equivalence_armor", "holds"),
    ("grants", "grants_parity_rungs", "password_hash_equivalence_armor", "refuses"),
    ("grants", "grants_parity_rungs", "refuse_unverified_grants_cutover", "holds"),
    ("grants", "grants_parity_rungs", "refuse_unverified_grants_cutover", "refuses"),
]

from tests.rung_branches import branch, watched, HOLDS, REFUSES


def test_grants_parity_decide_ladder_all_branches():
    from migkit import grants

    with watched() as seen:
        c = grants.decide_grants_parity({"roles_matched": True})
    assert branch(seen, "role_and_membership_parity") == [HOLDS]
    assert c.rung.name == "role_and_membership_parity"

    with watched() as seen:
        c = grants.decide_grants_parity({
            "roles_divergent": True,
            "object_grants_matched": True,
        })
    assert branch(seen, "role_and_membership_parity") == [REFUSES]
    assert branch(seen, "object_crud_permission_parity") == [HOLDS]
    assert c.rung.name == "object_crud_permission_parity"

    with watched() as seen:
        c = grants.decide_grants_parity({
            "roles_divergent": True,
            "object_grants_divergent": True,
            "sequence_grants_matched": True,
        })
    assert branch(seen, "object_crud_permission_parity") == [REFUSES]
    assert branch(seen, "sequence_usage_update_parity") == [HOLDS]
    assert c.rung.name == "sequence_usage_update_parity"

    with watched() as seen:
        c = grants.decide_grants_parity({
            "roles_divergent": True,
            "object_grants_divergent": True,
            "sequence_grants_divergent": True,
            "password_equivalent": True,
            "passwordless_locked": True,
        })
    assert branch(seen, "sequence_usage_update_parity") == [REFUSES]
    assert branch(seen, "password_hash_equivalence_armor") == [HOLDS]
    assert c.rung.name == "password_hash_equivalence_armor"

    with watched() as seen:
        c = grants.decide_grants_parity({
            "roles_divergent": True,
            "object_grants_divergent": True,
            "sequence_grants_divergent": True,
            "password_hash_divergent": True,
            "explicit_unverified_bypass": True,
        })
    assert branch(seen, "password_hash_equivalence_armor") == [REFUSES]
    assert branch(seen, "refuse_unverified_grants_cutover") == [HOLDS]
    assert c.rung.name == "refuse_unverified_grants_cutover"

    with watched() as seen:
        try:
            grants.decide_grants_parity({
                "roles_divergent": True,
                "object_grants_divergent": True,
                "sequence_grants_divergent": True,
                "password_hash_divergent": True,
                "grants_refused": True,
            })
        except Exception:
            pass
    assert branch(seen, "refuse_unverified_grants_cutover") == [REFUSES]
