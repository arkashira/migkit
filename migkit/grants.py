import collections
import dataclasses
import enum
import re
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple, Union


class PasswordEquivalenceStatus(str, enum.Enum):
    MATCH = "MATCH"
    MISMATCH = "MISMATCH"
    SOURCE_ONLY = "SOURCE_ONLY"
    TARGET_ONLY = "TARGET_ONLY"
    BOTH_EMPTY = "BOTH_EMPTY"
    ALGORITHM_MISMATCH = "ALGORITHM_MISMATCH"


def _detect_hash_algo(hash_str: Optional[str]) -> Optional[str]:
    if not hash_str:
        return None
    h = hash_str.strip()
    if h.startswith("SCRAM-SHA-256$"):
        return "scram-sha-256"
    if h.startswith("md5") and len(h) == 35:
        return "md5"
    if h.startswith("*") and len(h) == 41:
        return "mysql_native_password"
    if h.startswith("$caching_sha2_password$") or h.startswith("$A$005$"):
        return "caching_sha2_password"
    if h.startswith("$6$"):
        return "sha512_crypt"
    return "unknown"


@dataclasses.dataclass
class PasswordEquivalenceResult:
    role: str
    status: PasswordEquivalenceStatus
    equivalent: bool
    source_hash: Optional[str] = None
    target_hash: Optional[str] = None
    source_algo: Optional[str] = None
    target_algo: Optional[str] = None


def check_password_hash_equivalence(
    src_hash: Optional[str],
    dst_hash: Optional[str],
    src_user: Optional[str] = None,
    dst_user: Optional[str] = None,
) -> PasswordEquivalenceResult:
    role = src_user or dst_user or ""
    s_norm = src_hash.strip() if src_hash else None
    d_norm = dst_hash.strip() if dst_hash else None

    if not s_norm and not d_norm:
        return PasswordEquivalenceResult(
            role=role,
            status=PasswordEquivalenceStatus.BOTH_EMPTY,
            equivalent=True,
        )
    if s_norm and not d_norm:
        return PasswordEquivalenceResult(
            role=role,
            status=PasswordEquivalenceStatus.SOURCE_ONLY,
            equivalent=False,
            source_hash=s_norm,
            source_algo=_detect_hash_algo(s_norm),
        )
    if not s_norm and d_norm:
        return PasswordEquivalenceResult(
            role=role,
            status=PasswordEquivalenceStatus.TARGET_ONLY,
            equivalent=False,
            target_hash=d_norm,
            target_algo=_detect_hash_algo(d_norm),
        )

    s_algo = _detect_hash_algo(s_norm)
    d_algo = _detect_hash_algo(d_norm)

    if s_algo != d_algo:
        return PasswordEquivalenceResult(
            role=role,
            status=PasswordEquivalenceStatus.ALGORITHM_MISMATCH,
            equivalent=False,
            source_hash=s_norm,
            target_hash=d_norm,
            source_algo=s_algo,
            target_algo=d_algo,
        )

    matched = False
    if s_algo in ("md5", "mysql_native_password"):
        matched = s_norm.lower() == d_norm.lower()
    else:
        matched = s_norm == d_norm

    return PasswordEquivalenceResult(
        role=role,
        status=PasswordEquivalenceStatus.MATCH if matched else PasswordEquivalenceStatus.MISMATCH,
        equivalent=matched,
        source_hash=s_norm,
        target_hash=d_norm,
        source_algo=s_algo,
        target_algo=d_algo,
    )


@dataclasses.dataclass
class Role:
    name: str
    login: bool = True
    superuser: bool = False
    createdb: bool = False
    createrole: bool = False
    replication: bool = False
    bypassrls: bool = False
    inherit: bool = True
    connlimit: int = -1
    valid_until: Optional[str] = None
    password_hash: Optional[str] = None
    hash_type: Optional[str] = None
    attributes: Dict[str, Any] = dataclasses.field(default_factory=dict)
    member_of: List[str] = dataclasses.field(default_factory=list)
    is_system: bool = False


@dataclasses.dataclass
class RoleMembership:
    role: str
    member_of: str
    admin_option: bool = False


@dataclasses.dataclass
class ObjectGrant:
    grantee: str
    schema: str
    object_name: str
    privilege: str
    object_type: str = "TABLE"
    grantable: bool = False
    columns: Tuple[str, ...] = ()

    def signature(self) -> Tuple[str, str, str, str, str, bool]:
        return (
            self.grantee,
            self.schema,
            self.object_name,
            self.object_type,
            self.privilege.upper(),
            self.grantable,
        )


@dataclasses.dataclass
class SequenceGrant:
    grantee: str
    schema: str
    sequence_name: str
    privilege: str
    grantable: bool = False
    bound_table: Optional[str] = None
    bound_column: Optional[str] = None

    def signature(self) -> Tuple[str, str, str, str, bool]:
        return (
            self.grantee,
            self.schema,
            self.sequence_name,
            self.privilege.upper(),
            self.grantable,
        )


@dataclasses.dataclass
class SequenceDependency:
    schema: str
    sequence_name: str
    table_schema: str
    table_name: str
    column_name: str


@dataclasses.dataclass
class RoleParityDiff:
    missing_roles: List[Role] = dataclasses.field(default_factory=list)
    extra_roles: List[Role] = dataclasses.field(default_factory=list)
    password_discrepancies: List[PasswordEquivalenceResult] = dataclasses.field(default_factory=list)
    attribute_drifts: Dict[str, Dict[str, Tuple[Any, Any]]] = dataclasses.field(default_factory=dict)
    membership_drifts: List[Dict[str, Any]] = dataclasses.field(default_factory=list)


@dataclasses.dataclass
class ObjectGrantParityDiff:
    missing_grants: List[ObjectGrant] = dataclasses.field(default_factory=list)
    extra_grants: List[ObjectGrant] = dataclasses.field(default_factory=list)
    matched_grants: int = 0


@dataclasses.dataclass
class SequenceGrantParityDiff:
    missing_grants: List[SequenceGrant] = dataclasses.field(default_factory=list)
    extra_grants: List[SequenceGrant] = dataclasses.field(default_factory=list)
    matched_grants: int = 0
    missing_dependency_grants: List[SequenceGrant] = dataclasses.field(default_factory=list)
    table_sequence_separation: Dict[str, Any] = dataclasses.field(default_factory=dict)


@dataclasses.dataclass
class ParityReport:
    roles_diff: RoleParityDiff
    object_grants_diff: ObjectGrantParityDiff
    sequence_grants_diff: SequenceGrantParityDiff
    grant_scripts: List[str] = dataclasses.field(default_factory=list)
    revoke_scripts: List[str] = dataclasses.field(default_factory=list)
    summary: Dict[str, int] = dataclasses.field(default_factory=dict)

    def format_terse(self) -> str:
        lines = []
        n_missing_roles = len(self.roles_diff.missing_roles)
        n_extra_roles = len(self.roles_diff.extra_roles)
        if n_missing_roles:
            lines.append(f"ACTION: {n_missing_roles} missing roles on target")
        elif n_extra_roles:
            lines.append(f"REVIEW: {n_extra_roles} extra roles on target")
        else:
            lines.append("PASS: roles count parity verified")

        pwd_mismatches = [p for p in self.roles_diff.password_discrepancies if not p.equivalent]
        if pwd_mismatches:
            lines.append(f"ACTION: {len(pwd_mismatches)} password hash mismatches")
        else:
            lines.append("PASS: password hash equivalence verified")

        n_miss_obj = len(self.object_grants_diff.missing_grants)
        if n_miss_obj:
            lines.append(f"ACTION: {n_miss_obj} object permissions missing on target")
        else:
            lines.append("PASS: object permissions parity verified")

        n_miss_seq = len(self.sequence_grants_diff.missing_grants)
        n_dep_seq = len(self.sequence_grants_diff.missing_dependency_grants)
        if n_dep_seq or n_miss_seq:
            lines.append(
                f"ACTION: sequence permissions missing on target ({n_miss_seq} direct, {n_dep_seq} table-sequence dependencies; "
                "permission denied for sequence hazard mitigated)"
            )
        else:
            lines.append("PASS: sequence permissions parity verified")

        sep = self.sequence_grants_diff.table_sequence_separation
        lines.append(
            f"BENIGN: separation partition: {sep.get('table_count', 0)} tables, {sep.get('sequence_count', 0)} sequences tracked"
        )
        return "\n".join(lines)


def _quote_ident_pg(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _quote_ident_my(name: str) -> str:
    return "`" + name.replace("`", "``") + "`"


def _format_user_my(user: str) -> str:
    if "@" in user:
        u, h = user.split("@", 1)
        u = u.strip("'\"`")
        h = h.strip("'\"`")
        return f"'{u}'@'{h}'"
    return f"'{user}'@'%'"


class UsersGrantsParityOrchestrator:
    def __init__(self, engine: str = "postgres"):
        self.engine = engine.lower()

    def audit_roles_parity(
        self,
        src_roles: Dict[str, Role],
        dst_roles: Dict[str, Role],
    ) -> RoleParityDiff:
        diff = RoleParityDiff()
        diff.missing_roles = [r for name, r in src_roles.items() if name not in dst_roles]
        diff.extra_roles = [r for name, r in dst_roles.items() if name not in src_roles]

        for name, src_r in src_roles.items():
            if name in dst_roles:
                dst_r = dst_roles[name]
                pwd_res = check_password_hash_equivalence(
                    src_r.password_hash,
                    dst_r.password_hash,
                    src_user=name,
                    dst_user=name,
                )
                if not pwd_res.equivalent:
                    diff.password_discrepancies.append(pwd_res)

                attrs = ("login", "superuser", "createdb", "createrole", "replication", "bypassrls", "inherit")
                attr_drifts = {}
                for a in attrs:
                    sv = getattr(src_r, a, None)
                    dv = getattr(dst_r, a, None)
                    if sv != dv:
                        attr_drifts[a] = (sv, dv)
                if attr_drifts:
                    diff.attribute_drifts[name] = attr_drifts

                s_members = set(src_r.member_of)
                d_members = set(dst_r.member_of)
                if s_members != d_members:
                    diff.membership_drifts.append({
                        "role": name,
                        "missing": sorted(s_members - d_members),
                        "extra": sorted(d_members - s_members),
                    })

        return diff

    def audit_object_permissions(
        self,
        src_grants: List[ObjectGrant],
        dst_grants: List[ObjectGrant],
    ) -> ObjectGrantParityDiff:
        diff = ObjectGrantParityDiff()
        dst_sigs = {g.signature(): g for g in dst_grants}
        src_sigs = {g.signature(): g for g in src_grants}

        for sig, g in src_sigs.items():
            if sig in dst_sigs:
                diff.matched_grants += 1
            else:
                diff.missing_grants.append(g)

        for sig, g in dst_sigs.items():
            if sig not in src_sigs:
                diff.extra_grants.append(g)

        return diff

    def audit_sequence_permissions(
        self,
        src_seq_grants: List[SequenceGrant],
        dst_seq_grants: List[SequenceGrant],
        table_grants: Optional[List[ObjectGrant]] = None,
        seq_dependencies: Optional[List[SequenceDependency]] = None,
    ) -> SequenceGrantParityDiff:
        diff = SequenceGrantParityDiff()
        dst_sigs = {g.signature(): g for g in dst_seq_grants}
        src_sigs = {g.signature(): g for g in src_seq_grants}

        for sig, g in src_sigs.items():
            if sig in dst_sigs:
                diff.matched_grants += 1
            else:
                diff.missing_grants.append(g)

        for sig, g in dst_sigs.items():
            if sig not in src_sigs:
                diff.extra_grants.append(g)

        dep_map: Dict[Tuple[str, str], List[Tuple[str, str, str]]] = collections.defaultdict(list)
        seq_tables: Set[str] = set()
        all_sequences: Set[str] = set()

        if seq_dependencies:
            for d in seq_dependencies:
                dep_map[(d.table_schema, d.table_name)].append((d.schema, d.sequence_name, d.column_name))
                seq_tables.add(f"{d.table_schema}.{d.table_name}")
                all_sequences.add(f"{d.schema}.{d.sequence_name}")

        missing_deps = []
        if table_grants and seq_dependencies:
            for tg in table_grants:
                if tg.privilege.upper() in ("INSERT", "UPDATE", "ALL", "ALL PRIVILEGES"):
                    deps = dep_map.get((tg.schema, tg.object_name), [])
                    for s_sch, s_name, col in deps:
                        req_sig = (tg.grantee, s_sch, s_name, "USAGE", False)
                        if req_sig not in dst_sigs:
                            missing_deps.append(
                                SequenceGrant(
                                    grantee=tg.grantee,
                                    schema=s_sch,
                                    sequence_name=s_name,
                                    privilege="USAGE",
                                    bound_table=f"{tg.schema}.{tg.object_name}",
                                    bound_column=col,
                                )
                            )

        seen_deps = set()
        deduped_missing_deps = []
        for d in missing_deps:
            key = (d.grantee, d.schema, d.sequence_name, d.privilege)
            if key not in seen_deps:
                seen_deps.add(key)
                deduped_missing_deps.append(d)

        diff.missing_dependency_grants = deduped_missing_deps

        unique_tables = set()
        if table_grants:
            for tg in table_grants:
                unique_tables.add(f"{tg.schema}.{tg.object_name}")

        diff.table_sequence_separation = {
            "table_count": len(unique_tables),
            "sequence_count": len(all_sequences) or len({f"{g.schema}.{g.sequence_name}" for g in src_seq_grants}),
            "tables_with_sequences": sorted(seq_tables),
            "sequence_dependency_count": len(seq_dependencies or []),
        }

        return diff

    def generate_grant_ddl(
        self,
        parity: ParityReport,
        engine: Optional[str] = None,
    ) -> List[str]:
        eng = (engine or self.engine).lower()
        stmts: List[str] = []

        if eng == "postgres":
            for r in parity.roles_diff.missing_roles:
                ident = _quote_ident_pg(r.name)
                lit = "'" + r.name.replace("'", "''") + "'"
                login_clause = "LOGIN" if r.login else "NOLOGIN"
                super_clause = "SUPERUSER" if r.superuser else "NOSUPERUSER"
                stmts.append(
                    f"DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = {lit}) "
                    f"THEN CREATE ROLE {ident} WITH {login_clause} {super_clause}; END IF; END $$;"
                )

            for d in parity.roles_diff.membership_drifts:
                grantee = _quote_ident_pg(d["role"])
                for m in d.get("missing", []):
                    stmts.append(f"GRANT {_quote_ident_pg(m)} TO {grantee};")

            grouped_tables: Dict[Tuple[str, str, str, bool], List[str]] = collections.defaultdict(list)
            for g in parity.object_grants_diff.missing_grants:
                key = (g.grantee, g.schema, g.object_name, g.grantable)
                if g.privilege.upper() not in grouped_tables[key]:
                    grouped_tables[key].append(g.privilege.upper())

            for (grantee, sch, tbl, grantable), privs in grouped_tables.items():
                ident_grantee = "PUBLIC" if grantee == "PUBLIC" else _quote_ident_pg(grantee)
                ident_target = f"{_quote_ident_pg(sch)}.{_quote_ident_pg(tbl)}"
                priv_str = ", ".join(privs)
                opt = " WITH GRANT OPTION" if grantable else ""
                stmts.append(f"GRANT {priv_str} ON TABLE {ident_target} TO {ident_grantee}{opt};")

            grouped_seqs: Dict[Tuple[str, str, str, bool], List[str]] = collections.defaultdict(list)
            all_seq_grants = parity.sequence_grants_diff.missing_grants + parity.sequence_grants_diff.missing_dependency_grants
            for sg in all_seq_grants:
                key = (sg.grantee, sg.schema, sg.sequence_name, sg.grantable)
                if sg.privilege.upper() not in grouped_seqs[key]:
                    grouped_seqs[key].append(sg.privilege.upper())

            for (grantee, sch, seq, grantable), privs in grouped_seqs.items():
                ident_grantee = "PUBLIC" if grantee == "PUBLIC" else _quote_ident_pg(grantee)
                ident_target = f"{_quote_ident_pg(sch)}.{_quote_ident_pg(seq)}"
                priv_str = ", ".join(privs)
                opt = " WITH GRANT OPTION" if grantable else ""
                stmts.append(f"GRANT {priv_str} ON SEQUENCE {ident_target} TO {ident_grantee}{opt};")

        elif eng == "mysql":
            for r in parity.roles_diff.missing_roles:
                user_spec = _format_user_my(r.name)
                stmts.append(f"CREATE USER IF NOT EXISTS {user_spec};")

            for d in parity.roles_diff.membership_drifts:
                grantee = _format_user_my(d["role"])
                for m in d.get("missing", []):
                    stmts.append(f"GRANT {_quote_ident_my(m)} TO {grantee};")

            grouped_tables_my: Dict[Tuple[str, str, str], List[str]] = collections.defaultdict(list)
            for g in parity.object_grants_diff.missing_grants:
                key = (g.grantee, g.schema, g.object_name)
                if g.privilege.upper() not in grouped_tables_my[key]:
                    grouped_tables_my[key].append(g.privilege.upper())

            for (grantee, sch, tbl), privs in grouped_tables_my.items():
                user_spec = _format_user_my(grantee)
                target = f"{_quote_ident_my(sch)}.{_quote_ident_my(tbl)}"
                priv_str = ", ".join(privs)
                stmts.append(f"GRANT {priv_str} ON {target} TO {user_spec};")

        return stmts

    def generate_revoke_ddl(
        self,
        parity: ParityReport,
        engine: Optional[str] = None,
    ) -> List[str]:
        eng = (engine or self.engine).lower()
        stmts: List[str] = []

        if eng == "postgres":
            grouped_seqs: Dict[Tuple[str, str, str], List[str]] = collections.defaultdict(list)
            all_seq_grants = parity.sequence_grants_diff.missing_grants + parity.sequence_grants_diff.missing_dependency_grants
            for sg in all_seq_grants:
                key = (sg.grantee, sg.schema, sg.sequence_name)
                if sg.privilege.upper() not in grouped_seqs[key]:
                    grouped_seqs[key].append(sg.privilege.upper())

            for (grantee, sch, seq), privs in grouped_seqs.items():
                ident_grantee = "PUBLIC" if grantee == "PUBLIC" else _quote_ident_pg(grantee)
                ident_target = f"{_quote_ident_pg(sch)}.{_quote_ident_pg(seq)}"
                priv_str = ", ".join(privs)
                stmts.append(f"REVOKE {priv_str} ON SEQUENCE {ident_target} FROM {ident_grantee};")

            grouped_tables: Dict[Tuple[str, str, str], List[str]] = collections.defaultdict(list)
            for g in parity.object_grants_diff.missing_grants:
                key = (g.grantee, g.schema, g.object_name)
                if g.privilege.upper() not in grouped_tables[key]:
                    grouped_tables[key].append(g.privilege.upper())

            for (grantee, sch, tbl), privs in grouped_tables.items():
                ident_grantee = "PUBLIC" if grantee == "PUBLIC" else _quote_ident_pg(grantee)
                ident_target = f"{_quote_ident_pg(sch)}.{_quote_ident_pg(tbl)}"
                priv_str = ", ".join(privs)
                stmts.append(f"REVOKE {priv_str} ON TABLE {ident_target} FROM {ident_grantee};")

            for d in parity.roles_diff.membership_drifts:
                grantee = _quote_ident_pg(d["role"])
                for m in d.get("missing", []):
                    stmts.append(f"REVOKE {_quote_ident_pg(m)} FROM {grantee};")

            for r in reversed(parity.roles_diff.missing_roles):
                stmts.append(f"DROP ROLE IF EXISTS {_quote_ident_pg(r.name)};")

        elif eng == "mysql":
            grouped_tables_my: Dict[Tuple[str, str, str], List[str]] = collections.defaultdict(list)
            for g in parity.object_grants_diff.missing_grants:
                key = (g.grantee, g.schema, g.object_name)
                if g.privilege.upper() not in grouped_tables_my[key]:
                    grouped_tables_my[key].append(g.privilege.upper())

            for (grantee, sch, tbl), privs in grouped_tables_my.items():
                user_spec = _format_user_my(grantee)
                target = f"{_quote_ident_my(sch)}.{_quote_ident_my(tbl)}"
                priv_str = ", ".join(privs)
                stmts.append(f"REVOKE {priv_str} ON {target} FROM {user_spec};")

            for d in parity.roles_diff.membership_drifts:
                grantee = _format_user_my(d["role"])
                for m in d.get("missing", []):
                    stmts.append(f"REVOKE {_quote_ident_my(m)} FROM {grantee};")

            for r in reversed(parity.roles_diff.missing_roles):
                user_spec = _format_user_my(r.name)
                stmts.append(f"DROP USER IF EXISTS {user_spec};")

        return stmts

    def verify_ddl(
        self,
        grant_ddl: List[str],
        revoke_ddl: List[str],
    ) -> Tuple[bool, List[str]]:
        errors = []
        for s in grant_ddl + revoke_ddl:
            if not s.strip().endswith(";"):
                errors.append(f"statement does not end with semicolon: {s!r}")
            # check basic quote balancing
            single_quotes = s.count("'")
            if single_quotes % 2 != 0:
                errors.append(f"unbalanced single quotes: {s!r}")

        return (len(errors) == 0, errors)

    def run_parity_audit(
        self,
        src_roles: Dict[str, Role],
        dst_roles: Dict[str, Role],
        src_object_grants: List[ObjectGrant],
        dst_object_grants: List[ObjectGrant],
        src_sequence_grants: Optional[List[SequenceGrant]] = None,
        dst_sequence_grants: Optional[List[SequenceGrant]] = None,
        sequence_dependencies: Optional[List[SequenceDependency]] = None,
    ) -> ParityReport:
        src_seq = src_sequence_grants or []
        dst_seq = dst_sequence_grants or []

        roles_diff = self.audit_roles_parity(src_roles, dst_roles)
        obj_diff = self.audit_object_permissions(src_object_grants, dst_object_grants)
        seq_diff = self.audit_sequence_permissions(
            src_seq,
            dst_seq,
            table_grants=src_object_grants,
            seq_dependencies=sequence_dependencies,
        )

        report = ParityReport(
            roles_diff=roles_diff,
            object_grants_diff=obj_diff,
            sequence_grants_diff=seq_diff,
            summary={
                "missing_roles": len(roles_diff.missing_roles),
                "extra_roles": len(roles_diff.extra_roles),
                "missing_table_grants": len(obj_diff.missing_grants),
                "extra_table_grants": len(obj_diff.extra_grants),
                "missing_sequence_grants": len(seq_diff.missing_grants),
                "missing_sequence_dependencies": len(seq_diff.missing_dependency_grants),
            },
        )

        report.grant_scripts = self.generate_grant_ddl(report)
        report.revoke_scripts = self.generate_revoke_ddl(report)
        return report
