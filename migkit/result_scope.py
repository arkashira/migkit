import html
import json
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

CHECK_ORDER = ["schema", "counts", "autoinc", "data", "deep", "delta"]

UNMATCHED_DIMENSIONS = {
    "bidirectional_drift_proof": {
        "name": "Bidirectional Drift Proof",
        "description": "Continuous mathematical multiset hash proof proving zero drift in live bidirectional replication without stopping writes.",
        "barrier_type": "mathematical_proof",
    },
    "schema_invariant_barriers": {
        "name": "Schema Invariant Barriers",
        "description": "Topological dependency DAG and cycle barrier verification preventing FK violations and unsafe cutover ordering.",
        "barrier_type": "verified_barrier",
    },
    "replication_artifact_partitioning": {
        "name": "Replication Artifact Partitioning",
        "description": "Strict isolation of the bookkeeping tables a replication service leaves behind from application defects, with enforced repair guards.",
        "barrier_type": "verified_barrier",
    },
    "sequence_autoinc_headroom": {
        "name": "Sequence & Auto-increment Headroom Proof",
        "description": "Verification that target sequence next values exceed source high-water marks to guarantee zero duplicate key collisions upon write cutover.",
        "barrier_type": "mathematical_proof",
    },
    "routine_event_semantic_parity": {
        "name": "Routine & Event Semantic Parity",
        "description": "Dialect-aware semantic and execution compatibility checks for procedures, functions, triggers, and scheduled events.",
        "barrier_type": "verified_barrier",
    },
    "runtime_parameter_invariants": {
        "name": "Runtime Parameter & Collation Invariants",
        "description": "Cross-engine verification of timezones, character encodings, collations, and transaction isolation levels.",
        "barrier_type": "heuristic_warning",
    },
    "proof_invalidation_ddl_boundary": {
        "name": "Proof Invalidation on DDL Boundary",
        "description": "Detection of schema alterations during verification runs, invalidating prior checksums and preventing false-positive pass verdicts.",
        "barrier_type": "verified_barrier",
    },
}


@dataclass
class PracticalProblem:
    object_name: str
    problem: str
    source_state: str = ""
    target_state: str = ""
    impact: str = ""
    defect_class: str = "application_defect"
    is_replication_artifact: bool = False
    artifact_origin: str = ""
    category: str = "data"
    next_action: str = ""
    check: str = ""
    status: str = "diff"
    severity: str = "error"

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class DatabaseSummary:
    database: str
    status: str
    checks_completed: int
    checks_planned: int
    issues_count: int
    practical_problems: List[PracticalProblem] = field(default_factory=list)
    remaining_work: List[str] = field(default_factory=list)
    categories: Dict[str, int] = field(default_factory=dict)
    unmatched_findings: List[Dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "database": self.database,
            "status": self.status,
            "checks_completed": self.checks_completed,
            "checks_planned": self.checks_planned,
            "issues_count": self.issues_count,
            "practical_problems": [p.to_dict() for p in self.practical_problems],
            "remaining_work": self.remaining_work,
            "categories": self.categories,
            "unmatched_findings": self.unmatched_findings,
        }


@dataclass
class GroupedResultEnvelope:
    status: str
    has_differences: bool
    databases: Dict[str, DatabaseSummary]
    databases_with_issues: List[str]
    databases_clean: List[str]
    databases_incomplete: List[str]
    total_checks: int
    total_passed: int
    total_issues: int
    scope: Dict[str, Any] = field(default_factory=dict)
    coverage: Dict[str, Any] = field(default_factory=dict)
    observation_window: Dict[str, Any] = field(default_factory=dict)
    unmatched_dimensions: List[Dict[str, Any]] = field(default_factory=list)
    format_version: str = "2.0-result-scope"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "format_version": self.format_version,
            "status": self.status,
            "has_differences": self.has_differences,
            "databases": {k: v.to_dict() for k, v in self.databases.items()},
            "databases_with_issues": self.databases_with_issues,
            "databases_clean": self.databases_clean,
            "databases_incomplete": self.databases_incomplete,
            "total_checks": self.total_checks,
            "total_passed": self.total_passed,
            "total_issues": self.total_issues,
            "scope": self.scope,
            "coverage": self.coverage,
            "observation_window": self.observation_window,
            "unmatched_dimensions": self.unmatched_dimensions,
        }


def _db_name_of(scope_str: str) -> str:
    s = str(scope_str or "").strip()
    if not s:
        return "global"
    parts = s.split()
    return parts[0].split(".")[0] if parts else "global"


def _assess_impact(category: str, check: str, detail: str, is_artifact: bool) -> str:
    if is_artifact:
        return "Replication engine leftover; benign for application queries but blocks clean schema validation."
    cat = (category or "").lower()
    chk = (check or "").lower()
    det = (detail or "").lower()
    if "barrier" in det or "barrier" in chk or "foreign key" in det:
        return "Foreign key dependency ordering violated; concurrent writes may fail constraint checks."
    if "drift" in det or "drift" in chk or "bidirectional" in det or "bidirectional" in chk:
        return "Live writes diverted or lost between active database nodes; data convergence violated."
    if "autoinc" in chk or "autoinc" in cat or "sequence" in det:
        return "Target sequence/autoinc is behind source; cutover will produce duplicate primary key errors."
    if "schema" in chk or "schema" in cat:
        return "Schema or object mismatch; queries targeting missing columns, constraints, or tables will fail."
    if "data" in chk or "counts" in chk:
        return "Row data differs; business operations will observe missing or corrupt records."
    return "Verification finding requires operator attention before cutover."


def _suggest_next_action(check: str, status: str, fix_hint: str, is_artifact: bool) -> str:
    if is_artifact:
        return "Isolate or ignore replication table from application repair commands."
    if fix_hint:
        return fix_hint
    chk = (check or "").lower()
    if "autoinc" in chk:
        return "Advance target sequence/auto-increment value beyond source maximum."
    if "counts" in chk or "data" in chk:
        return "Execute repair sync on affected tables (`migkit repair`)."
    if "schema" in chk:
        return "Apply missing DDL constraints or indexes on target."
    return "Inspect detailed evidence and rerun check."


def format_unmatched_dimension(finding: Dict[str, Any]) -> Dict[str, Any]:
    dim_key = finding.get("dimension") or "bidirectional_drift_proof"
    spec = UNMATCHED_DIMENSIONS.get(dim_key, {
        "name": dim_key.replace("_", " ").title(),
        "barrier_type": "verified_barrier",
        "description": "A proof beyond row comparison.",
    })
    return {
        "dimension": dim_key,
        "name": spec.get("name", dim_key),
        "barrier_type": spec.get("barrier_type", "verified_barrier"),
        "database": finding.get("database", "global"),
        "affected_object": finding.get("affected_object") or finding.get("scope", "global"),
        "short_problem": finding.get("short_problem") or finding.get("detail", "Unmatched invariant check"),
        "source_state": str(finding.get("source_state", "")),
        "target_state": str(finding.get("target_state", "")),
        "impact": finding.get("impact") or _assess_impact("unmatched", dim_key, finding.get("detail", ""), False),
        "verdict_certainty": finding.get("verdict_certainty", spec.get("barrier_type", "verified_barrier")),
        "next_action": finding.get("next_action") or "Review dimension barrier requirements before cutover.",
        "barrier_status": finding.get("barrier_status", "passed" if finding.get("status") in ("ok", "same") else "violated"),
    }


def build_grouped_summary(
    records: List[Dict[str, Any]],
    hop: Any = None,
    coverage: Optional[Dict[str, Any]] = None,
    observation_window: Optional[Dict[str, Any]] = None,
) -> GroupedResultEnvelope:
    from . import leftovers, verdict

    db_records: Dict[str, List[Dict[str, Any]]] = {}
    for r in records:
        db = _db_name_of(r.get("scope", ""))
        db_records.setdefault(db, []).append(r)

    db_summaries: Dict[str, DatabaseSummary] = {}
    databases_with_issues: List[str] = []
    databases_clean: List[str] = []
    databases_incomplete: List[str] = []
    all_unmatched_findings: List[Dict[str, Any]] = []

    total_checks = 0
    total_passed = 0
    total_issues = 0

    all_checks_observed = sorted({r.get("check") for r in records if r.get("check")})
    if coverage and coverage.get("checks"):
        planned_checks = coverage["checks"]
    elif coverage and coverage.get("only"):
        planned_checks = coverage["only"]
    else:
        planned_checks = all_checks_observed or CHECK_ORDER

    for db, recs in db_records.items():
        checks_completed_set = {r.get("check") for r in recs if r.get("check")}
        checks_completed = len(checks_completed_set)
        checks_planned = len(set(planned_checks))

        problems: List[PracticalProblem] = []
        remaining_checks = sorted(set(planned_checks) - checks_completed_set)
        cat_counts: Dict[str, int] = {}
        unmatched_in_db: List[Dict[str, Any]] = []

        db_has_diff = False
        db_has_error = False
        db_incomplete = bool(remaining_checks)

        for r in recs:
            st = r.get("status", "unknown")
            cat = r.get("category", "data")
            chk = r.get("check", "unknown")
            sc = str(r.get("scope", ""))
            det = str(r.get("detail", ""))
            total_checks += 1

            dim_key = r.get("dimension")
            if not dim_key:
                for k in UNMATCHED_DIMENSIONS:
                    if k in chk.lower() or k in cat.lower() or k in det.lower():
                        dim_key = k
                        break

            if dim_key:
                norm_unmatched = format_unmatched_dimension({
                    "dimension": dim_key,
                    "database": db,
                    "affected_object": sc,
                    "short_problem": det,
                    "status": st,
                    "source_state": r.get("source_state", ""),
                    "target_state": r.get("target_state", ""),
                    "detail": det,
                })
                unmatched_in_db.append(norm_unmatched)
                all_unmatched_findings.append(norm_unmatched)

            if st in ("ok", "same"):
                total_passed += 1
                continue

            if st == "diff":
                db_has_diff = True
            elif st == "error":
                db_has_error = True
            elif st in ("skip", "warn", "incomplete"):
                db_incomplete = True

            total_issues += 1
            cat_counts[cat] = cat_counts.get(cat, 0) + 1

            target_ident = sc.replace("/", ".").split()[-1] if sc else ""
            tbl = target_ident.split(".")[-1] if target_ident else ""
            sch = target_ident.split(".")[-2] if len(target_ident.split(".")) > 1 else None
            origin = leftovers.artifact_origin(tbl, sch)
            is_artifact = bool(origin)

            prob = PracticalProblem(
                object_name=sc or db,
                problem=det or f"{chk} {st}",
                source_state=str(r.get("source_state", "")),
                target_state=str(r.get("target_state", "")),
                impact=_assess_impact(cat, chk, det, is_artifact),
                defect_class="replication_artifact" if is_artifact else "application_defect",
                is_replication_artifact=is_artifact,
                artifact_origin=origin or "",
                category=cat,
                next_action=_suggest_next_action(chk, st, r.get("fix_hint", ""), is_artifact),
                check=chk,
                status=st,
                severity="error" if st not in ("diff", "warn", "skip") else st,
            )
            problems.append(prob)

        if db_has_error:
            db_status = "error"
            databases_with_issues.append(db)
        elif db_has_diff:
            db_status = "different"
            databases_with_issues.append(db)
        elif db_incomplete:
            db_status = "incomplete"
            databases_incomplete.append(db)
        else:
            db_status = "same"
            databases_clean.append(db)

        db_summaries[db] = DatabaseSummary(
            database=db,
            status=db_status,
            checks_completed=checks_completed,
            checks_planned=checks_planned,
            issues_count=len(problems),
            practical_problems=problems,
            remaining_work=remaining_checks,
            categories=cat_counts,
            unmatched_findings=unmatched_in_db,
        )

    if any(s.status == "error" for s in db_summaries.values()):
        overall_status = "error"
    elif any(s.status == "different" for s in db_summaries.values()):
        overall_status = "different"
    elif any(s.status == "incomplete" for s in db_summaries.values()):
        overall_status = "incomplete"
    else:
        overall_status = "same"

    hop_scope_data = {}
    if hop is not None:
        try:
            hop_scope_data = verdict.hop_scope(hop)
        except Exception:
            hop_scope_data = {}

    return GroupedResultEnvelope(
        status=overall_status,
        has_differences=bool(databases_with_issues),
        databases=db_summaries,
        databases_with_issues=databases_with_issues,
        databases_clean=databases_clean,
        databases_incomplete=databases_incomplete,
        total_checks=total_checks,
        total_passed=total_passed,
        total_issues=total_issues,
        scope=hop_scope_data,
        coverage=coverage or {},
        observation_window=observation_window or {},
        unmatched_dimensions=all_unmatched_findings,
    )


def format_console(envelope: GroupedResultEnvelope) -> str:
    lines = []
    lines.append("=" * 80)
    lines.append(f"MIGRATION RESULT SUMMARY: {envelope.status.upper()} "
                 f"({envelope.total_passed}/{envelope.total_checks} checks passed, {envelope.total_issues} issues)")
    lines.append("=" * 80)

    lines.append(f"{'DATABASE':<20} {'CHECKS':<12} {'STATUS':<14} {'ISSUES':<8} {'PRACTICAL SUMMARY'}")
    lines.append("-" * 80)

    for db_name, summary in envelope.databases.items():
        checks_str = f"{summary.checks_completed}/{summary.checks_planned}"
        if summary.practical_problems:
            first_prob = summary.practical_problems[0]
            desc = first_prob.problem
            if first_prob.is_replication_artifact:
                desc = f"[{first_prob.artifact_origin} artifact] {desc}"
        elif summary.remaining_work:
            desc = f"Remaining checks: {', '.join(summary.remaining_work)}"
        else:
            desc = "All checks verified clean"
        if len(desc) > 35:
            desc = desc[:32] + "..."
        lines.append(f"{db_name:<20} {checks_str:<12} {summary.status.upper():<14} {summary.issues_count:<8} {desc}")

    if envelope.databases_clean:
        lines.append("")
        lines.append(f"Clean databases ({len(envelope.databases_clean)}): {', '.join(envelope.databases_clean)}")

    if envelope.databases_with_issues:
        lines.append("")
        lines.append("-" * 80)
        lines.append("ACTIONABLE ISSUES & IMPACT:")
        for db_name in envelope.databases_with_issues:
            summary = envelope.databases[db_name]
            lines.append(f"Database: {db_name} [{summary.status.upper()}]")
            for p in summary.practical_problems:
                badge = f" ({p.artifact_origin} artifact)" if p.is_replication_artifact else ""
                lines.append(f"  * [{p.severity.upper()}] {p.object_name}{badge}: {p.problem}")
                if p.impact:
                    lines.append(f"    Impact: {p.impact}")
                if p.next_action:
                    lines.append(f"    Action: {p.next_action}")

    if envelope.unmatched_dimensions:
        lines.append("")
        lines.append("-" * 80)
        lines.append("PROOFS BEYOND ROW COMPARISON:")
        for u in envelope.unmatched_dimensions:
            lines.append(f"  * {u['name']} [{u['barrier_status'].upper()}]: {u['database']} - {u['affected_object']}")
            lines.append(f"    Barrier Type: {u['barrier_type']} | Certainty: {u['verdict_certainty']}")
            lines.append(f"    Impact: {u['impact']}")
            lines.append(f"    Action: {u['next_action']}")

    lines.append("=" * 80)
    return "\n".join(lines)


def format_html(envelope: GroupedResultEnvelope) -> str:
    db_rows = []
    for db_name, summary in envelope.databases.items():
        status_class = "ok" if summary.status == "same" else ("diff" if summary.status == "different" else "error")
        checks_str = f"{summary.checks_completed}/{summary.checks_planned}"
        if summary.practical_problems:
            first_p = summary.practical_problems[0]
            summary_desc = html.escape(first_p.problem)
            if first_p.is_replication_artifact:
                summary_desc = f'<span class="fix" style="font-size:11px;margin:0 4px 0 0;">{html.escape(first_p.artifact_origin)} artifact</span>' + summary_desc
        elif summary.remaining_work:
            summary_desc = f"Remaining: {html.escape(', '.join(summary.remaining_work))}"
        else:
            summary_desc = '<span style="color:#0ca30c;">All checks verified clean</span>'

        db_rows.append(
            f'<tr><td class="db"><strong>{html.escape(db_name)}</strong></td>'
            f'<td>{checks_str}</td>'
            f'<td><span class="chip {status_class}"><span class="dot {status_class}"></span>{summary.status.upper()}</span></td>'
            f'<td>{summary.issues_count}</td>'
            f'<td>{summary_desc}</td></tr>'
        )

    unmatched_cards = []
    for u in envelope.unmatched_dimensions:
        status_color = "#0ca30c" if u["barrier_status"] == "passed" else "#d03b3b"
        unmatched_cards.append(
            f'<div class="finding" style="border-left-color:{status_color};">'
            f'<div class="head"><strong>{html.escape(u["name"])}</strong> &middot; '
            f'<span style="font-family:ui-monospace,monospace;font-size:12px;">{html.escape(u["database"])} / {html.escape(u["affected_object"])}</span>'
            f'<span class="fix" style="margin:0 0 0 8px;padding:2px 6px;font-size:11px;">{html.escape(u["barrier_type"])}</span></div>'
            f'<div class="what">{html.escape(u["short_problem"])}</div>'
            f'<div style="font-size:12.5px;color:#52514e;margin-top:6px;"><strong>Impact:</strong> {html.escape(u["impact"])}</div>'
            f'<div class="fix" style="margin-top:6px;"><strong>Next action:</strong> {html.escape(u["next_action"])}</div>'
            f'</div>'
        )

    unmatched_section = ""
    if unmatched_cards:
        unmatched_section = (
            f'<div class="card"><h2>proofs beyond row comparison</h2>'
            f'<div class="sub" style="margin-bottom:12px;">Invariants proved across the two sides, each with its own verdict.</div>'
            f'{"".join(unmatched_cards)}</div>'
        )

    return f"""<div class="card"><h2>database summary (database-first grouping)</h2>
<table><thead><tr><th>database</th><th>checks (comp/plan)</th><th>status</th><th>issues</th><th>practical summary</th></tr></thead>
<tbody>{"".join(db_rows)}</tbody></table></div>
{unmatched_section}"""
