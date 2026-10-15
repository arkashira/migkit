"""Enterprise change sheet exporter mapping migration steps to task tickets with evidence IDs (R25.3)."""
import csv
from dataclasses import asdict, dataclass, field
import io
import json
import time
from typing import Any, Dict, List, Optional, Tuple


@dataclass
class ChangeTask:
    step_id: str
    ticket_id: str
    title: str
    phase: str  # "assess", "plan", "copy", "verify", "cutover", "postcheck", "rollback"
    assignee: str = "DevOps"
    status: str = "pending"  # "pending", "in_progress", "completed", "failed", "rolled_back"
    planned_duration_min: int = 15
    evidence_id: Optional[str] = None
    evidence_summary: str = ""
    gate: bool = False
    dependencies: List[str] = field(default_factory=list)
    started_at: Optional[float] = None
    completed_at: Optional[float] = None


@dataclass
class ChangeSheet:
    change_id: str
    title: str
    service: str
    environment: str = "production"
    cab_reference: str = ""
    tasks: List[ChangeTask] = field(default_factory=list)

    def add_task(self, task: ChangeTask) -> None:
        self.tasks.append(task)

    def get_task(self, identifier: str) -> Optional[ChangeTask]:
        for t in self.tasks:
            if t.step_id == identifier or t.ticket_id == identifier:
                return t
        return None

    def update_progress(self, identifier: str, status: str, evidence_id: Optional[str] = None,
                        summary: str = "") -> bool:
        t = self.get_task(identifier)
        if not t:
            return False
        t.status = status
        now = time.time()
        if status == "in_progress" and not t.started_at:
            t.started_at = now
        elif status in ("completed", "failed", "rolled_back"):
            t.completed_at = now
        if evidence_id:
            t.evidence_id = evidence_id
        if summary:
            t.evidence_summary = summary
        return True

    def is_ready_for_cutover(self) -> Tuple[bool, List[str]]:
        reasons = []
        for t in self.tasks:
            if t.phase in ("assess", "plan", "copy", "verify"):
                if t.status != "completed":
                    reasons.append(f"task '{t.ticket_id}' ({t.title}) is {t.status}, expected completed")
                elif not t.evidence_id:
                    reasons.append(f"task '{t.ticket_id}' ({t.title}) is completed but lacks evidence ID")
        return len(reasons) == 0, reasons

    def export_json(self) -> str:
        data = {
            "change_id": self.change_id,
            "title": self.title,
            "service": self.service,
            "environment": self.environment,
            "cab_reference": self.cab_reference,
            "tasks": [asdict(t) for t in self.tasks],
        }
        return json.dumps(data, indent=2)

    def export_markdown(self) -> str:
        lines = [
            f"# Enterprise Change Implementation Sheet: {self.change_id}",
            f"**Service:** {self.service} | **Environment:** {self.environment} | **CAB Ref:** {self.cab_reference or 'N/A'}",
            "",
            "| Ticket ID | Step | Phase | Status | Assignee | Duration | Evidence ID | Evidence Summary |",
            "| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |",
        ]
        for t in self.tasks:
            ev_id = f"`{t.evidence_id}`" if t.evidence_id else "-"
            ev_sum = t.evidence_summary.replace("|", "/") if t.evidence_summary else "-"
            lines.append(
                f"| **{t.ticket_id}** | {t.title} | {t.phase} | `{t.status}` | {t.assignee} | "
                f"{t.planned_duration_min}m | {ev_id} | {ev_sum} |"
            )
        return "\n".join(lines)

    def export_csv(self) -> str:
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow([
            "change_id", "ticket_id", "step_id", "title", "phase", "status",
            "assignee", "planned_duration_min", "evidence_id", "evidence_summary"
        ])
        for t in self.tasks:
            writer.writerow([
                self.change_id, t.ticket_id, t.step_id, t.title, t.phase, t.status,
                t.assignee, t.planned_duration_min, t.evidence_id or "", t.evidence_summary or ""
            ])
        return buf.getvalue()


def create_from_plan(hop_name: str, plan_steps: Optional[List[dict]] = None,
                     ticket_prefix: str = "CHG-MIG") -> ChangeSheet:
    sheet = ChangeSheet(
        change_id=f"{ticket_prefix}-{int(time.time())}",
        title=f"Database Migration Plan for Hop {hop_name}",
        service=hop_name,
    )
    default_steps = [
        {"step_id": "assess", "title": "Pre-flight cloud & schema readiness assessment", "phase": "assess", "duration": 15, "gate": True},
        {"step_id": "plan", "title": "Generate and ratify joint execution plan", "phase": "plan", "duration": 10, "gate": True},
        {"step_id": "copy", "title": "Execute baseline snapshot and bulk table copy", "phase": "copy", "duration": 60, "gate": True},
        {"step_id": "stream", "title": "Start change data capture (CDC) and catch up lag", "phase": "copy", "duration": 30, "gate": False},
        {"step_id": "verify", "title": "Data parity hash verification across partitions", "phase": "verify", "duration": 20, "gate": True},
        {"step_id": "cutover", "title": "Quiesce source, drain stream and cut over traffic", "phase": "cutover", "duration": 15, "gate": True},
        {"step_id": "postcheck", "title": "Post-cutover application sanity and health check", "phase": "postcheck", "duration": 15, "gate": False},
        {"step_id": "rollback", "title": "Contingency rollback to source instance", "phase": "rollback", "duration": 30, "gate": False},
    ]

    steps_to_use = plan_steps if plan_steps else default_steps
    for idx, s in enumerate(steps_to_use, 1):
        task = ChangeTask(
            step_id=s.get("step_id", f"step_{idx}"),
            ticket_id=f"{ticket_prefix}-{idx:02d}",
            title=s.get("title", f"Migration step {idx}"),
            phase=s.get("phase", "copy"),
            assignee=s.get("assignee", "DevOps"),
            planned_duration_min=s.get("duration", 15),
            gate=s.get("gate", False),
            dependencies=s.get("dependencies", []),
        )
        sheet.add_task(task)
    return sheet
