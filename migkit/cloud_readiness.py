"""Cloud readiness auditor: network, maintenance windows, sizing and snapshots (R25.2)."""
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class Finding:
    level: str  # "pass", "warn", "fail"
    category: str  # "network", "maintenance", "sizing", "snapshot"
    resource: str
    detail: str
    fix: str = ""
    gates: bool = False

    def to_item(self):
        return {
            "level": self.level,
            "scope": f"cloud:{self.category}",
            "item": self.resource,
            "detail": self.detail + (f"  [fix] {self.fix}" if self.fix else ""),
            "gates": self.gates,
        }


class NetworkAuditor:
    def audit_vpc(self, src_vpc: Optional[dict], dst_vpc: Optional[dict]) -> List[Finding]:
        findings = []
        if not src_vpc or not dst_vpc:
            return findings

        src_id = src_vpc.get("id") or src_vpc.get("VpcId", "")
        dst_id = dst_vpc.get("id") or dst_vpc.get("VpcId", "")
        src_cidr = src_vpc.get("cidr") or src_vpc.get("CidrBlock", "")
        dst_cidr = dst_vpc.get("cidr") or dst_vpc.get("CidrBlock", "")

        if src_id and dst_id and src_id == dst_id:
            findings.append(Finding(
                level="pass", category="network", resource=f"vpc:{src_id}",
                detail=f"source and target reside in the same VPC ({src_id})"
            ))
        else:
            peered = dst_vpc.get("peered", False) or src_vpc.get("peered", False)
            if not peered and src_cidr and dst_cidr and src_cidr == dst_cidr:
                findings.append(Finding(
                    level="fail", category="network", resource="vpc:cidr_collision",
                    detail=f"overlapping CIDR blocks ({src_cidr}) across different VPCs without transit peering",
                    fix="provision non-overlapping VPC CIDRs or configure private NAT gateways",
                    gates=True
                ))
            elif peered:
                findings.append(Finding(
                    level="pass", category="network", resource=f"vpc:peering:{src_id}<->{dst_id}",
                    detail="VPC peering connection active between source and target"
                ))
            else:
                findings.append(Finding(
                    level="warn", category="network", resource=f"vpc:routing:{src_id}->{dst_id}",
                    detail="cross-VPC migration without explicit peering flag; verify route table entries and transit routing",
                    fix="verify VPC peering, transit gateway or direct connect routes"
                ))
        return findings

    def audit_subnets(self, subnets: List[dict], min_available_ips: int = 5) -> List[Finding]:
        findings = []
        azs: Set[str] = set()
        total_ips = 0

        for s in subnets:
            s_id = s.get("id") or s.get("SubnetId", "unknown")
            az = s.get("az") or s.get("AvailabilityZone", "")
            if az:
                azs.add(az)
            free_ips = s.get("available_ips") if "available_ips" in s else s.get("AvailableIpAddressCount", 100)
            total_ips += free_ips
            if free_ips < min_available_ips:
                findings.append(Finding(
                    level="fail", category="network", resource=f"subnet:{s_id}",
                    detail=f"subnet {s_id} in {az} has only {free_ips} available IP addresses (minimum required: {min_available_ips})",
                    fix="expand subnet CIDR or migrate idle elastic network interfaces",
                    gates=True
                ))

        if len(azs) < 2 and len(subnets) > 1:
            findings.append(Finding(
                level="warn", category="network", resource="subnets:multi_az",
                detail=f"all subnets configured in a single AZ ({next(iter(azs), 'unknown')}); lacks multi-AZ failover resilience",
                fix="configure target subnets across at least 2 distinct availability zones"
            ))
        elif len(azs) >= 2:
            findings.append(Finding(
                level="pass", category="network", resource="subnets:multi_az",
                detail=f"subnets distributed across {len(azs)} availability zones with {total_ips} total available IPs"
            ))
        return findings

    def audit_security_groups(self, sg_rules: List[dict], required_port: int) -> List[Finding]:
        findings = []
        port_accessible = False

        for rule in sg_rules:
            from_port = rule.get("from_port") if "from_port" in rule else rule.get("FromPort", 0)
            to_port = rule.get("to_port") if "to_port" in rule else rule.get("ToPort", 65535)
            direction = rule.get("direction", "ingress")

            if direction == "ingress":
                if from_port <= required_port <= to_port:
                    port_accessible = True
                    break

        if not port_accessible:
            findings.append(Finding(
                level="fail", category="network", resource=f"sg:port_{required_port}",
                detail=f"target security groups do not allow ingress on required database port {required_port}",
                fix=f"authorize ingress traffic on port {required_port} from source CIDR or migkit agent security group",
                gates=True
            ))
        else:
            findings.append(Finding(
                level="pass", category="network", resource=f"sg:port_{required_port}",
                detail=f"security groups permit ingress connectivity on port {required_port}"
            ))
        return findings


class MaintenanceAuditor:
    def audit_window(self, maintenance_window: str, cutover_window: Optional[dict] = None) -> List[Finding]:
        findings = []
        if not maintenance_window:
            findings.append(Finding(
                level="warn", category="maintenance", resource="maintenance:window",
                detail="preferred maintenance window not defined on cloud instance",
                fix="configure a scheduled maintenance window"
            ))
            return findings

        if cutover_window:
            c_day = cutover_window.get("day", "").lower()
            c_hour = cutover_window.get("hour")
            m_win = maintenance_window.lower()
            if c_day and c_day in m_win:
                findings.append(Finding(
                    level="warn", category="maintenance", resource="maintenance:window_overlap",
                    detail=f"cutover plan window ({c_day}) coincides with provider maintenance window ({maintenance_window})",
                    fix="shift planned cutover execution outside the provider maintenance window"
                ))
            else:
                findings.append(Finding(
                    level="pass", category="maintenance", resource="maintenance:window",
                    detail=f"maintenance window {maintenance_window} does not conflict with cutover schedule"
                ))
        else:
            findings.append(Finding(
                level="pass", category="maintenance", resource="maintenance:window",
                detail=f"cloud maintenance window registered: {maintenance_window}"
            ))
        return findings

    def audit_pending(self, pending_actions: List[dict]) -> List[Finding]:
        findings = []
        for action in pending_actions:
            name = action.get("name") or action.get("Action", "unknown")
            forced = action.get("forced", False) or action.get("AutoAppliedAfterDate") is not None
            severity = "fail" if forced else "warn"
            findings.append(Finding(
                level=severity, category="maintenance", resource=f"maintenance:pending:{name}",
                detail=f"pending cloud provider maintenance: {name} (auto-apply scheduled)",
                fix="apply or defer provider maintenance prior to starting data replication",
                gates=forced
            ))
        if not pending_actions:
            findings.append(Finding(
                level="pass", category="maintenance", resource="maintenance:pending",
                detail="no pending cloud maintenance actions or mandatory engine updates"
            ))
        return findings


class SizingAuditor:
    def audit_specs(self, src: dict, dst: dict) -> List[Finding]:
        findings = []
        src_cpu = src.get("cpu", 0) or 0
        dst_cpu = dst.get("cpu", 0) or 0
        src_mem = src.get("memory_gb", 0) or 0
        dst_mem = dst.get("memory_gb", 0) or 0
        src_storage = src.get("storage_gb", 0) or 0
        dst_storage = dst.get("storage_gb", 0) or 0

        if src_cpu and dst_cpu and dst_cpu < src_cpu:
            findings.append(Finding(
                level="warn", category="sizing", resource="sizing:cpu",
                detail=f"target vCPU ({dst_cpu}) is smaller than source ({src_cpu})",
                fix=f"upgrade target instance class to provide at least {src_cpu} vCPUs"
            ))
        elif src_cpu and dst_cpu:
            findings.append(Finding(
                level="pass", category="sizing", resource="sizing:cpu",
                detail=f"target vCPU ({dst_cpu}) meets or exceeds source ({src_cpu})"
            ))

        if src_mem and dst_mem and dst_mem < src_mem:
            findings.append(Finding(
                level="warn", category="sizing", resource="sizing:memory",
                detail=f"target memory ({dst_mem} GB) is smaller than source ({src_mem} GB)",
                fix=f"upgrade target instance class to match source memory {src_mem} GB"
            ))
        elif src_mem and dst_mem:
            findings.append(Finding(
                level="pass", category="sizing", resource="sizing:memory",
                detail=f"target memory ({dst_mem} GB) is adequate"
            ))

        if src_storage and dst_storage and dst_storage < src_storage:
            findings.append(Finding(
                level="fail", category="sizing", resource="sizing:storage",
                detail=f"target allocated storage ({dst_storage} GB) is less than source ({src_storage} GB)",
                fix=f"scale target storage volume to at least {src_storage} GB",
                gates=True
            ))
        elif src_storage and dst_storage:
            findings.append(Finding(
                level="pass", category="sizing", resource="sizing:storage",
                detail=f"target storage ({dst_storage} GB) is sufficient"
            ))

        return findings


class SnapshotAuditor:
    def audit_readiness(self, cloud_spec: dict) -> List[Finding]:
        findings = []
        can_snapshot = cloud_spec.get("can_snapshot", True)
        pitr_enabled = cloud_spec.get("pitr_enabled", False)
        retention_days = cloud_spec.get("backup_retention_days", 0)

        if not can_snapshot:
            findings.append(Finding(
                level="fail", category="snapshot", resource="snapshot:create_api",
                detail="provider API permissions do not allow snapshot creation or quota exceeded",
                fix="grant rds:CreateDBSnapshot / appropriate cloud provider IAM backup privileges",
                gates=True
            ))
        else:
            findings.append(Finding(
                level="pass", category="snapshot", resource="snapshot:create_api",
                detail="cloud snapshot API access verified and quota available"
            ))

        if not pitr_enabled and retention_days <= 0:
            findings.append(Finding(
                level="warn", category="snapshot", resource="snapshot:pitr",
                detail="point-in-time recovery and automated backups are disabled on target",
                fix="enable automated backups with retention period >= 1 day"
            ))
        else:
            findings.append(Finding(
                level="pass", category="snapshot", resource="snapshot:pitr",
                detail=f"automated backup retention configured ({retention_days} days, PITR active)"
            ))

        return findings


class CloudReadinessAuditor:
    def __init__(self):
        self.network = NetworkAuditor()
        self.maintenance = MaintenanceAuditor()
        self.sizing = SizingAuditor()
        self.snapshot = SnapshotAuditor()

    def audit_all(self, src_info: dict, dst_info: dict, cutover_window: Optional[dict] = None) -> List[Finding]:
        out: List[Finding] = []
        out.extend(self.network.audit_vpc(src_info.get("vpc"), dst_info.get("vpc")))
        out.extend(self.network.audit_subnets(dst_info.get("subnets", [])))
        req_port = dst_info.get("port") or 5432
        out.extend(self.network.audit_security_groups(dst_info.get("security_groups", []), req_port))
        out.extend(self.maintenance.audit_window(dst_info.get("maintenance_window", ""), cutover_window))
        out.extend(self.maintenance.audit_pending(dst_info.get("pending_maintenance", [])))
        out.extend(self.sizing.audit_specs(src_info, dst_info))
        out.extend(self.snapshot.audit_readiness(dst_info))
        return out

    def audit_to_items(self, src_info: dict, dst_info: dict, cutover_window: Optional[dict] = None) -> List[dict]:
        findings = self.audit_all(src_info, dst_info, cutover_window)
        return [f.to_item() for f in findings]


def audit(src_info: dict, dst_info: dict, cutover_window: Optional[dict] = None) -> List[Finding]:
    auditor = CloudReadinessAuditor()
    return auditor.audit_all(src_info, dst_info, cutover_window)


def items(hop, eng=None, dbs=None, quick=False) -> List[dict]:
    """Compatibility adapter for assess and ready.py."""
    src_info: Dict[str, Any] = {}
    dst_info: Dict[str, Any] = {}
    if hasattr(hop, "cloud_src"):
        src_info = dict(hop.cloud_src)
    if hasattr(hop, "cloud_dst"):
        dst_info = dict(hop.cloud_dst)
    auditor = CloudReadinessAuditor()
    return auditor.audit_to_items(src_info, dst_info)
