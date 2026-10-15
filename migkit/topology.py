"""Multi-node topologies: many-to-one, one-to-many, and full mesh (R3.topo)."""
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple


class TopologyType(str, Enum):
    MANY_TO_ONE = "many_to_one"
    ONE_TO_MANY = "one_to_many"
    FULL_MESH = "full_mesh"


@dataclass
class TopologyNode:
    id: str
    endpoint: str
    role: str = "peer"  # "source", "target", "peer"
    shard_prefix: str = ""
    region: str = ""
    tags: Dict[str, str] = field(default_factory=dict)


@dataclass
class TopologyLeg:
    id: str
    source_id: str
    target_id: str
    state: str = "active"  # "idle", "active", "paused", "converged"
    lag_seconds: float = 0.0
    watermark: str = ""
    tables: List[str] = field(default_factory=list)
    origin_tag: str = ""


@dataclass
class ChangeEvent:
    event_id: str
    origin_id: str
    table: str
    key: str
    data: Dict[str, Any] = field(default_factory=dict)
    visited: List[str] = field(default_factory=list)


@dataclass
class Topology:
    id: str
    topo_type: TopologyType
    nodes: Dict[str, TopologyNode] = field(default_factory=dict)
    legs: Dict[str, TopologyLeg] = field(default_factory=dict)
    conflict_resolution: str = "lww"  # "lww", "priority", "vector_clock"

    def add_node(self, node: TopologyNode) -> None:
        self.nodes[node.id] = node

    def add_leg(self, leg: TopologyLeg) -> None:
        self.legs[leg.id] = leg

    def validate(self) -> Tuple[bool, List[str]]:
        errors = []
        if not self.nodes:
            errors.append("topology contains no nodes")
            return False, errors

        for leg in self.legs.values():
            if leg.source_id not in self.nodes:
                errors.append(f"leg {leg.id} references missing source node {leg.source_id}")
            if leg.target_id not in self.nodes:
                errors.append(f"leg {leg.id} references missing target node {leg.target_id}")

        if self.topo_type == TopologyType.MANY_TO_ONE:
            prefixes: Set[str] = set()
            for n_id, n in self.nodes.items():
                if n.role == "source":
                    if not n.shard_prefix:
                        errors.append(f"source node {n_id} lacks shard_prefix required for collision-free fan-in")
                    elif n.shard_prefix in prefixes:
                        errors.append(f"duplicate shard_prefix '{n.shard_prefix}' on source {n_id}")
                    else:
                        prefixes.add(n.shard_prefix)

        elif self.topo_type == TopologyType.FULL_MESH:
            n_count = len(self.nodes)
            expected_legs = n_count * (n_count - 1)
            if len(self.legs) < expected_legs:
                errors.append(f"full mesh requires {expected_legs} directional legs for {n_count} nodes, found {len(self.legs)}")

        return len(errors) == 0, errors

    def route_change(self, event: ChangeEvent, current_node_id: str) -> List[str]:
        """Calculates next forward hops enforcing strict loop suppression."""
        if current_node_id not in event.visited:
            event.visited.append(current_node_id)

        next_targets = []
        for leg in self.legs.values():
            if leg.source_id == current_node_id and leg.state == "active":
                target = leg.target_id
                if target == event.origin_id or target in event.visited:
                    continue
                next_targets.append(target)
        return next_targets

    def is_converged(self, max_allowed_lag_sec: float = 1.0) -> Tuple[bool, float]:
        if not self.legs:
            return True, 0.0
        max_lag = max(leg.lag_seconds for leg in self.legs.values())
        return max_lag <= max_allowed_lag_sec, max_lag


def many_to_one(sources: List[Dict[str, Any]], target: Dict[str, Any],
                topo_id: str = "topo-many-to-one") -> Topology:
    topo = Topology(id=topo_id, topo_type=TopologyType.MANY_TO_ONE)
    t_id = target.get("id", "target")
    topo.add_node(TopologyNode(
        id=t_id, endpoint=target.get("endpoint", ""), role="target", region=target.get("region", "")
    ))

    for idx, s in enumerate(sources, 1):
        s_id = s.get("id", f"src_{idx}")
        prefix = s.get("shard_prefix", f"s{idx}_")
        topo.add_node(TopologyNode(
            id=s_id, endpoint=s.get("endpoint", ""), role="source",
            shard_prefix=prefix, region=s.get("region", "")
        ))
        topo.add_leg(TopologyLeg(
            id=f"leg_{s_id}->{t_id}", source_id=s_id, target_id=t_id,
            origin_tag=s_id, tables=s.get("tables", [])
        ))
    return topo


def one_to_many(source: Dict[str, Any], targets: List[Dict[str, Any]],
                topo_id: str = "topo-one-to-many") -> Topology:
    topo = Topology(id=topo_id, topo_type=TopologyType.ONE_TO_MANY)
    s_id = source.get("id", "source")
    topo.add_node(TopologyNode(
        id=s_id, endpoint=source.get("endpoint", ""), role="source", region=source.get("region", "")
    ))

    for idx, t in enumerate(targets, 1):
        t_id = t.get("id", f"dst_{idx}")
        topo.add_node(TopologyNode(
            id=t_id, endpoint=t.get("endpoint", ""), role="target", region=t.get("region", "")
        ))
        topo.add_leg(TopologyLeg(
            id=f"leg_{s_id}->{t_id}", source_id=s_id, target_id=t_id,
            origin_tag=s_id, tables=t.get("tables", [])
        ))
    return topo


def full_mesh(nodes: List[Dict[str, Any]], topo_id: str = "topo-mesh",
              conflict_resolution: str = "lww") -> Topology:
    topo = Topology(id=topo_id, topo_type=TopologyType.FULL_MESH,
                    conflict_resolution=conflict_resolution)
    for n in nodes:
        n_id = n.get("id")
        topo.add_node(TopologyNode(
            id=n_id, endpoint=n.get("endpoint", ""), role="peer", region=n.get("region", "")
        ))

    node_ids = list(topo.nodes.keys())
    for s_id in node_ids:
        for t_id in node_ids:
            if s_id != t_id:
                topo.add_leg(TopologyLeg(
                    id=f"leg_{s_id}->{t_id}", source_id=s_id, target_id=t_id,
                    origin_tag=s_id
                ))
    return topo
