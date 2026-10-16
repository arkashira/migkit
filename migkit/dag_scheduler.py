"""Dynamic Transaction DAG Scheduler with Barrier Splitting (Item 566, SUPER.dag-barrier)."""
from collections import deque
import dataclasses
import enum
import itertools
import threading
import time
from typing import Any, Callable, Dict, Iterable, List, Optional, Set, Tuple, Union


class BarrierType(str, enum.Enum):
    CASCADE = "cascade"
    DDL = "ddl"
    SEQUENCE = "sequence"
    CUTOVER = "cutover"
    CUSTOM = "custom"


@dataclasses.dataclass
class BarrierFence:
    lsn: int
    barrier_type: Union[BarrierType, str] = BarrierType.CUSTOM
    table: Optional[str] = None
    action: Optional[Callable[[], Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    fence_id: str = ""

    def __post_init__(self):
        if not self.fence_id:
            self.fence_id = f"fence-{self.lsn}-{time.time_ns()}"
        if isinstance(self.barrier_type, str):
            try:
                self.barrier_type = BarrierType(self.barrier_type.lower())
            except ValueError:
                self.barrier_type = BarrierType.CUSTOM

    def execute(self) -> Any:
        if self.action:
            return self.action()
        return None


@dataclasses.dataclass
class CDCMutation:
    table: str
    op: str
    lsn: int
    key: Optional[Dict[str, Any]] = None
    values: Optional[Dict[str, Any]] = None
    old_values: Optional[Dict[str, Any]] = None
    tx_id: Optional[Union[int, str]] = None
    is_cascade: bool = False
    is_ddl: bool = False
    is_sequence_reset: bool = False
    is_cutover_mark: bool = False
    action: Optional[Callable[[], Any]] = None
    metadata: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "CDCMutation":
        op = str(d.get("op", "insert")).lower()
        is_ddl = bool(d.get("is_ddl", False) or op in ("ddl", "schema"))
        is_cascade = bool(d.get("is_cascade", False) or op in ("cascade", "cascade_delete", "cascade_update"))
        is_seq = bool(d.get("is_sequence_reset", False) or op in ("sequence_reset", "sequence_jump", "sequence"))
        is_cutover = bool(d.get("is_cutover_mark", False) or op in ("cutover_mark", "cutover", "mark", "drain_mark"))
        return cls(
            table=str(d.get("table", "")),
            op=op,
            lsn=int(d.get("lsn", 0)),
            key=d.get("key"),
            values=d.get("values"),
            old_values=d.get("old_values"),
            tx_id=d.get("tx_id") or d.get("txn"),
            is_cascade=is_cascade,
            is_ddl=is_ddl,
            is_sequence_reset=is_seq,
            is_cutover_mark=is_cutover,
            action=d.get("action"),
            metadata=d.get("metadata") or {},
        )


@dataclasses.dataclass
class Transaction:
    tx_id: Union[int, str]
    commit_lsn: int
    mutations: List[CDCMutation]
    metadata: Optional[Dict[str, Any]] = None


@dataclasses.dataclass
class ReplayStats:
    total_mutations: int = 0
    total_transactions: int = 0
    barriers_encountered: int = 0
    committed_lsn: int = -1
    worker_counts: Dict[int, int] = dataclasses.field(default_factory=dict)
    work_stolen_count: int = 0
    execution_order: List[Tuple[int, str, str]] = dataclasses.field(default_factory=list)
    commit_order: List[int] = dataclasses.field(default_factory=list)
    barrier_history: List[Dict[str, Any]] = dataclasses.field(default_factory=list)


class TableDependencyGraph:
    def __init__(self, foreign_keys: Optional[Iterable[Any]] = None):
        self._parents: Dict[str, Set[str]] = {}
        self._children: Dict[str, Set[str]] = {}
        self._tables: Set[str] = set()
        if foreign_keys:
            if isinstance(foreign_keys, dict):
                for child, parents in foreign_keys.items():
                    if isinstance(parents, (list, tuple, set)):
                        for p in parents:
                            self.add_foreign_key((child, p))
                    else:
                        self.add_foreign_key((child, parents))
            else:
                for fk in foreign_keys:
                    self.add_foreign_key(fk)

    def add_foreign_key(self, fk: Any) -> None:
        if isinstance(fk, (tuple, list)):
            if len(fk) >= 4:
                child, parent = str(fk[0]), str(fk[2])
            elif len(fk) >= 2:
                child, parent = str(fk[0]), str(fk[1])
            else:
                return
        elif isinstance(fk, dict):
            child = str(fk.get("child") or fk.get("table", ""))
            parent = str(fk.get("parent") or fk.get("references", ""))
        else:
            return

        if not child or not parent or child == parent:
            if child:
                self._tables.add(child)
            if parent:
                self._tables.add(parent)
            return

        self._tables.add(child)
        self._tables.add(parent)
        self._parents.setdefault(child, set()).add(parent)
        self._children.setdefault(parent, set()).add(child)

    def get_parents(self, table: str) -> Set[str]:
        return set(self._parents.get(table, set()))

    def get_children(self, table: str) -> Set[str]:
        return set(self._children.get(table, set()))

    def all_tables(self) -> Set[str]:
        return set(self._tables)

    def has_dependency(self, child: str, parent: str) -> bool:
        visited = set()
        queue = deque([child])
        while queue:
            curr = queue.popleft()
            if curr == parent:
                return True
            for p in self.get_parents(curr):
                if p not in visited:
                    visited.add(p)
                    queue.append(p)
        return False

    def connected_components(self) -> List[Set[str]]:
        visited = set()
        components = []
        for table in sorted(self._tables):
            if table not in visited:
                comp = set()
                queue = deque([table])
                visited.add(table)
                while queue:
                    curr = queue.popleft()
                    comp.add(curr)
                    neighbors = self.get_parents(curr) | self.get_children(curr)
                    for nbr in neighbors:
                        if nbr not in visited:
                            visited.add(nbr)
                            queue.append(nbr)
                components.append(comp)
        return components

    def topological_order(self, tables: Optional[Iterable[str]] = None, op: str = "insert") -> List[str]:
        subset = set(tables) if tables is not None else set(self._tables)
        if not subset:
            return []

        op_lower = op.lower()
        if op_lower == "delete":
            deps = {t: set(c for c in self.get_children(t) if c in subset) for t in subset}
            dependents = {t: set(p for p in self.get_parents(t) if p in subset) for t in subset}
        else:
            deps = {t: set(p for p in self.get_parents(t) if p in subset) for t in subset}
            dependents = {t: set(c for c in self.get_children(t) if c in subset) for t in subset}

        in_degree = {t: len(deps[t]) for t in subset}
        ready = sorted([t for t, d in in_degree.items() if d == 0])
        result = []

        while ready:
            curr = ready.pop(0)
            result.append(curr)
            for dep in dependents[curr]:
                in_degree[dep] -= 1
                if in_degree[dep] == 0:
                    ready.append(dep)
            ready.sort()

        if len(result) < len(subset):
            remaining = sorted([t for t in subset if t not in result])
            result.extend(remaining)

        return result

    def sort_mutations(self, mutations: List[CDCMutation]) -> List[CDCMutation]:
        if not mutations:
            return []

        tables_in_muts = set(m.table for m in mutations if m.table)

        deletes = [m for m in mutations if m.op == "delete"]
        inserts = [m for m in mutations if m.op == "insert"]
        others = [m for m in mutations if m.op not in ("delete", "insert")]

        del_order = self.topological_order(tables_in_muts, op="delete")
        del_ranks = {tbl: i for i, tbl in enumerate(del_order)}
        sorted_deletes = sorted(deletes, key=lambda m: del_ranks.get(m.table, 999999))

        ins_order = self.topological_order(tables_in_muts, op="insert")
        ins_ranks = {tbl: i for i, tbl in enumerate(ins_order)}
        sorted_inserts = sorted(inserts, key=lambda m: ins_ranks.get(m.table, 999999))

        sorted_others = sorted(others, key=lambda m: ins_ranks.get(m.table, 999999))

        return sorted_deletes + sorted_others + sorted_inserts


class TableLockManager:
    def __init__(self):
        self._locks: Dict[str, threading.Lock] = {}
        self._meta_lock = threading.Lock()

    def get_lock(self, table: Optional[str]) -> threading.Lock:
        if not table:
            return self._meta_lock
        with self._meta_lock:
            if table not in self._locks:
                self._locks[table] = threading.Lock()
            return self._locks[table]


class WorkStealingQueue:
    def __init__(self, queue_id: int):
        self.queue_id = queue_id
        self._deque: deque = deque()
        self._lock = threading.Lock()

    def push(self, item: Any) -> None:
        with self._lock:
            self._deque.append(item)

    def pop(self) -> Optional[Any]:
        with self._lock:
            if self._deque:
                return self._deque.popleft()
            return None

    def steal(self) -> Optional[Any]:
        with self._lock:
            if self._deque:
                return self._deque.pop()
            return None

    def steal_safe(
        self,
        can_execute_fn: Optional[Callable[..., bool]] = None,
        can_execute_table_fn: Optional[Callable[[str], bool]] = None,
    ) -> Optional[Any]:
        with self._lock:
            for idx in range(len(self._deque)):
                item = self._deque[idx]
                table = getattr(item, "table", "")
                if can_execute_table_fn is not None and not can_execute_table_fn(table):
                    continue
                if can_execute_fn is not None:
                    prior_items = list(itertools.islice(self._deque, idx))
                    try:
                        if not can_execute_fn(item, prior_items):
                            continue
                    except TypeError:
                        if not can_execute_fn(item):
                            continue
                del self._deque[idx]
                return item
            return None


    def peek(self) -> Optional[Any]:
        with self._lock:
            if self._deque:
                return self._deque[0]
            return None

    def __len__(self) -> int:
        with self._lock:
            return len(self._deque)

    def is_empty(self) -> bool:
        with self._lock:
            return len(self._deque) == 0


class CommitWatermarkTracker:
    def __init__(self, on_commit: Optional[Callable[[int], None]] = None):
        self._on_commit = on_commit
        self._pending_counts: Dict[int, int] = {}
        self._completed_counts: Dict[int, int] = {}
        self._expected_lsns: Set[int] = set()
        self._committed_lsn: int = -1
        self._lock = threading.Lock()
        self._condition = threading.Condition(self._lock)
        self._commit_log: List[int] = []

    def register_lsn(self, lsn: int, count: int = 1) -> None:
        with self._condition:
            self._pending_counts[lsn] = self._pending_counts.get(lsn, 0) + count
            self._expected_lsns.add(lsn)

    def mark_completed(self, lsn: int, count: int = 1) -> int:
        commits_fired = []
        with self._condition:
            self._completed_counts[lsn] = self._completed_counts.get(lsn, 0) + count
            while self._expected_lsns:
                lowest_lsn = min(self._expected_lsns)
                pending = self._pending_counts.get(lowest_lsn, 0)
                completed = self._completed_counts.get(lowest_lsn, 0)
                if completed >= pending and pending > 0:
                    self._expected_lsns.remove(lowest_lsn)
                    self._committed_lsn = lowest_lsn
                    self._commit_log.append(lowest_lsn)
                    commits_fired.append(lowest_lsn)
                else:
                    break
            self._condition.notify_all()

        for c_lsn in commits_fired:
            if self._on_commit:
                self._on_commit(c_lsn)

        return self._committed_lsn

    @property
    def committed_lsn(self) -> int:
        with self._lock:
            return self._committed_lsn

    @property
    def commit_log(self) -> List[int]:
        with self._lock:
            return list(self._commit_log)

    def is_drained(self) -> bool:
        with self._lock:
            return len(self._expected_lsns) == 0

    def drain_wait(self, target_lsn: Optional[int] = None, timeout: Optional[float] = None) -> bool:
        start_time = time.time()
        with self._condition:
            while True:
                if target_lsn is None:
                    if len(self._expected_lsns) == 0:
                        return True
                else:
                    if self._committed_lsn >= target_lsn:
                        return True
                    if not self._expected_lsns:
                        return True
                    if min(self._expected_lsns) > target_lsn:
                        return True

                if timeout is not None:
                    elapsed = time.time() - start_time
                    remaining = timeout - elapsed
                    if remaining <= 0:
                        return False
                    self._condition.wait(remaining)
                else:
                    self._condition.wait(0.01)


class TransactionDAGScheduler:
    def __init__(
        self,
        foreign_keys: Optional[Iterable[Any]] = None,
        num_workers: int = 4,
        on_commit: Optional[Callable[[int], None]] = None,
        apply_fn: Optional[Callable[[CDCMutation], Any]] = None,
    ):
        self.num_workers = max(1, int(num_workers))
        self.foreign_keys = foreign_keys
        self.dependency_graph = TableDependencyGraph(foreign_keys)
        self.on_commit = on_commit
        self.apply_fn = apply_fn

        self._queues: List[WorkStealingQueue] = [WorkStealingQueue(i) for i in range(self.num_workers)]
        self._watermark = CommitWatermarkTracker(on_commit=self._handle_watermark_commit)
        self._table_locks = TableLockManager()
        self._active_tables: Set[str] = set()
        self._active_tables_lock = threading.Lock()

        self._stats = ReplayStats()
        self._workers: List[threading.Thread] = []
        self._stopped = False

        self._barrier_in_progress = threading.Event()
        self._barrier_lock = threading.RLock()

        self._rebuild_partition_map()

    def _rebuild_partition_map(self) -> None:
        self._component_map: Dict[str, str] = {}
        comps = self.dependency_graph.connected_components()
        for comp in comps:
            comp_id = sorted(comp)[0]
            for tbl in comp:
                self._component_map[tbl] = comp_id

    def _handle_watermark_commit(self, lsn: int) -> None:
        if self.on_commit:
            self.on_commit(lsn)

    def partition_for_table(self, table: str) -> int:
        if not table:
            return 0
        comp_id = self._component_map.get(table, table)
        return abs(hash(comp_id)) % self.num_workers

    def get_partitions(self) -> List[Set[str]]:
        partitions = [set() for _ in range(self.num_workers)]
        for tbl in self.dependency_graph.all_tables():
            idx = self.partition_for_table(tbl)
            partitions[idx].add(tbl)
        return partitions

    @classmethod
    def detect_barrier(cls, event: Any) -> Optional[BarrierFence]:
        if isinstance(event, BarrierFence):
            return event

        if isinstance(event, CDCMutation):
            if event.is_cascade:
                return BarrierFence(lsn=event.lsn, barrier_type=BarrierType.CASCADE, table=event.table, action=event.action, metadata=event.metadata)
            if event.is_ddl or event.op in ("ddl", "schema"):
                return BarrierFence(lsn=event.lsn, barrier_type=BarrierType.DDL, table=event.table, action=event.action, metadata=event.metadata)
            if event.is_sequence_reset or event.op in ("sequence_reset", "sequence_jump", "sequence"):
                return BarrierFence(lsn=event.lsn, barrier_type=BarrierType.SEQUENCE, table=event.table, action=event.action, metadata=event.metadata)
            if event.is_cutover_mark or event.op in ("cutover_mark", "cutover", "mark", "drain_mark"):
                return BarrierFence(lsn=event.lsn, barrier_type=BarrierType.CUTOVER, table=event.table, action=event.action, metadata=event.metadata)
            if event.op in ("barrier", "fence"):
                return BarrierFence(lsn=event.lsn, barrier_type=BarrierType.CUSTOM, table=event.table, action=event.action, metadata=event.metadata)
            return None

        if isinstance(event, dict):
            op = str(event.get("op", "")).lower()
            lsn = int(event.get("lsn", 0))
            table = event.get("table")
            action = event.get("action")
            meta = event.get("metadata") or {}
            if event.get("is_cascade") or op in ("cascade", "cascade_delete", "cascade_update"):
                return BarrierFence(lsn=lsn, barrier_type=BarrierType.CASCADE, table=table, action=action, metadata=meta)
            if event.get("is_ddl") or op in ("ddl", "schema"):
                return BarrierFence(lsn=lsn, barrier_type=BarrierType.DDL, table=table, action=action, metadata=meta)
            if event.get("is_sequence_reset") or op in ("sequence_reset", "sequence_jump", "sequence"):
                return BarrierFence(lsn=lsn, barrier_type=BarrierType.SEQUENCE, table=table, action=action, metadata=meta)
            if event.get("is_cutover_mark") or op in ("cutover_mark", "cutover", "mark", "drain_mark"):
                return BarrierFence(lsn=lsn, barrier_type=BarrierType.CUTOVER, table=table, action=action, metadata=meta)
            if op in ("barrier", "fence"):
                return BarrierFence(lsn=lsn, barrier_type=BarrierType.CUSTOM, table=table, action=action, metadata=meta)
            return None

        return None

    def _normalize_mutation(self, item: Any) -> CDCMutation:
        if isinstance(item, CDCMutation):
            return item
        if isinstance(item, dict):
            return CDCMutation.from_dict(item)
        raise TypeError(f"Cannot normalize mutation of type {type(item)}")

    def _drain_worker_queues(self, timeout: float = 30.0) -> bool:
        start_t = time.time()
        while time.time() - start_t < timeout:
            all_empty = all(q.is_empty() for q in self._queues)
            with self._active_tables_lock:
                no_active = len(self._active_tables) == 0
            if all_empty and no_active:
                return True
            time.sleep(0.001)
        return False

    def _process_barrier(self, barrier: BarrierFence) -> None:
        self._drain_worker_queues(timeout=30.0)

        if barrier.lsn > 0:
            self._watermark.drain_wait(target_lsn=barrier.lsn - 1, timeout=30.0)
        else:
            self._watermark.drain_wait(timeout=30.0)

        self._barrier_in_progress.set()
        try:
            action_result = barrier.execute()
        finally:
            self._barrier_in_progress.clear()

        if barrier.lsn > 0:
            self._watermark.register_lsn(barrier.lsn, 1)
            self._watermark.mark_completed(barrier.lsn, 1)

        self._stats.barriers_encountered += 1
        self._stats.barrier_history.append({
            "fence_id": barrier.fence_id,
            "lsn": barrier.lsn,
            "type": str(barrier.barrier_type),
            "table": barrier.table,
            "result": action_result,
            "ts": time.time(),
        })

    def add_barrier(self, barrier: Union[BarrierFence, Dict[str, Any]]) -> None:
        if isinstance(barrier, dict):
            barrier = BarrierFence(
                lsn=int(barrier.get("lsn", 0)),
                barrier_type=barrier.get("barrier_type", BarrierType.CUSTOM),
                table=barrier.get("table"),
                action=barrier.get("action"),
                metadata=barrier.get("metadata") or {},
                fence_id=barrier.get("fence_id", ""),
            )
        with self._barrier_lock:
            self._process_barrier(barrier)

    def add_mutation(self, mutation: Union[CDCMutation, Dict[str, Any]]) -> None:
        barrier = self.detect_barrier(mutation)
        if barrier:
            self.add_barrier(barrier)
            return

        with self._barrier_lock:
            mut = self._normalize_mutation(mutation)
            self._watermark.register_lsn(mut.lsn, count=1)
            self._stats.total_mutations += 1

            part_idx = self.partition_for_table(mut.table)
            self._queues[part_idx].push(mut)

    def add_transaction(self, txn: Union[Transaction, Dict[str, Any], List[Any]]) -> None:
        if isinstance(txn, Transaction):
            mutations = txn.mutations
            tx_id = txn.tx_id
        elif isinstance(txn, dict):
            raw_muts = txn.get("mutations") or txn.get("changes") or []
            mutations = [self._normalize_mutation(m) for m in raw_muts]
            tx_id = txn.get("tx_id") or txn.get("txn")
        elif isinstance(txn, (list, tuple)):
            mutations = [self._normalize_mutation(m) for m in txn]
            tx_id = None
        else:
            raise TypeError(f"Unsupported transaction type: {type(txn)}")

        self._stats.total_transactions += 1

        for m in mutations:
            if tx_id is not None and m.tx_id is None:
                m.tx_id = tx_id

        sorted_muts = self.dependency_graph.sort_mutations(mutations)

        with self._barrier_lock:
            for mut in sorted_muts:
                barrier = self.detect_barrier(mut)
                if barrier:
                    self._process_barrier(barrier)
                else:
                    self._watermark.register_lsn(mut.lsn, count=1)
                    self._stats.total_mutations += 1
                    part_idx = self.partition_for_table(mut.table)
                    self._queues[part_idx].push(mut)

    def feed(self, events: Iterable[Any]) -> None:
        for event in events:
            if isinstance(event, (Transaction, list)):
                self.add_transaction(event)
            elif isinstance(event, dict) and ("mutations" in event or "changes" in event):
                self.add_transaction(event)
            elif isinstance(event, BarrierFence):
                self.add_barrier(event)
            else:
                barrier = self.detect_barrier(event)
                if barrier:
                    self.add_barrier(barrier)
                else:
                    self.add_mutation(event)

    def _can_execute_task(self, task: Any, queue_items_before: Optional[List[Any]] = None) -> bool:
        table = getattr(task, "table", "")
        if not table:
            return True

        op = getattr(task, "op", "insert").lower()

        with self._active_tables_lock:
            if table in self._active_tables:
                return False

            if op == "delete":
                children = self.dependency_graph.get_children(table)
                if any(c in self._active_tables for c in children):
                    return False
            else:
                parents = self.dependency_graph.get_parents(table)
                if any(p in self._active_tables for p in parents):
                    return False

        if queue_items_before:
            for prior in queue_items_before:
                prior_tbl = getattr(prior, "table", "")
                if prior_tbl == table:
                    return False
                prior_lsn = getattr(prior, "lsn", 0)
                task_lsn = getattr(task, "lsn", 0)
                if prior_lsn < task_lsn and self._component_map.get(prior_tbl) == self._component_map.get(table):
                    return False

            if op == "delete":
                children = self.dependency_graph.get_children(table)
                for prior in queue_items_before:
                    prior_tbl = getattr(prior, "table", "")
                    if prior_tbl in children:
                        return False
            else:
                parents = self.dependency_graph.get_parents(table)
                for prior in queue_items_before:
                    prior_tbl = getattr(prior, "table", "")
                    if prior_tbl in parents:
                        return False

        return True


    def _worker_loop(self, worker_id: int) -> None:
        my_queue = self._queues[worker_id]
        while not self._stopped:
            if self._barrier_in_progress.is_set():
                time.sleep(0.001)
                continue

            task = None
            stolen = False

            if not my_queue.is_empty():
                candidate = my_queue.peek()
                if candidate is not None and self._can_execute_task(candidate):
                    task = my_queue.pop()

            if task is None:
                for other_id in range(self.num_workers):
                    if other_id == worker_id:
                        continue
                    stolen_task = self._queues[other_id].steal_safe(self._can_execute_task)
                    if stolen_task is not None:
                        task = stolen_task
                        stolen = True
                        break

            if task is None:
                time.sleep(0.001)
                continue

            table = getattr(task, "table", "")
            with self._active_tables_lock:
                if table:
                    self._active_tables.add(table)

            lock = self._table_locks.get_lock(table)
            try:
                with lock:
                    if stolen:
                        self._stats.work_stolen_count += 1

                    self._stats.worker_counts[worker_id] = self._stats.worker_counts.get(worker_id, 0) + 1
                    self._stats.execution_order.append((task.lsn, task.table, task.op))

                    if task.action:
                        task.action()
                    elif self.apply_fn:
                        self.apply_fn(task)

                    self._watermark.mark_completed(task.lsn, count=1)
            finally:
                with self._active_tables_lock:
                    if table:
                        self._active_tables.discard(table)

    def start(self) -> None:
        if self._workers:
            return
        self._stopped = False
        for i in range(self.num_workers):
            t = threading.Thread(target=self._worker_loop, args=(i,), daemon=True, name=f"dag-worker-{i}")
            self._workers.append(t)
            t.start()

    def stop(self) -> None:
        self._stopped = True
        for t in self._workers:
            t.join(timeout=1.0)
        self._workers.clear()

    def drain(self, timeout: Optional[float] = 30.0) -> bool:
        start_t = time.time()
        ok_queues = self._drain_worker_queues(timeout=timeout or 30.0)
        if not ok_queues:
            return False
        elapsed = time.time() - start_t
        rem = (timeout - elapsed) if timeout is not None else None
        return self._watermark.drain_wait(timeout=rem)

    def replay(self, events: Iterable[Any], timeout: Optional[float] = 30.0) -> ReplayStats:
        self.start()
        try:
            self.feed(events)
            self.drain(timeout=timeout)
            self._stats.committed_lsn = self._watermark.committed_lsn
            self._stats.commit_order = self._watermark.commit_log
            return self._stats
        finally:
            self.stop()

    @property
    def stats(self) -> ReplayStats:
        self._stats.committed_lsn = self._watermark.committed_lsn
        self._stats.commit_order = self._watermark.commit_log
        return self._stats
