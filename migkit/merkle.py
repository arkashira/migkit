"""Hierarchical in-database recursive Merkle tree folding (SUPER.merkle-fold)."""
import hashlib
import math
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

EMPTY_LEAF_HASH = hashlib.sha256(b"").digest()


def normalize_dialect(dialect: str) -> str:
    d = (dialect or "").strip().lower()
    if d in ("postgres", "postgresql", "pgsql"):
        return "postgres"
    if d in ("mysql", "mariadb"):
        return "mysql"
    if d in ("sqlite", "sqlite3"):
        return "sqlite"
    return d


def row_hash_sql(
    dialect: str,
    columns: Sequence[str],
    hash_alg: str = "sha256",
    salt: str = "",
    table_alias: Optional[str] = None,
) -> str:
    d = normalize_dialect(dialect)
    alg = hash_alg.lower()
    prefix = f"{table_alias}." if table_alias else ""
    cols = list(columns) if columns else ["*"]

    if d == "postgres":
        coalesced = [f"coalesce({prefix}{c}::text, '')" for c in cols]
        salt_part = f"'{salt}', " if salt else ""
        text_expr = f"concat_ws('|', {salt_part}{', '.join(coalesced)})"
        if alg == "md5":
            return f"md5({text_expr})"
        return f"encode(sha256(convert_to({text_expr}, 'UTF8')), 'hex')"

    if d == "mysql":
        coalesced = [f"coalesce(cast({prefix}{c} as char), '')" for c in cols]
        salt_part = f"'{salt}', " if salt else ""
        text_expr = f"concat_ws('|', {salt_part}{', '.join(coalesced)})"
        if alg == "md5":
            return f"md5({text_expr})"
        return f"sha2({text_expr}, 256)"

    if d == "sqlite":
        parts = []
        if salt:
            parts.append(f"'{salt}'")
        for c in cols:
            parts.append(f"coalesce(cast({prefix}{c} as text), '')")
        text_expr = " || '|' || ".join(parts) if parts else "''"
        if alg == "md5":
            return f"md5({text_expr})"
        return f"sha256({text_expr})"

    raise ValueError(f"unsupported dialect: {dialect}")


def bucket_expr(
    dialect: str,
    num_buckets: int = 64,
    key_col: Optional[str] = None,
    min_key: Optional[int] = None,
    max_key: Optional[int] = None,
    keyless_mode: str = "modulo_hash",
    columns: Optional[Sequence[str]] = None,
    hash_alg: str = "sha256",
    salt: str = "",
) -> str:
    d = normalize_dialect(dialect)
    b = int(num_buckets)

    if key_col is not None:
        if min_key is not None and max_key is not None:
            if d == "postgres":
                return (
                    f"(case when {max_key} <= {min_key} then 0 else "
                    f"least({b - 1}, greatest(0, floor((cast({key_col} as double precision) - {min_key}) * "
                    f"{b} / ({max_key} - {min_key} + 1))))::int end)"
                )
            if d == "mysql":
                return (
                    f"(case when {max_key} <= {min_key} then 0 else "
                    f"least({b - 1}, greatest(0, floor((cast({key_col} as double) - {min_key}) * "
                    f"{b} / ({max_key} - {min_key} + 1)))) end)"
                )
            if d == "sqlite":
                return (
                    f"(case when {max_key} <= {min_key} then 0 else "
                    f"min({b - 1}, max(0, cast((cast({key_col} as real) - {min_key}) * "
                    f"{b} / ({max_key} - {min_key} + 1) as integer))) end)"
                )
        if d == "postgres":
            return f"(abs(cast({key_col} as bigint)) % {b})"
        if d == "mysql":
            return f"(abs(cast({key_col} as signed)) % {b})"
        if d == "sqlite":
            return f"(abs(cast({key_col} as integer)) % {b})"

    mode = keyless_mode.lower()
    if mode == "ctid" and d == "postgres":
        return f"(abs(hashtext(ctid::text)) % {b})"
    if mode == "rowid" and d == "sqlite":
        return f"(abs(rowid) % {b})"

    rh = row_hash_sql(d, columns or ["*"], hash_alg=hash_alg, salt=salt)
    if d == "postgres":
        return f"(abs(('x' || substr({rh}, 1, 8))::bit(32)::bigint) % {b})"
    if d == "mysql":
        return f"(conv(substr({rh}, 1, 8), 16, 10) % {b})"
    if d == "sqlite":
        return f"((unicode(substr({rh}, 1, 1)) * 256 + unicode(substr({rh}, 2, 1))) % {b})"

    raise ValueError(f"unsupported dialect for keyless bucket: {dialect}")


def bucket_hash_agg_sql(
    dialect: str,
    row_hash: str,
    hash_alg: str = "sha256",
    order_by: Optional[str] = None,
) -> str:
    d = normalize_dialect(dialect)
    alg = hash_alg.lower()
    order_part = f" ORDER BY {order_by}" if order_by else ""

    if d == "postgres":
        if alg == "md5":
            return f"coalesce(md5(string_agg({row_hash}, ''{order_part})), md5(''))"
        return (
            f"coalesce(encode(sha256(convert_to(string_agg({row_hash}, ''{order_part}), 'UTF8')), 'hex'), "
            f"encode(sha256(convert_to('', 'UTF8')), 'hex'))"
        )

    if d == "mysql":
        if alg == "md5":
            return f"coalesce(md5(group_concat({row_hash}{order_part} separator '')), md5(''))"
        sep_order = f"{order_part} separator ''" if order_part else " separator ''"
        return f"coalesce(sha2(group_concat({row_hash}{sep_order}), 256), sha2('', 256))"

    if d == "sqlite":
        if alg == "md5":
            return f"coalesce(md5(group_concat({row_hash}, '')), md5(''))"
        return f"coalesce(sha256(group_concat({row_hash}, '')), sha256(''))"

    raise ValueError(f"unsupported dialect for aggregate: {dialect}")


def leaf_buckets_query(
    dialect: str,
    table: str,
    columns: Sequence[str],
    num_buckets: int = 64,
    key_col: Optional[str] = None,
    min_key: Optional[int] = None,
    max_key: Optional[int] = None,
    keyless_mode: str = "modulo_hash",
    hash_alg: str = "sha256",
    salt: str = "",
    where: Optional[str] = None,
) -> str:
    d = normalize_dialect(dialect)
    b_expr = bucket_expr(
        d,
        num_buckets=num_buckets,
        key_col=key_col,
        min_key=min_key,
        max_key=max_key,
        keyless_mode=keyless_mode,
        columns=columns,
        hash_alg=hash_alg,
        salt=salt,
    )
    rh = row_hash_sql(d, columns, hash_alg=hash_alg, salt=salt)
    order_col = key_col if key_col else rh
    b_hash = bucket_hash_agg_sql(d, rh, hash_alg=hash_alg, order_by=order_col)
    where_part = f" WHERE {where}" if where else ""

    return (
        f"SELECT {b_expr} AS bucket_id, "
        f"COUNT(*) AS row_count, "
        f"{b_hash} AS bucket_hash "
        f"FROM {table}{where_part} "
        f"GROUP BY {b_expr} "
        f"ORDER BY bucket_id"
    )


def hierarchical_fold_sql(
    dialect: str,
    table: str,
    columns: Sequence[str],
    num_buckets: int = 64,
    key_col: Optional[str] = None,
    min_key: Optional[int] = None,
    max_key: Optional[int] = None,
    keyless_mode: str = "modulo_hash",
    hash_alg: str = "sha256",
    salt: str = "",
    where: Optional[str] = None,
) -> str:
    d = normalize_dialect(dialect)
    p = 1
    while p < num_buckets:
        p <<= 1
    num_buckets = p
    depth = int(math.log2(num_buckets))

    b_expr = bucket_expr(
        d,
        num_buckets=num_buckets,
        key_col=key_col,
        min_key=min_key,
        max_key=max_key,
        keyless_mode=keyless_mode,
        columns=columns,
        hash_alg=hash_alg,
        salt=salt,
    )
    rh = row_hash_sql(d, columns, hash_alg=hash_alg, salt=salt)
    order_col = key_col if key_col else rh
    leaf_hash_agg = bucket_hash_agg_sql(d, rh, hash_alg=hash_alg, order_by=order_col)
    where_part = f" WHERE {where}" if where else ""

    empty_hash = hashlib.sha256(b"").hexdigest() if hash_alg.lower() == "sha256" else hashlib.md5(b"").hexdigest()

    def combine_expr():
        if d == "postgres":
            if hash_alg.lower() == "md5":
                return "coalesce(md5(string_agg(node_hash, '' ORDER BY bucket_id)), md5(''))"
            return (
                "coalesce(encode(sha256(convert_to(string_agg(node_hash, '' ORDER BY bucket_id), 'UTF8')), 'hex'), "
                "encode(sha256(convert_to('', 'UTF8')), 'hex'))"
            )
        if d == "mysql":
            if hash_alg.lower() == "md5":
                return "coalesce(md5(group_concat(node_hash ORDER BY bucket_id separator '')), md5(''))"
            return "coalesce(sha2(group_concat(node_hash ORDER BY bucket_id separator ''), 256), sha2('', 256))"
        if d == "sqlite":
            if hash_alg.lower() == "md5":
                return "coalesce(md5(group_concat(node_hash, '')), md5(''))"
            return "coalesce(sha256(group_concat(node_hash, '')), sha256(''))"
        raise ValueError(f"unsupported dialect: {dialect}")

    levels_cte = []
    levels_cte.append(
        f"bucket_seq(b) AS (\n"
        f"    SELECT 0\n"
        f"    UNION ALL\n"
        f"    SELECT b + 1 FROM bucket_seq WHERE b < {num_buckets - 1}\n"
        f")"
    )
    levels_cte.append(
        f"dense_level_0 AS (\n"
        f"    SELECT\n"
        f"        s.b AS bucket_id,\n"
        f"        COALESCE(l.row_count, 0) AS row_count,\n"
        f"        COALESCE(l.node_hash, '{empty_hash}') AS node_hash\n"
        f"    FROM bucket_seq s\n"
        f"    LEFT JOIN (\n"
        f"        SELECT\n"
        f"            {b_expr} AS bucket_id,\n"
        f"            COUNT(*) AS row_count,\n"
        f"            {leaf_hash_agg} AS node_hash\n"
        f"        FROM {table}{where_part}\n"
        f"        GROUP BY {b_expr}\n"
        f"    ) l ON s.b = l.bucket_id\n"
        f")"
    )

    prev = "dense_level_0"
    for lvl in range(1, depth + 1):
        target_name = "merkle_root" if lvl == depth else f"level_{lvl}"
        order_wrap = f"(SELECT * FROM {prev} ORDER BY bucket_id)" if d == "sqlite" else prev
        levels_cte.append(
            f"{target_name} AS (\n"
            f"    SELECT\n"
            f"        bucket_id / 2 AS bucket_id,\n"
            f"        SUM(row_count) AS row_count,\n"
            f"        {combine_expr()} AS node_hash\n"
            f"    FROM {order_wrap}\n"
            f"    GROUP BY bucket_id / 2\n"
            f")"
        )
        prev = target_name

    cte_body = ",\n".join(levels_cte)
    return f"WITH RECURSIVE\n{cte_body}\nSELECT bucket_id, row_count, node_hash AS root_hash FROM merkle_root;"


@dataclass
class DiscordantRange:
    bucket_id: int
    source_hash: str
    target_hash: str
    source_count: int
    target_count: int
    range_min: Any = None
    range_max: Any = None
    key_column: Optional[str] = None
    is_keyless: bool = False
    sub_ranges: List["DiscordantRange"] = field(default_factory=list)


@dataclass
class MerkleNode:
    level: int
    index: int
    hash_bytes: bytes
    row_count: int
    bucket_id: Optional[int] = None
    range_min: Any = None
    range_max: Any = None
    left_child: Optional["MerkleNode"] = None
    right_child: Optional["MerkleNode"] = None

    @property
    def hash_hex(self) -> str:
        return self.hash_bytes.hex()

    @property
    def is_leaf(self) -> bool:
        return self.level == 0


@dataclass
class MerkleTree:
    root: MerkleNode
    levels: List[List[MerkleNode]]
    leaves: List[MerkleNode]
    num_buckets: int
    depth: int
    hash_alg: str
    key_column: Optional[str] = None

    @property
    def root_bytes(self) -> bytes:
        return self.root.hash_bytes

    @property
    def root_hex(self) -> str:
        return self.root.hash_hex


class MerkleTreeFolder:
    def __init__(
        self,
        dialect: str = "postgres",
        hash_alg: str = "sha256",
        num_buckets: int = 64,
        key_column: Optional[str] = None,
        min_key: Optional[int] = None,
        max_key: Optional[int] = None,
        keyless_mode: str = "modulo_hash",
        salt: str = "",
    ):
        self.dialect = normalize_dialect(dialect)
        self.hash_alg = hash_alg.lower()
        p = 1
        while p < max(2, num_buckets):
            p <<= 1
        self.num_buckets = p
        self.depth = int(math.log2(self.num_buckets))
        self.key_column = key_column
        self.min_key = min_key
        self.max_key = max_key
        self.keyless_mode = keyless_mode
        self.salt = salt
        self.tree: Optional[MerkleTree] = None
        self.root_bytes: Optional[bytes] = None
        self.root_hex: Optional[str] = None

    def row_hash_expr(self, columns: Sequence[str], table_alias: Optional[str] = None) -> str:
        return row_hash_sql(self.dialect, columns, hash_alg=self.hash_alg, salt=self.salt, table_alias=table_alias)

    def bucket_expr(self, columns: Optional[Sequence[str]] = None) -> str:
        return bucket_expr(
            self.dialect,
            num_buckets=self.num_buckets,
            key_col=self.key_column,
            min_key=self.min_key,
            max_key=self.max_key,
            keyless_mode=self.keyless_mode,
            columns=columns,
            hash_alg=self.hash_alg,
            salt=self.salt,
        )

    def leaf_query(self, table: str, columns: Sequence[str], where: Optional[str] = None) -> str:
        return leaf_buckets_query(
            dialect=self.dialect,
            table=table,
            columns=columns,
            num_buckets=self.num_buckets,
            key_col=self.key_column,
            min_key=self.min_key,
            max_key=self.max_key,
            keyless_mode=self.keyless_mode,
            hash_alg=self.hash_alg,
            salt=self.salt,
            where=where,
        )

    def hierarchical_query(self, table: str, columns: Sequence[str], where: Optional[str] = None) -> str:
        return hierarchical_fold_sql(
            dialect=self.dialect,
            table=table,
            columns=columns,
            num_buckets=self.num_buckets,
            key_col=self.key_column,
            min_key=self.min_key,
            max_key=self.max_key,
            keyless_mode=self.keyless_mode,
            hash_alg=self.hash_alg,
            salt=self.salt,
            where=where,
        )

    def _bucket_range(self, idx: int) -> Tuple[Optional[int], Optional[int]]:
        if self.key_column is not None and self.min_key is not None and self.max_key is not None:
            span = self.max_key - self.min_key + 1
            step = span / self.num_buckets
            r_min = min(self.max_key, int(self.min_key + idx * step))
            r_max = (
                min(self.max_key, int(self.min_key + (idx + 1) * step - 1))
                if idx < self.num_buckets - 1
                else self.max_key
            )
            return r_min, max(r_min, r_max)
        return None, None

    def _to_32bytes(self, h: Union[str, bytes]) -> bytes:
        if isinstance(h, bytes):
            raw = h
        else:
            raw = bytes.fromhex(h)
        if len(raw) == 32:
            return raw
        return hashlib.sha256(raw).digest()

    def fold_bucket_hashes(
        self,
        bucket_data: Union[Sequence[Any], Dict[int, Any]],
    ) -> bytes:
        buckets: Dict[int, Tuple[int, bytes]] = {}
        if isinstance(bucket_data, dict):
            for k, val in bucket_data.items():
                b_id = int(k)
                if isinstance(val, (tuple, list)):
                    count = int(val[0])
                    h_val = self._to_32bytes(val[1])
                else:
                    count = 1
                    h_val = self._to_32bytes(val)
                buckets[b_id] = (count, h_val)
        else:
            for item in bucket_data:
                if isinstance(item, (tuple, list)):
                    if len(item) >= 3:
                        b_id = int(item[0])
                        count = int(item[1])
                        h_val = self._to_32bytes(item[2])
                    elif len(item) == 2:
                        b_id = int(item[0])
                        count = 1
                        h_val = self._to_32bytes(item[1])
                    else:
                        continue
                    buckets[b_id] = (count, h_val)
                elif isinstance(item, (str, bytes)):
                    b_id = len(buckets)
                    buckets[b_id] = (1, self._to_32bytes(item))

        leaf_nodes: List[MerkleNode] = []
        for i in range(self.num_buckets):
            r_min, r_max = self._bucket_range(i)
            if i in buckets:
                cnt, h_bytes = buckets[i]
            else:
                cnt = 0
                h_bytes = EMPTY_LEAF_HASH
            node = MerkleNode(
                level=0,
                index=i,
                hash_bytes=h_bytes,
                row_count=cnt,
                bucket_id=i,
                range_min=r_min,
                range_max=r_max,
            )
            leaf_nodes.append(node)

        levels: List[List[MerkleNode]] = [leaf_nodes]
        current = leaf_nodes
        for lvl in range(1, self.depth + 1):
            next_lvl: List[MerkleNode] = []
            for i in range(0, len(current), 2):
                left = current[i]
                right = current[i + 1] if i + 1 < len(current) else left
                combined_hash = hashlib.sha256(left.hash_bytes + right.hash_bytes).digest()
                p_min = left.range_min
                p_max = right.range_max if right else left.range_max
                parent = MerkleNode(
                    level=lvl,
                    index=i // 2,
                    hash_bytes=combined_hash,
                    row_count=left.row_count + right.row_count,
                    range_min=p_min,
                    range_max=p_max,
                    left_child=left,
                    right_child=right,
                )
                next_lvl.append(parent)
            levels.append(next_lvl)
            current = next_lvl

        root = levels[-1][0]
        self.tree = MerkleTree(
            root=root,
            levels=levels,
            leaves=leaf_nodes,
            num_buckets=self.num_buckets,
            depth=self.depth,
            hash_alg=self.hash_alg,
            key_column=self.key_column,
        )
        self.root_bytes = root.hash_bytes
        self.root_hex = root.hash_hex
        return self.root_bytes

    def compute_hierarchical_root(
        self,
        conn: Any,
        table: str,
        columns: Sequence[str],
        where: Optional[str] = None,
    ) -> Tuple[int, str]:
        query = self.hierarchical_query(table, columns, where=where)
        cursor = conn.cursor() if hasattr(conn, "cursor") else conn
        cursor.execute(query)
        row = cursor.fetchone()
        if row:
            return int(row[1]), str(row[2])
        return 0, ""

    def compute_from_db(
        self,
        conn: Any,
        table: str,
        columns: Sequence[str],
        where: Optional[str] = None,
    ) -> bytes:
        query = self.leaf_query(table, columns, where=where)
        cursor = conn.cursor() if hasattr(conn, "cursor") else conn
        cursor.execute(query)
        rows = cursor.fetchall()
        return self.fold_bucket_hashes(rows)

    def compute_root(
        self,
        conn: Any,
        table: str,
        columns: Sequence[str],
        where: Optional[str] = None,
    ) -> bytes:
        return self.compute_from_db(conn, table, columns, where=where)

    def find_discordant_ranges(
        self,
        other: Union["MerkleTreeFolder", MerkleTree],
    ) -> List[DiscordantRange]:
        other_tree = other.tree if isinstance(other, MerkleTreeFolder) else other
        if self.tree is None:
            raise ValueError("source tree not folded yet")
        if other_tree is None:
            raise ValueError("target tree not folded yet")
        return find_discordant_ranges(self.tree, other_tree)

    def bisect_discordant_range(
        self,
        source_conn: Any,
        target_conn: Any,
        table: str,
        discordant_range: DiscordantRange,
        columns: Sequence[str],
        target_dialect: Optional[str] = None,
        max_depth: int = 8,
    ) -> List[Any]:
        if discordant_range.is_keyless or discordant_range.range_min is None or discordant_range.range_max is None:
            return [discordant_range.bucket_id]

        r_min = discordant_range.range_min
        r_max = discordant_range.range_max
        key_col = discordant_range.key_column or self.key_column
        tgt_d = normalize_dialect(target_dialect or self.dialect)

        def _fetch_hash(conn, d_name, lo, hi):
            rh = row_hash_sql(d_name, columns, hash_alg=self.hash_alg, salt=self.salt)
            order_col = key_col
            h_agg = bucket_hash_agg_sql(d_name, rh, hash_alg=self.hash_alg, order_by=order_col)
            sql = (
                f"SELECT COUNT(*), {h_agg} FROM {table} "
                f"WHERE {key_col} >= {lo} AND {key_col} <= {hi}"
            )
            cur = conn.cursor() if hasattr(conn, "cursor") else conn
            cur.execute(sql)
            row = cur.fetchone()
            cnt = int(row[0]) if row and row[0] is not None else 0
            h_val = str(row[1]) if row and row[1] is not None else ""
            return cnt, h_val

        drifted_keys = []
        stack = [(r_min, r_max, 0)]
        while stack:
            lo, hi, cur_depth = stack.pop()
            if lo > hi:
                continue
            if lo == hi or cur_depth >= max_depth:
                s_cnt, s_h = _fetch_hash(source_conn, self.dialect, lo, hi)
                t_cnt, t_h = _fetch_hash(target_conn, tgt_d, lo, hi)
                if s_cnt != t_cnt or s_h != t_h:
                    drifted_keys.append((lo, hi))
                continue

            mid = (lo + hi) // 2
            s_cnt_l, s_h_l = _fetch_hash(source_conn, self.dialect, lo, mid)
            t_cnt_l, t_h_l = _fetch_hash(target_conn, tgt_d, lo, mid)
            if s_cnt_l != t_cnt_l or s_h_l != t_h_l:
                stack.append((lo, mid, cur_depth + 1))

            s_cnt_r, s_h_r = _fetch_hash(source_conn, self.dialect, mid + 1, hi)
            t_cnt_r, t_h_r = _fetch_hash(target_conn, tgt_d, mid + 1, hi)
            if s_cnt_r != t_cnt_r or s_h_r != t_h_r:
                stack.append((mid + 1, hi, cur_depth + 1))

        return drifted_keys


def find_discordant_ranges(
    source: Union[MerkleTreeFolder, MerkleTree],
    target: Union[MerkleTreeFolder, MerkleTree],
) -> List[DiscordantRange]:
    s_tree = source.tree if isinstance(source, MerkleTreeFolder) else source
    t_tree = target.tree if isinstance(target, MerkleTreeFolder) else target

    if s_tree is None or t_tree is None:
        raise ValueError("both source and target must have non-empty trees")

    if s_tree.root.hash_bytes == t_tree.root.hash_bytes:
        return []

    discordant: List[DiscordantRange] = []
    queue: List[Tuple[MerkleNode, MerkleNode]] = [(s_tree.root, t_tree.root)]

    while queue:
        s_node, t_node = queue.pop(0)
        if s_node.is_leaf:
            discordant.append(
                DiscordantRange(
                    bucket_id=s_node.bucket_id if s_node.bucket_id is not None else s_node.index,
                    source_hash=s_node.hash_hex,
                    target_hash=t_node.hash_hex,
                    source_count=s_node.row_count,
                    target_count=t_node.row_count,
                    range_min=s_node.range_min,
                    range_max=s_node.range_max,
                    key_column=s_tree.key_column or t_tree.key_column,
                    is_keyless=(s_node.range_min is None),
                )
            )
            continue

        if s_node.left_child and t_node.left_child:
            if s_node.left_child.hash_bytes != t_node.left_child.hash_bytes:
                queue.append((s_node.left_child, t_node.left_child))

        if s_node.right_child and t_node.right_child:
            if s_node.right_child.hash_bytes != t_node.right_child.hash_bytes:
                queue.append((s_node.right_child, t_node.right_child))

    return discordant


def register_sqlite_merkle_functions(conn: Any) -> None:
    def _md5(s):
        if s is None:
            return None
        return hashlib.md5(str(s).encode("utf-8")).hexdigest()

    def _sha256(s):
        if s is None:
            return None
        return hashlib.sha256(str(s).encode("utf-8")).hexdigest()

    def _sha2(s, bits=256):
        if s is None:
            return None
        return hashlib.sha256(str(s).encode("utf-8")).hexdigest()

    conn.create_function("md5", 1, _md5)
    conn.create_function("sha256", 1, _sha256)
    conn.create_function("sha2", 2, _sha2)
