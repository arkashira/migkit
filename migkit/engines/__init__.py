ALIASES = {"mariadb": "mysql", "percona": "mysql", "aurora-mysql": "mysql",
           "aurora-postgres": "postgres", "alloydb": "postgres",
           "documentdb": "mongodb", "cosmosdb-mongo": "mongodb",
           "azure-sql": "mssql", "tdsql": "mysql",
           "elasticsearch": "opensearch", "scylladb": "cassandra",
           "sybase": "ase", "sap-ase": "ase", "vsam": "copybook",
           "cobol": "copybook", "ibmi": "db2i", "as400": "db2i",
           "iseries": "db2i", "db2-for-i": "db2i", "db2-zos": "db2zos",
           "db2z": "db2zos", "delta-lake": "delta",
           "cockroach": "cockroachdb", "crdb": "cockroachdb",
           "aurora-dsql": "dsql", "cosmos": "cosmosdb",
           "cosmosdb-nosql": "cosmosdb", "azure-cosmos": "cosmosdb"}

#: engines reached over another's wire protocol, whose values a driver
#: hands over, a table is built and a value written as that one's are
#: (`canon.dialect`): TiDB over MySQL's, CockroachDB and Aurora DSQL over
#: PostgreSQL's. Everything else - the catalogue, the change feed, what a
#: transaction may hold - is the engine's own
WIRE = {"tidb": "mysql", "cockroachdb": "postgres", "dsql": "postgres"}

#: an address's scheme and the engine it names - `source:
#: postgres://app@db1/shop` in a hop (`migkit.addresses`). The one place a
#: scheme is mapped: an engine added to `NAMES` without a scheme here is
#: one an address cannot name, which `tests/test_a_hop_from_two_addresses`
#: fails on. `generic` takes the address whole, as its own `url`.
SCHEMES = {
    "postgres": "postgres", "postgresql": "postgres", "pgsql": "postgres",
    "mysql": "mysql", "mariadb": "mysql",
    "mongodb": "mongodb", "mongodb+srv": "mongodb",
    "redis": "redis", "rediss": "redis", "valkey": "redis",
    "mssql": "mssql", "sqlserver": "mssql",
    "oracle": "oracle",
    "clickhouse": "clickhouse",
    "kafka": "kafka",
    "sqlite": "sqlite",
    "duckdb": "duckdb",
    "db2": "db2",
    "db2i": "db2i", "ibmi": "db2i", "as400": "db2i",
    "db2zos": "db2zos",
    "copybook": "copybook", "vsam": "copybook",
    "ase": "ase", "sybase": "ase",
    "opensearch": "opensearch", "elasticsearch": "opensearch",
    "cassandra": "cassandra", "scylla": "cassandra",
    "dynamodb": "dynamodb",
    "redshift": "redshift",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "kinesis": "kinesis",
    "pubsub": "pubsub",
    "parquet": "parquet", "parquet+s3": "parquet",
    "iceberg": "iceberg", "iceberg+http": "iceberg",
    "iceberg+https": "iceberg",
    "delta": "delta", "delta+s3": "delta",
    "trino": "generic", "presto": "generic", "vertica": "generic",
    "tidb": "tidb",
    "cockroachdb": "cockroachdb", "cockroach": "cockroachdb",
    "dsql": "dsql",
    "couchbase": "couchbase", "couchbases": "couchbase",
    "cosmosdb": "cosmosdb", "cosmos": "cosmosdb",
}


def _class_for(name):
    """The engine class for a canonical name, or None."""
    if name == "postgres":
        from .postgres import PostgresEngine
        return PostgresEngine
    if name == "mysql":
        from .mysql import MySQLEngine
        return MySQLEngine
    if name == "mongodb":
        from .mongodb import MongoEngine
        return MongoEngine
    if name == "mssql":
        from .mssql import MSSQLEngine
        return MSSQLEngine
    if name == "redis":
        from .redis import RedisEngine
        return RedisEngine
    if name == "kafka":
        from .kafka import KafkaEngine
        return KafkaEngine
    if name == "generic":
        from .generic import GenericEngine
        return GenericEngine
    if name == "sqlite":
        from .sqlite import SQLiteEngine
        return SQLiteEngine
    if name == "parquet":
        from .parquet import ParquetEngine
        return ParquetEngine
    if name == "clickhouse":
        from .clickhouse import ClickHouseEngine
        return ClickHouseEngine
    if name == "dynamodb":
        from .dynamodb import DynamoDBEngine
        return DynamoDBEngine
    if name == "oracle":
        from .oracle import OracleEngine
        return OracleEngine
    if name == "db2":
        from .db2 import Db2Engine
        return Db2Engine
    if name == "opensearch":
        from .opensearch import OpenSearchEngine
        return OpenSearchEngine
    if name == "cassandra":
        from .cassandra import CassandraEngine
        return CassandraEngine
    if name == "kinesis":
        from .kinesis import KinesisEngine
        return KinesisEngine
    if name == "pubsub":
        from .pubsub import PubSubEngine
        return PubSubEngine
    if name == "ase":
        from .ase import AseEngine
        return AseEngine
    if name == "redshift":
        from .warehouse import RedshiftEngine
        return RedshiftEngine
    if name == "snowflake":
        from .warehouse import SnowflakeEngine
        return SnowflakeEngine
    if name == "bigquery":
        from .warehouse import BigQueryEngine
        return BigQueryEngine
    if name == "duckdb":
        from .duckdb import DuckDBEngine
        return DuckDBEngine
    if name == "iceberg":
        from .lake import IcebergEngine
        return IcebergEngine
    if name == "delta":
        from .lake import DeltaEngine
        return DeltaEngine
    if name == "copybook":
        from .copybook import CopybookEngine
        return CopybookEngine
    if name == "db2i":
        from .db2i import Db2iEngine
        return Db2iEngine
    if name == "db2zos":
        from .db2zos import Db2ZosEngine
        return Db2ZosEngine
    if name == "tidb":
        from .tidb import TiDBEngine
        return TiDBEngine
    if name == "cockroachdb":
        from .cockroachdb import CockroachEngine
        return CockroachEngine
    if name == "dsql":
        from .dsql import DsqlEngine
        return DsqlEngine
    if name == "couchbase":
        from .couchbase import CouchbaseEngine
        return CouchbaseEngine
    if name == "cosmosdb":
        from .cosmosdb import CosmosEngine
        return CosmosEngine
    if name == "hetero":
        from .hetero import HeteroEngine
        return HeteroEngine
    return None


#: every canonical engine name, in the order a report lists them
NAMES = ("postgres", "mysql", "mongodb", "mssql", "redis", "kafka", "sqlite",
         "parquet", "clickhouse", "dynamodb", "oracle", "db2", "db2i",
         "db2zos", "ase",
         "opensearch", "cassandra", "redshift", "snowflake", "bigquery",
         "kinesis", "pubsub", "duckdb", "copybook", "iceberg", "delta",
         "tidb", "cockroachdb", "dsql", "couchbase", "cosmosdb",
         "hetero", "generic")


def engines_with(method):
    """Canonical names of the engines that implement `method`.

    So a message telling an operator which engines can do something is read
    off the engines. The hand-written version of this sentence named three
    engines while six had the method, which is the sort of thing nobody
    notices until they believe it.
    """
    out = []
    for name in NAMES:
        cls = _class_for(name)
        if cls is not None and getattr(cls, method, None) is not None:
            out.append(name)
    return out


def engine_named(name, hop):
    """Build the engine for `name` on this hop, whatever the hop's own engine.

    A cross-engine hop needs one driver per side, and both hang off the same
    hop - so the lookup cannot go through `hop.engine`. One table for both
    entry points: a second copy would be one rename away from a hop that
    resolves differently depending on which of the two was asked.
    """
    canonical = ALIASES.get(name, name)
    cls = _class_for(canonical)
    if cls is None:
        raise SystemExit(f"unsupported engine {name}")
    return cls(hop)


def get_engine(hop):
    # a server reached through a tunnel is reached through its open end
    from ..tunnel import through
    through(hop)
    return engine_named(hop.engine, hop)
