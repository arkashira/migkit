ALIASES = {"mariadb": "mysql", "percona": "mysql", "aurora-mysql": "mysql",
           "aurora-postgres": "postgres", "alloydb": "postgres",
           "documentdb": "mongodb", "cosmosdb-mongo": "mongodb",
           "azure-sql": "mssql", "tdsql": "mysql",
           "elasticsearch": "opensearch", "scylladb": "cassandra",
           "sybase": "ase", "sap-ase": "ase"}


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
    if name == "hetero":
        from .hetero import HeteroEngine
        return HeteroEngine
    return None


#: every canonical engine name, in the order a report lists them
NAMES = ("postgres", "mysql", "mongodb", "mssql", "redis", "kafka", "sqlite",
         "parquet", "clickhouse", "dynamodb", "oracle", "db2", "ase",
         "opensearch", "cassandra", "redshift", "snowflake", "bigquery",
         "kinesis", "pubsub", "hetero", "generic")


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
    return engine_named(hop.engine, hop)
