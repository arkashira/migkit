#: every canonical engine name, in the order a report lists them
NAMES = ("postgres", "mysql", "mongodb", "mssql", "redis", "kafka", "sqlite",
         "parquet", "clickhouse", "dynamodb", "oracle", "db2", "db2i",
         "db2zos", "ase", "teradata",
         "opensearch", "cassandra", "redshift", "snowflake", "bigquery",
         "kinesis", "pubsub", "duckdb", "copybook", "iceberg", "delta",
         "tidb", "cockroachdb", "dsql", "couchbase", "cosmosdb",
         "sapodp", "saas", "hetero", "generic")

#: every other name an engine goes by - a brand on its wire (`mariadb`,
#: `aurora-postgres`), a vendor's or a protocol's spelling (`postgresql`,
#: `sqlserver`, `valkey`) - and the engine it is: the one table `engine:`
#: in a hop and an address's scheme are both read through (`canonical`).
#: Measured before it: 14 names were taken only as a scheme and 16 only as
#: `engine:`, so `engine: postgresql` was refused while `postgresql://`
#: was read (audit 2026-10-01, DU6)
ALIASES = {"mariadb": "mysql", "percona": "mysql", "aurora-mysql": "mysql",
           "aurora-postgres": "postgres", "alloydb": "postgres",
           "postgresql": "postgres", "pgsql": "postgres",
           "documentdb": "mongodb", "cosmosdb-mongo": "mongodb",
           "azure-sql": "mssql", "sqlserver": "mssql", "tdsql": "mysql",
           "valkey": "redis",
           "elasticsearch": "opensearch", "scylladb": "cassandra",
           "scylla": "cassandra",
           "sybase": "ase", "sap-ase": "ase", "vsam": "copybook",
           "cobol": "copybook", "ibmi": "db2i", "as400": "db2i",
           "iseries": "db2i", "db2-for-i": "db2i", "db2-zos": "db2zos",
           "db2z": "db2zos", "delta-lake": "delta",
           "cockroach": "cockroachdb", "crdb": "cockroachdb",
           "aurora-dsql": "dsql", "cosmos": "cosmosdb",
           "cosmosdb-nosql": "cosmosdb", "azure-cosmos": "cosmosdb",
           "td": "teradata", "vantage": "teradata",
           "sap-odp": "sapodp", "odp": "sapodp", "program": "saas"}

#: engines reached over another's wire protocol, whose values a driver
#: hands over, a table is built and a value written as that one's are
#: (`canon.dialect`): TiDB over MySQL's, CockroachDB and Aurora DSQL over
#: PostgreSQL's. Everything else - the catalogue, the change feed, what a
#: transaction may hold - is the engine's own
WIRE = {"tidb": "mysql", "cockroachdb": "postgres", "dsql": "postgres"}

#: what an address's scheme says beyond the engine's name: the way it is
#: reached (`mongodb+srv`, `rediss`, a place on S3, an SAP sandbox over
#: plain HTTP) or, for a program source, its kind (`engines.saas`) - and
#: the engines spoken to through the address whole (`generic`)
SCHEME_ONLY = {
    "mongodb+srv": "mongodb", "rediss": "redis",
    "parquet+s3": "parquet", "iceberg+http": "iceberg",
    "iceberg+https": "iceberg", "delta+s3": "delta",
    # an SAP system's OData services, https or - a sandbox's - http
    "sapodp+http": "sapodp",
    "couchbases": "couchbase",
    "dlt": "saas", "singer": "saas", "airbyte": "saas",
    "salesforce": "saas",
    "trino": "generic", "presto": "generic", "vertica": "generic",
}

#: engines an address names by a word of its own rather than the engine's
#: name: a program source's scheme is its kind, `generic` takes the
#: address whole as its `url`, `hetero` is a pair
_NO_SCHEME = ("saas", "generic", "hetero")

#: an address's scheme and the engine it names - `source:
#: postgres://app@db1/shop` in a hop (`migkit.addresses`): every engine's
#: own name, every other name it goes by (`ALIASES`) and `SCHEME_ONLY` -
#: so a word `engine:` takes is a scheme too. The engine's own name comes
#: first, the one `addresses.render` writes back; an engine added to
#: `NAMES` is one an address can name, which
#: `tests/test_a_hop_from_two_addresses` holds
SCHEMES = {**{n: n for n in NAMES if n not in _NO_SCHEME},
           **{a: e for a, e in ALIASES.items() if e not in _NO_SCHEME},
           **SCHEME_ONLY}

#: the port a side listens on where it names none - the one table every
#: side is built from (`config.build_hop`), an address's and the long
#: form's alike, and the port each engine's own connection falls back to.
#: Measured before it: six engines here and the rest inline in their
#: engines, so a cross-engine hop's long-form sides got port 0 while the
#: same sides as addresses got 3306 and 5432 (audit 2026-10-01, DU6, K15)
PORTS = {
    "postgres": 5432, "mysql": 3306, "mssql": 1433,
    "mongodb": 27017, "redis": 6379, "kafka": 9092,
    "tidb": 4000, "cockroachdb": 26257, "dsql": 5432, "cosmosdb": 443,
    "oracle": 1521, "clickhouse": 8123, "cassandra": 9042,
    "opensearch": 9200, "redshift": 5439, "db2": 50000, "db2zos": 446,
    "db2i": 8076, "ase": 5000, "teradata": 1025,
}


def canonical(name):
    """The engine `name` is - its own name, or the one it is another name
    of (`ALIASES`) - or None for a name no engine goes by."""
    got = str(name or "").strip().lower()
    if got in NAMES:
        return got
    return ALIASES.get(got)


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
    if name == "teradata":
        from .teradata import TeradataEngine
        return TeradataEngine
    if name == "sapodp":
        from .sapodp import SapOdpEngine
        return SapOdpEngine
    if name == "saas":
        from .saas import SaasEngine
        return SaasEngine
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


#: what each engine is called where a person reads it - the manual, the
#: site (`tools/gen_docs.py`) and the troubleshooting guide's headings; a
#: name added to `NAMES` without one here fails
#: `tests/test_the_manual_is_generated_from_the_code.py`
TITLES = {"postgres": "PostgreSQL", "mysql": "MySQL", "mongodb": "MongoDB",
          "mssql": "SQL Server", "redis": "Redis", "kafka": "Kafka",
          "sqlite": "SQLite", "parquet": "Parquet files",
          "clickhouse": "ClickHouse", "dynamodb": "DynamoDB",
          "oracle": "Oracle Database", "db2": "Db2 (Linux, UNIX, Windows)",
          "db2i": "Db2 for i", "db2zos": "Db2 for z/OS", "ase": "SAP ASE",
          "teradata": "Teradata", "opensearch": "OpenSearch",
          "cassandra": "Cassandra", "redshift": "Amazon Redshift",
          "snowflake": "Snowflake", "bigquery": "BigQuery",
          "kinesis": "Kinesis Data Streams", "pubsub": "Google Pub/Sub",
          "duckdb": "DuckDB", "copybook": "Files described by COBOL copybooks",
          "iceberg": "Iceberg tables", "delta": "Delta tables",
          "tidb": "TiDB", "cockroachdb": "CockroachDB",
          "dsql": "Aurora DSQL", "couchbase": "Couchbase",
          "cosmosdb": "Cosmos DB for NoSQL",
          "sapodp": "SAP application data (ODP)",
          "saas": "SaaS applications, read by a connector program",
          "hetero": "Any pair of two engines",
          "generic": "Any other SQL engine"}


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
    cls = _class_for(canonical(name) or name)
    if cls is None:
        raise SystemExit(f"unsupported engine {name}")
    return cls(hop)


def get_engine(hop):
    # a server reached through a tunnel is reached through its open end
    from ..tunnel import through
    through(hop)
    return engine_named(hop.engine, hop)
