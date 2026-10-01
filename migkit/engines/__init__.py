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
           "cosmosdb-nosql": "cosmosdb", "azure-cosmos": "cosmosdb",
           "td": "teradata", "vantage": "teradata",
           "sap-odp": "sapodp", "odp": "sapodp", "program": "saas",
           # the cloud-native services (P0-CLOUDS part 2)
           "cloud-spanner": "spanner", "spanner-pg": "spanner",
           "cloud-bigtable": "bigtable", "firestore-native": "firestore",
           "firestore-datastore": "datastore",
           "cloud-datastore": "datastore", "opengauss": "gaussdb",
           "gaussdb-opengauss": "gaussdb", "oceanbase-mysql": "oceanbase",
           "polardb-x": "polardbx", "drds": "polardbx",
           "tdsql-distributed": "dcdb", "tdsql-shard": "dcdb",
           "tdsql-c": "mysql", "amazon-keyspaces": "keyspaces",
           "aws-keyspaces": "keyspaces", "amazon-neptune": "neptune",
           "amazon-timestream": "timestream",
           "geminidb": "geminidb-cassandra"}

#: engines reached over another's wire protocol, whose values a driver
#: hands over, a table is built and a value written as that one's are
#: (`canon.dialect`): TiDB over MySQL's, CockroachDB and Aurora DSQL over
#: PostgreSQL's. Everything else - the catalogue, the change feed, what a
#: transaction may hold - is the engine's own
WIRE = {"tidb": "mysql", "cockroachdb": "postgres", "dsql": "postgres",
        "gaussdb": "postgres", "oceanbase": "mysql",
        "oceanbase-oracle": "oracle", "polardbx": "mysql", "dcdb": "mysql",
        "keyspaces": "cassandra", "geminidb-cassandra": "cassandra",
        "geminidb-redis": "redis", "geminidb-mongo": "mongodb"}

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
    "teradata": "teradata",
    # an SAP system's OData services, https or - a sandbox's - http
    "sapodp": "sapodp", "sapodp+http": "sapodp",
    # a program source: the scheme is its kind (`engines.saas`)
    "dlt": "saas", "singer": "saas", "airbyte": "saas",
    "salesforce": "saas",
    "trino": "generic", "presto": "generic", "vertica": "generic",
    "tidb": "tidb",
    "cockroachdb": "cockroachdb", "cockroach": "cockroachdb",
    "dsql": "dsql",
    "couchbase": "couchbase", "couchbases": "couchbase",
    "cosmosdb": "cosmosdb", "cosmos": "cosmosdb",
    "spanner": "spanner", "bigtable": "bigtable",
    "firestore": "firestore", "datastore": "datastore",
    "gaussdb": "gaussdb", "opengauss": "gaussdb",
    "oceanbase": "oceanbase", "oceanbase-oracle": "oceanbase-oracle",
    "polardbx": "polardbx", "dcdb": "dcdb",
    "keyspaces": "keyspaces", "neptune": "neptune",
    "timestream": "timestream", "lindorm": "lindorm",
    "geminidb-cassandra": "geminidb-cassandra",
    "geminidb-redis": "geminidb-redis", "geminidb-mongo": "geminidb-mongo",
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
    return _cloud_class(name)


#: the cloud-native services' engines (P0-CLOUDS part 2): module, class
CLOUD = {"spanner": ("spanner", "SpannerEngine"),
         "bigtable": ("bigtable", "BigtableEngine"),
         "firestore": ("firestore", "FirestoreEngine"),
         "datastore": ("firestore", "DatastoreEngine"),
         "gaussdb": ("gaussdb", "GaussEngine"),
         "oceanbase": ("oceanbase", "OceanBaseEngine"),
         "oceanbase-oracle": ("oceanbase", "OceanBaseOracleEngine"),
         "polardbx": ("distmysql", "PolarDBXEngine"),
         "dcdb": ("distmysql", "TdsqlEngine"),
         "keyspaces": ("keyspaces", "KeyspacesEngine"),
         "neptune": ("neptune", "NeptuneEngine"),
         "timestream": ("timestream", "TimestreamEngine"),
         "lindorm": ("lindorm", "LindormEngine"),
         "geminidb-cassandra": ("geminidb", "GeminiCassandraEngine"),
         "geminidb-redis": ("geminidb", "GeminiRedisEngine"),
         "geminidb-mongo": ("geminidb", "GeminiMongoEngine")}


def _cloud_class(name):
    import importlib
    got = CLOUD.get(name)
    if got is None:
        return None
    return getattr(importlib.import_module(f"{__name__}.{got[0]}"), got[1])


def same_engine(eng):
    """Whether `eng`'s hop moves between two sides of its own engine - as
    the hop names it, or as the pair a same-engine hop is carried by
    names both of its sides (`Engine._as_pair`)."""
    name = getattr(eng, "PAIR_ENGINE", "") or getattr(eng, "CANON_ENGINE",
                                                      "")
    hop = eng.hop
    own = ALIASES.get(hop.engine, hop.engine)
    if own != "hetero":
        return own == name
    opts = hop.options or {}
    sides = (opts.get("source_engine"), opts.get("target_engine"))
    return all(ALIASES.get(e, e) == name for e in sides)


#: every canonical engine name, in the order a report lists them
NAMES = ("postgres", "mysql", "mongodb", "mssql", "redis", "kafka", "sqlite",
         "parquet", "clickhouse", "dynamodb", "oracle", "db2", "db2i",
         "db2zos", "ase", "teradata",
         "opensearch", "cassandra", "redshift", "snowflake", "bigquery",
         "kinesis", "pubsub", "duckdb", "copybook", "iceberg", "delta",
         "tidb", "cockroachdb", "dsql", "couchbase", "cosmosdb",
         "spanner", "bigtable", "firestore", "datastore", "gaussdb",
         "oceanbase", "oceanbase-oracle", "polardbx", "dcdb", "keyspaces",
         "neptune", "timestream", "lindorm", "geminidb-cassandra",
         "geminidb-redis", "geminidb-mongo",
         "sapodp", "saas", "hetero", "generic")

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
          "spanner": "Google Cloud Spanner", "bigtable": "Google Cloud Bigtable",
          "firestore": "Firestore (native mode)",
          "datastore": "Firestore in Datastore mode",
          "gaussdb": "GaussDB (openGauss)",
          "oceanbase": "OceanBase (MySQL mode)",
          "oceanbase-oracle": "OceanBase (Oracle mode)",
          "polardbx": "PolarDB-X", "dcdb": "TDSQL (distributed)",
          "keyspaces": "Amazon Keyspaces", "neptune": "Amazon Neptune",
          "timestream": "Amazon Timestream", "lindorm": "Lindorm",
          "geminidb-cassandra": "GeminiDB (Cassandra API)",
          "geminidb-redis": "GeminiDB (Redis API)",
          "geminidb-mongo": "GeminiDB (MongoDB API)",
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
