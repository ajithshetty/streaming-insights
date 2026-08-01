"""
PyFlink Table API job.

Reads raw user-interaction events from Kafka/Redpanda, computes 30s tumbling
window aggregates (by event type, by page, and an overall summary), and
writes the results to Postgres tables that the FastAPI service reads from.
"""

import glob
import os

from pyflink.table import EnvironmentSettings, TableEnvironment

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "redpanda:9092")
EVENTS_TOPIC = os.environ.get("EVENTS_TOPIC", "user-events")

PG_HOST = os.environ.get("POSTGRES_HOST", "postgres")
PG_PORT = os.environ.get("POSTGRES_PORT", "5432")
PG_DB = os.environ.get("POSTGRES_DB", "streamdb")
PG_USER = os.environ.get("POSTGRES_USER", "postgres")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
PG_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

WINDOW_SECONDS = os.environ.get("WINDOW_SECONDS", "30")


def main() -> None:
    settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(settings)
    t_env.get_config().set("parallelism.default", "1")
    t_env.get_config().set("pipeline.name", "streaming-insights-aggregator")

    # Kafka + JDBC SQL connector jars are downloaded into this directory at
    # image build time (see Dockerfile) and registered on the pipeline
    # classpath so they're available to the local mini-cluster.
    connector_jars = glob.glob("/opt/flink-connectors/*.jar")
    if connector_jars:
        jars_conf = ";".join(f"file://{jar}" for jar in connector_jars)
        t_env.get_config().set("pipeline.jars", jars_conf)

    # ---- Source: raw events from Kafka ----
    t_env.execute_sql(f"""
        CREATE TABLE events (
            event_id     STRING,
            user_id      STRING,
            event_type   STRING,
            page         STRING,
            device       STRING,
            country      STRING,
            session_id   STRING,
            event_time   TIMESTAMP(3),
            WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{EVENTS_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-aggregator',
            'scan.startup.mode' = 'latest-offset',
            'format' = 'json',
            'json.ignore-parse-errors' = 'true',
            'json.timestamp-format.standard' = 'ISO-8601'
        )
    """)

    # ---- Sinks: aggregate tables in Postgres ----
    t_env.execute_sql(f"""
        CREATE TABLE event_type_agg (
            window_start TIMESTAMP(3),
            window_end   TIMESTAMP(3),
            event_type   STRING,
            event_count  BIGINT,
            unique_users BIGINT,
            PRIMARY KEY (window_start, event_type) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'event_type_agg',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE page_agg (
            window_start TIMESTAMP(3),
            window_end   TIMESTAMP(3),
            page         STRING,
            event_count  BIGINT,
            PRIMARY KEY (window_start, page) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'page_agg',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}'
        )
    """)

    t_env.execute_sql(f"""
        CREATE TABLE summary_agg (
            window_start   TIMESTAMP(3),
            window_end     TIMESTAMP(3),
            total_events   BIGINT,
            unique_users   BIGINT,
            events_per_sec DOUBLE,
            PRIMARY KEY (window_start) NOT ENFORCED
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{PG_URL}',
            'table-name' = 'summary_agg',
            'username' = '{PG_USER}',
            'password' = '{PG_PASSWORD}'
        )
    """)

    # ---- Run all three windowed aggregations as one statement set ----
    stmt_set = t_env.create_statement_set()

    stmt_set.add_insert_sql(f"""
        INSERT INTO event_type_agg
        SELECT
            window_start,
            window_end,
            event_type,
            COUNT(*) AS event_count,
            COUNT(DISTINCT user_id) AS unique_users
        FROM TABLE(
            TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '{WINDOW_SECONDS}' SECONDS)
        )
        GROUP BY window_start, window_end, event_type
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO page_agg
        SELECT
            window_start,
            window_end,
            page,
            COUNT(*) AS event_count
        FROM TABLE(
            TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '{WINDOW_SECONDS}' SECONDS)
        )
        GROUP BY window_start, window_end, page
    """)

    stmt_set.add_insert_sql(f"""
        INSERT INTO summary_agg
        SELECT
            window_start,
            window_end,
            COUNT(*) AS total_events,
            COUNT(DISTINCT user_id) AS unique_users,
            CAST(COUNT(*) AS DOUBLE) / {WINDOW_SECONDS} AS events_per_sec
        FROM TABLE(
            TUMBLE(TABLE events, DESCRIPTOR(event_time), INTERVAL '{WINDOW_SECONDS}' SECONDS)
        )
        GROUP BY window_start, window_end
    """)

    stmt_set.execute().wait()


if __name__ == "__main__":
    main()
