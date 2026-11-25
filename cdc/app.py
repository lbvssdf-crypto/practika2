import argparse
import json
import os
import time
from contextlib import closing
from datetime import datetime, timedelta
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen
from uuid import uuid4

import boto3
import psycopg
import trino
from botocore.config import Config
from kafka import KafkaConsumer, TopicPartition
from kafka.structs import OffsetAndMetadata

SQL = Path(os.environ.get("SQL_DIR", "/sql"))
CONNECT = os.environ.get("CONNECT_URL", "http://connect:8083")
LAKEKEEPER = os.environ.get("LAKEKEEPER_URL", "http://lakekeeper:8181")
S3 = os.environ.get("S3_URL", "http://silo:9000")
PG_DSN = os.environ.get("PG_DSN", "postgresql://postgres:pg-demo@postgres/operations")
BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = "p2.public.accounts"
PARTITION = TopicPartition(TOPIC, 0)
UTC_EPOCH = datetime(1970, 1, 1)
BUCKET = "p2-history"
ACCESS_KEY = "silo-root-user"
SECRET_KEY = "silo-root-password"


def request(base, path, method="GET", data=None, accepted_error_types=()):
    body = json.dumps(data).encode() if data is not None else None
    req = Request(base + path, data=body, method=method,
                  headers={"Content-Type": "application/json"})
    try:
        with urlopen(req, timeout=8) as result:
            payload = result.read()
            return json.loads(payload) if payload else None
    except HTTPError as error:
        body = error.read().decode()
        if accepted_error_types:
            details = json.loads(body).get("error", {})
            if details.get("type") in accepted_error_types:
                return None
        raise RuntimeError(f"{method} {path}: {error.code} {body}") from error


def wait_for(name, action, timeout=180):
    deadline = time.monotonic() + timeout
    while True:
        try:
            result = action()
            if result:
                return result
            reason = "условие ещё не выполнено"
        except Exception as error:
            reason = str(error)
        if time.monotonic() >= deadline:
            raise RuntimeError(f"{name}: {reason}")
        time.sleep(2)


def trino_cursor():
    return trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", "8080")),
        user="p2_analytics",
        timezone="UTC",
    ).cursor()


def query(cursor, sql):
    cursor.execute(sql)
    return cursor.fetchall()


def sql_statements(name):
    return [part.strip() for part in (SQL / name).read_text().split(";") if part.strip()]


def setup():
    storage = boto3.client("s3", endpoint_url=S3, region_name="us-east-1",
                           aws_access_key_id=ACCESS_KEY, aws_secret_access_key=SECRET_KEY,
                           config=Config(s3={"addressing_style": "path"}))
    buckets = wait_for("Silo", storage.list_buckets)
    if BUCKET not in {bucket["Name"] for bucket in buckets["Buckets"]}:
        storage.create_bucket(Bucket=BUCKET)

    project = "00000000-0000-0000-0000-000000000000"
    def lakekeeper_ready():
        request(LAKEKEEPER, "/health")
        return True

    wait_for("Lakekeeper", lakekeeper_ready)
    request(LAKEKEEPER, "/management/v1/bootstrap", method="POST",
            data={"accept-terms-of-use": True}, accepted_error_types=("CatalogAlreadyBootstrapped",))
    path = f"/management/v1/warehouse?project-id={project}"
    warehouses = request(LAKEKEEPER, path)["warehouses"]
    if not any(w["name"] == "p2_history" for w in warehouses):
        request(LAKEKEEPER, "/management/v1/warehouse", method="POST", data={
            "warehouse-name": "p2_history",
            "project-id": project,
            "storage-profile": {
                "type": "s3", "bucket": BUCKET, "key-prefix": "warehouse",
                "endpoint": S3, "region": "us-east-1",
                "path-style-access": True, "flavor": "s3-compat", "sts-enabled": False,
            },
            "storage-credential": {
                "type": "s3", "credential-type": "access-key",
                "access-key-id": ACCESS_KEY, "secret-access-key": SECRET_KEY,
            },
        })

    cursor = trino_cursor()
    wait_for("Trino", lambda: query(cursor, "SELECT 1"))
    for sql in sql_statements("01_tables.sql"):
        query(cursor, sql)

    wait_for("Kafka Connect", lambda: request(CONNECT, "/connector-plugins"))
    config = json.loads(Path(__file__).with_name("connector.json").read_text())
    config["database.hostname"] = os.environ.get("PG_HOST", config["database.hostname"])
    config["database.port"] = os.environ.get("PG_PORT", config["database.port"])
    request(CONNECT, "/connectors/p2-accounts/config", method="PUT", data=config)

    def running():
        status = request(CONNECT, "/connectors/p2-accounts/status")
        states = [task["state"] for task in status["tasks"]]
        if "FAILED" in states:
            raise RuntimeError(status["tasks"][0].get("trace", "connector task failed"))
        return status["connector"]["state"] == "RUNNING" and states == ["RUNNING"]

    wait_for("Debezium", running)
    print("Lakekeeper warehouse, Iceberg tables and Debezium connector are ready.", flush=True)


def create_consumer(group=None):
    return KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        max_poll_records=100,
    )


def decode(record):
    event = json.loads(record.value)
    op = event["op"]
    source = event["source"]
    key = json.loads(record.key)["account_id"]
    if op == "r":
        event_id = f"snapshot/{key}"
    elif op in {"c", "u", "d"}:
        transaction = event["transaction"]
        event_id = f'{source["lsn"]}/{transaction["id"]}/{transaction["total_order"]}'
    else:
        raise ValueError(f"Unsupported CDC operation: {op}")

    row = event["after"] or {}
    if op == "d":
        effective_at = UTC_EPOCH + timedelta(milliseconds=source["ts_ms"])
    else:
        effective_at = UTC_EPOCH + timedelta(microseconds=row["effective_at"])
    return (event_id, key, op, row.get("owner_name"), row.get("city"), row.get("status"),
            effective_at, source["lsn"], record.partition, record.offset, record.value.decode())


def store_batch(cursor, records):
    decoded = {}
    for record in records:
        if record.value is not None:
            row = decode(record)
            decoded.setdefault(row[0], row)
    if not decoded:
        return
    columns = ("event_id, account_id, operation, owner_name, city, status, effective_at, "
               "source_lsn, kafka_partition, kafka_offset, envelope")
    bind_row = "(" + ", ".join(["?"] * 6 + ["CAST(? AS timestamp(6))"] + ["?"] * 4) + ")"
    values = ", ".join([bind_row] * len(decoded))
    # Atomic append by source identity. Kafka position is kept from the first arrival.
    source_columns = ", ".join(f"source.{column.strip()}" for column in columns.split(","))
    cursor.execute(f"""
        MERGE INTO iceberg.history.accounts_cdc AS target
        USING (VALUES {values}) AS source ({columns})
        ON target.event_id = source.event_id
        WHEN NOT MATCHED THEN INSERT ({columns}) VALUES ({source_columns})
        """,
        [value for row in decoded.values() for value in row],
    )
    cursor.fetchall()
    keys = sorted({row[1] for row in decoded.values()})
    placeholders = ", ".join("?" for _ in keys)
    cursor.execute((SQL / "02_scd2.sql").read_text().strip().rstrip(";").format(keys=placeholders), keys)
    cursor.fetchall()
    print(f"batch [{records[0].offset}..{records[-1].offset}]: "
          f"{len(decoded)} CDC events, keys {keys}", flush=True)


def worker():
    cursor = trino_cursor()
    with closing(create_consumer("p2-scd2")) as consumer:
        consumer.subscribe([TOPIC])
        while True:
            records = consumer.poll(timeout_ms=1200).get(PARTITION, [])
            if records:
                store_batch(cursor, records)
                consumer.commit({PARTITION: OffsetAndMetadata(records[-1].offset + 1, "", -1)})


def table_fingerprints(cursor):
    return query(cursor, """
        SELECT 'cdc', count(*), to_hex(checksum(ROW(
            event_id, account_id, operation, owner_name, city, status,
            effective_at, source_lsn, kafka_partition, kafka_offset, envelope)))
        FROM iceberg.history.accounts_cdc
        UNION ALL
        SELECT 'scd2', count(*), to_hex(checksum(ROW(
            version_id, account_id, owner_name, city, status, valid_from, valid_to, is_current)))
        FROM iceberg.history.accounts_scd2
        ORDER BY 1
    """)


def replay():
    cursor = trino_cursor()
    before = table_fingerprints(cursor)
    with closing(create_consumer(f"p2-replay-{uuid4()}")) as consumer:
        consumer.subscribe([TOPIC])
        deadline = time.monotonic() + 30
        stop = None
        while True:
            records = consumer.poll(timeout_ms=1200).get(PARTITION, [])
            if not records:
                if time.monotonic() >= deadline:
                    raise TimeoutError("Kafka replay did not reach its end offset")
                continue
            if stop is None:
                stop = consumer.highwater(PARTITION)
            batch = [record for record in records if record.offset < stop]
            if batch:
                store_batch(cursor, batch)
                deadline = time.monotonic() + 30
            if records[-1].offset + 1 >= stop:
                break
    after = table_fingerprints(cursor)
    if before != after or [row[1] for row in after] != [9, 7]:
        raise AssertionError(f"Unexpected change after replay. Before: {before}, after: {after}")
    print(f"Replay complete through offset {stop - 1}.")
    print(f"CDC rows: {after[0][1]}, SCD2 rows: {after[1][1]}.")
    print(f"Unchanged content checksums: CDC {after[0][2]}, SCD2 {after[1][2]}.")


def demo():
    cursor = trino_cursor()
    snapshot_count = wait_for("Начальный снимок", lambda: current_count(cursor))
    if snapshot_count != 2:
        raise RuntimeError("The demo requires the initial two snapshot events and a fresh project.")
    with psycopg.connect(PG_DSN) as connection:
        statements = sql_statements("03_changes.sql")
        for sql in statements[:4]:
            connection.execute(sql)
            connection.commit()
        for sql in statements[4:6]:
            connection.execute(sql)
        connection.commit()
        connection.execute(statements[6])
        connection.commit()

    wait_for("SCD Type 2", lambda: query(cursor,
        "SELECT count(*) FROM iceberg.history.accounts_scd2")[0][0] == 7)
    expected = [(101, "Анна Романова", "Казань", "ACTIVE")]
    for index, sql in enumerate(sql_statements("04_checks.sql"), 1):
        rows = query(cursor, sql)
        if index == 2 and list(map(tuple, rows)) != expected:
            raise AssertionError(rows)
        if index == 3 and (len(rows) != 1 or rows[0][2] != "SUSPENDED"):
            raise AssertionError(rows)
        if index == 4 and sorted(row[3] for row in rows) != sorted("rruuuduuc"):
            raise AssertionError(rows)
        if index == 5 and list(map(tuple, rows)) != [(101, 5, 1), (102, 1, 0), (103, 1, 1)]:
            raise AssertionError(rows)
        if index in {6, 7} and rows:
            raise AssertionError(rows)
        print(f"Q{index}: {len(rows)} rows")
        print("\t".join(column[0] for column in cursor.description))
        for row in rows:
            print("\t".join("NULL" if value is None else str(value) for value in row))
        print()


def current_count(cursor):
    count = query(cursor, "SELECT count(*) FROM iceberg.history.accounts_cdc")[0][0]
    initial_versions = query(cursor, "SELECT count(*) FROM iceberg.history.accounts_scd2")[0][0]
    return count if count >= 2 and initial_versions >= 2 else 0


if __name__ == "__main__":
    commands = {"setup": setup, "worker": worker, "demo": demo, "replay": replay}
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=commands)
    commands[parser.parse_args().mode]()
