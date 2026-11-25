CREATE SCHEMA IF NOT EXISTS iceberg.history;

CREATE TABLE IF NOT EXISTS iceberg.history.accounts_cdc (
    event_id varchar NOT NULL,
    account_id bigint NOT NULL,
    operation varchar NOT NULL,
    owner_name varchar,
    city varchar,
    status varchar,
    effective_at timestamp(6) NOT NULL,
    source_lsn bigint,
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    envelope varchar NOT NULL
) WITH (format = 'PARQUET', format_version = 2);

CREATE TABLE IF NOT EXISTS iceberg.history.accounts_scd2 (
    version_id varchar NOT NULL,
    account_id bigint NOT NULL,
    owner_name varchar NOT NULL,
    city varchar NOT NULL,
    status varchar NOT NULL,
    valid_from timestamp(6) NOT NULL,
    valid_to timestamp(6),
    is_current boolean NOT NULL
) WITH (format = 'PARQUET', format_version = 2);
