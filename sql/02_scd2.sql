MERGE INTO iceberg.history.accounts_scd2 AS target
USING (
    WITH states AS (
        SELECT *, ROW(operation = 'd', owner_name, city, status) AS state
        FROM iceberg.history.accounts_cdc
        WHERE account_id IN ({keys})
    ),
    transitions AS (
        SELECT *, lag(state) OVER (
            PARTITION BY account_id, effective_at
            ORDER BY kafka_partition, kafka_offset
        ) AS previous_state_at_same_time
        FROM states
    ),
    intervals AS (
        SELECT event_id AS version_id, account_id, operation, owner_name,
               city, status, effective_at AS valid_from,
               lead(effective_at) OVER (
                   PARTITION BY account_id
                   ORDER BY effective_at, kafka_partition, kafka_offset
               ) AS valid_to
        FROM transitions
        WHERE state IS DISTINCT FROM previous_state_at_same_time
    )
    SELECT version_id, account_id, owner_name, city, status, valid_from,
           valid_to, valid_to IS NULL AS is_current
    FROM intervals
    WHERE operation IN ('r', 'c', 'u')
) AS source
ON target.version_id = source.version_id
WHEN MATCHED AND target.valid_to IS DISTINCT FROM source.valid_to
    THEN UPDATE SET valid_to = source.valid_to, is_current = source.is_current
WHEN NOT MATCHED THEN INSERT (
    version_id, account_id, owner_name, city, status, valid_from, valid_to, is_current
) VALUES (
    source.version_id, source.account_id, source.owner_name, source.city,
    source.status, source.valid_from, source.valid_to, source.is_current
);
