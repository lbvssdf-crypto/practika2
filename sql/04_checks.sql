-- Полная история
SELECT account_id, owner_name, city, status, valid_from, valid_to, is_current
FROM iceberg.history.accounts_scd2
ORDER BY account_id, valid_from, valid_to;

-- Владелец счёта 101 на 1 марта по UTC
SELECT account_id, owner_name, city, status
FROM iceberg.history.accounts_scd2
WHERE account_id = 101
  AND valid_from <= TIMESTAMP '2026-03-01 00:00:00'
  AND (valid_to > TIMESTAMP '2026-03-01 00:00:00' OR valid_to IS NULL);

-- Состояние непосредственно перед отменой счёта 101
SELECT account_id, owner_name, status, valid_to
FROM iceberg.history.accounts_scd2
WHERE account_id = 101
  AND valid_from < TIMESTAMP '2026-03-09 18:00:00'
  AND valid_to = TIMESTAMP '2026-03-09 18:00:00';

-- События в порядке доставки Kafka
SELECT kafka_partition, kafka_offset, account_id, operation, effective_at, source_lsn
FROM iceberg.history.accounts_cdc
ORDER BY kafka_partition, kafka_offset;

-- Удалённый счёт остаётся в истории, но текущей версии для него нет
SELECT account_id, count(*) AS versions, count_if(is_current) AS current_versions
FROM iceberg.history.accounts_scd2
GROUP BY account_id
ORDER BY account_id;

-- Контроль, должен вернуть ноль строк
SELECT account_id, count_if(is_current) AS current_versions,
       count(*) - count(DISTINCT version_id) AS duplicates
FROM iceberg.history.accounts_scd2
GROUP BY account_id
HAVING count_if(is_current) > 1 OR count(*) <> count(DISTINCT version_id);

-- Контроль отрицательных и пересекающихся интервалов, должен вернуть ноль строк
WITH ordered AS (
    SELECT account_id, valid_from, valid_to,
           lead(valid_from) OVER (
               PARTITION BY account_id
               ORDER BY valid_from, valid_to NULLS LAST, version_id
           ) AS next_valid_from
    FROM iceberg.history.accounts_scd2
)
SELECT account_id, valid_from, valid_to, next_valid_from
FROM ordered
WHERE valid_to < valid_from
   OR (next_valid_from IS NOT NULL AND (valid_to IS NULL OR valid_to > next_valid_from));
