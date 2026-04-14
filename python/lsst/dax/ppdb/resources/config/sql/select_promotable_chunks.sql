/*
 * Selects promotable chunks in sequential order.
 *
 * Finds a contiguous range of 'staged' chunks that can be promoted together,
 * starting from the earliest unprocessed chunk (not 'promoted' or 'skipped')
 * and continuing until the first gap in staging (where a chunk is not 'staged').
 *
 * This ensures chunks are promoted in order without gaps, maintaining
 * consistency in the promotion process.
 */
WITH start AS (
SELECT MIN(apdb_replica_chunk) AS s
FROM {table_name}
WHERE status <> 'promoted'
    AND status <> 'skipped'
),
stop AS (
SELECT MIN(p.apdb_replica_chunk) AS e
FROM {table_name} p
JOIN start ON TRUE
WHERE start.s IS NOT NULL
    AND p.apdb_replica_chunk >= start.s
    AND p.status <> 'staged'
    AND status <> 'skipped'
)
SELECT p.apdb_replica_chunk
FROM {table_name} p
JOIN start ON TRUE
LEFT JOIN stop ON TRUE
WHERE start.s IS NOT NULL
AND p.status = 'staged'
AND p.apdb_replica_chunk >= start.s
AND (stop.e IS NULL OR p.apdb_replica_chunk < stop.e)
ORDER BY p.apdb_replica_chunk;
