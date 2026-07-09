-- This SQL script fills in null validityEndMjdTai values for DiaObject rows
-- in the target table, which is the DiaObject table in the promotion dataset
-- containing the existing data plus updates from staging. It uses a MERGE
-- statement to update the target table in place.
MERGE `{target_table}` AS T
USING (
-- Select relevant fields from the target table along with the computed end
-- time from the window function.
  SELECT
    diaObjectId,
    validityStartMjdTai,
    computedValidityEndMjdTai
  FROM (
-- Use a window function to compute the next validityStartMjdTai for each
-- DiaObject, which will become the validityEndMjdTai for the current row.
    SELECT
      T.diaObjectId,
      T.validityStartMjdTai,
      T.validityEndMjdTai,
      LEAD(T.validityStartMjdTai) OVER (
        PARTITION BY T.diaObjectId
        ORDER BY T.validityStartMjdTai
      ) AS computedValidityEndMjdTai
    FROM `{target_table}` AS T
    WHERE T.diaObjectId IN (
-- Only consider DiaObjects that are present in the staging table, since those
-- are the ones being updated. This avoids scanning the entire target table.
      SELECT DISTINCT diaObjectId
      FROM `{staging_table}`
    )
  )
-- Only include rows where validityEndMjdTai is still NULL and where a non-NULL
-- end time was computed.
  WHERE validityEndMjdTai IS NULL AND computedValidityEndMjdTai IS NOT NULL
) AS S
-- Match on the full composite key to uniquely identify the row to update.
ON T.diaObjectId = S.diaObjectId
  AND T.validityStartMjdTai = S.validityStartMjdTai
-- Update matching rows by setting validityEndMjdTai to the computed value.
WHEN MATCHED THEN UPDATE SET validityEndMjdTai = S.computedValidityEndMjdTai;
