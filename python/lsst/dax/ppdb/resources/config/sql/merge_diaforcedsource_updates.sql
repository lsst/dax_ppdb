MERGE `{target_dataset}.{target_table}` T
USING (
  WITH patch AS (
    SELECT
      record_id[OFFSET(0)] AS diaObjectId,
      record_id[OFFSET(1)] AS visit,
      record_id[OFFSET(2)] AS detector,

      ANY_VALUE(
        CASE WHEN field_name = 'timeWithdrawnMjdTai'
             THEN CAST(JSON_VALUE(value_json) AS FLOAT64)
        END
      ) AS timeWithdrawnMjdTai_value,
      COUNTIF(field_name = 'timeWithdrawnMjdTai') > 0 AS timeWithdrawnMjdTai_present

    FROM `{updates_table}`
    WHERE table_name = 'DiaForcedSource'
      AND field_name IN ('timeWithdrawnMjdTai')
    GROUP BY diaObjectId, visit, detector
  )
  SELECT * FROM patch
) P
ON T.diaObjectId = P.diaObjectId
   AND T.visit = P.visit
   AND T.detector = P.detector
WHEN MATCHED THEN
UPDATE SET
  timeWithdrawnMjdTai = IF(P.timeWithdrawnMjdTai_present, P.timeWithdrawnMjdTai_value, T.timeWithdrawnMjdTai);
