MERGE `{target_dataset}.{target_table}` T
USING (
  WITH patch AS (
    SELECT
      record_id[OFFSET(0)] AS diaSourceId,

      ANY_VALUE(
        CASE WHEN field_name = 'diaObjectId'
             THEN CAST(JSON_VALUE(value_json) AS INT64)
        END
      ) AS diaObjectId_value,
      COUNTIF(field_name = 'diaObjectId') > 0 AS diaObjectId_present,

      ANY_VALUE(
        CASE WHEN field_name = 'ssObjectId'
             THEN CAST(JSON_VALUE(value_json) AS INT64)
        END
      ) AS ssObjectId_value,
      COUNTIF(field_name = 'ssObjectId') > 0 AS ssObjectId_present,

      ANY_VALUE(
        CASE WHEN field_name = 'ssObjectReassocTimeMjdTai'
             THEN CAST(JSON_VALUE(value_json) AS FLOAT64)
        END
      ) AS ssObjectReassocTimeMjdTai_value,
      COUNTIF(field_name = 'ssObjectReassocTimeMjdTai') > 0 AS ssObjectReassocTimeMjdTai_present,

      ANY_VALUE(
        CASE WHEN field_name = 'timeWithdrawnMjdTai'
             THEN CAST(JSON_VALUE(value_json) AS FLOAT64)
        END
      ) AS timeWithdrawnMjdTai_value,
      COUNTIF(field_name = 'timeWithdrawnMjdTai') > 0 AS timeWithdrawnMjdTai_present

    FROM `{updates_table}`
    WHERE table_name = 'DiaSource'
      AND field_name IN ('diaObjectId', 'ssObjectId', 'ssObjectReassocTimeMjdTai', 'timeWithdrawnMjdTai')
    GROUP BY diaSourceId
  )
  SELECT * FROM patch
) P
ON T.diaSourceId = P.diaSourceId
WHEN MATCHED THEN
UPDATE SET
  diaObjectId              = IF(P.diaObjectId_present,              P.diaObjectId_value,              T.diaObjectId),
  ssObjectId               = IF(P.ssObjectId_present,               P.ssObjectId_value,               T.ssObjectId),
  ssObjectReassocTimeMjdTai = IF(P.ssObjectReassocTimeMjdTai_present, P.ssObjectReassocTimeMjdTai_value, T.ssObjectReassocTimeMjdTai),
  timeWithdrawnMjdTai      = IF(P.timeWithdrawnMjdTai_present,      P.timeWithdrawnMjdTai_value,      T.timeWithdrawnMjdTai)
