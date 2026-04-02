MERGE `{target_dataset}.{target_table}` T
USING (
  WITH patch AS (
    SELECT
      record_id[OFFSET(0)] AS diaObjectId,

      ANY_VALUE(
        CASE WHEN field_name = 'validityEndMjdTai'
             THEN CAST(JSON_VALUE(value_json) AS FLOAT64)
        END
      ) AS validityEndMjdTai_value,
      COUNTIF(field_name = 'validityEndMjdTai') > 0 AS validityEndMjdTai_present,

      ANY_VALUE(
        CASE WHEN field_name = 'nDiaSources'
             THEN CAST(JSON_VALUE(value_json) AS INT64)
        END
      ) AS nDiaSources_value,
      COUNTIF(field_name = 'nDiaSources') > 0 AS nDiaSources_present

    FROM `{updates_table}`
    WHERE table_name = 'DiaObject'
      AND field_name IN ('validityEndMjdTai', 'nDiaSources')
    GROUP BY diaObjectId
  )
  SELECT * FROM patch
) P
ON T.diaObjectId = P.diaObjectId
WHEN MATCHED THEN
UPDATE SET
  validityEndMjdTai = IF(P.validityEndMjdTai_present, P.validityEndMjdTai_value, T.validityEndMjdTai),
  nDiaSources       = IF(P.nDiaSources_present AND P.nDiaSources_value IS NOT NULL, P.nDiaSources_value, T.nDiaSources);
