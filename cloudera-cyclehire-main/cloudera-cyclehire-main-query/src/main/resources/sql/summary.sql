--
-- Cyclehire Summary
--

SELECT 
  COUNT(1) AS readings
FROM cyclehire_processed_cleansed_canonical;

SELECT 
  COUNT(1) AS readings
FROM cyclehire_processed_cleansed_rewrite_parquet_none;

SELECT 
  FROM_UNIXTIME(CAST(polled/1000 AS BIGINT)) AS polled,
  FROM_UNIXTIME(CAST(updated/1000 AS BIGINT)) AS updated,
  id,
  RPAD(name, 45, ' ') AS name,
  bikes,
  empty,
  docks
FROM cyclehire_processed_cleansed_rewrite_parquet_none
LIMIT 10;

SELECT readings_per_update, count(1) AS updates
FROM (
  SELECT
    COUNT(1) AS readings_per_update
  FROM cyclehire_processed_cleansed_rewrite_parquet_none
  GROUP BY updated
) AS T
GROUP BY readings_per_update
ORDER BY readings_per_update ASC;
