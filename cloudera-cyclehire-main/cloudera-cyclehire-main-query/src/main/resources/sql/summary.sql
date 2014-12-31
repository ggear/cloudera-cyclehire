--
-- Cyclehire Summary
--

SELECT 
  COUNT(1) AS updates
FROM cyclehire_processed_cleansed_canonical;

SELECT 
  COUNT(1) AS updates
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

SELECT
  stations,
  count(1) AS updates
FROM (
  SELECT
    COUNT(1) AS stations
  FROM cyclehire_processed_cleansed_rewrite_parquet_none
  GROUP BY updated
) AS stations_per_update
GROUP BY stations
ORDER BY stations ASC;

SELECT
  year,
  month,
  AVG(bikes/docks*100) AS bikes_avg,
  STDDEV_POP(bikes/docks*100) AS bikes_stddev,
  AVG(empty/docks*100) AS empty_avg,
  STDDEV_POP(empty/docks*100) AS empty_stddev,
  AVG((docks-bikes-empty)/docks*100) AS locked_avg,
  STDDEV_POP((docks-bikes-empty)/docks*100) AS locked_stddev
FROM cyclehire_processed_cleansed_rewrite_parquet_none
WHERE docks != 0
GROUP BY year,month;

SELECT
  COUNT(DISTINCT(id)) AS disabled_stations
FROM cyclehire_processed_cleansed_rewrite_parquet_none
WHERE
  is_locked = true OR
  is_installed = false OR
  docks = 0;
