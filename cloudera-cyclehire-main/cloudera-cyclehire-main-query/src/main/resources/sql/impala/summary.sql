--
-- Cyclehire Summary
--

SELECT 
  MILLISECONDS_ADD(FROM_UNIXTIME(CAST(polled/1000 AS BIGINT)),PMOD(updateDate,1000)) AS polled,
  MILLISECONDS_ADD(FROM_UNIXTIME(CAST(updated/1000 AS BIGINT)),PMOD(updateDate,1000)) AS updated,
  id,
  RPAD(name, 45, ' ') AS name,
  bikes,
  empty,
  docks
FROM cyclehire_processed_cleansed_rewrite_parquet_none
LIMIT 10;
