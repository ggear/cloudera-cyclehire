--
-- Cyclehire Summary
--

SELECT 
  COUNT(1) AS total_station_readings
FROM cyclehire_processed_cleansed_canonical;

SELECT
  FROM_UTC_TIMESTAMP(updateDate, "GMT") AS station_reading_timestamp,
  COUNT(1) AS station_readings
FROM cyclehire_processed_cleansed_canonical
GROUP BY updateDate;

SELECT 
  FROM_UTC_TIMESTAMP(getDate, "GMT"),
  FROM_UTC_TIMESTAMP(updateDate, "GMT"),
  id,
  RPAD(name, 45, ' '),
  bikes,
  empty,
  docks
FROM cyclehire_processed_cleansed_canonical
LIMIT 5;

SELECT 
  FROM_UTC_TIMESTAMP(getDate, "GMT"),
  FROM_UTC_TIMESTAMP(updateDate, "GMT"),
  id,
  RPAD(name, 45, ' '),
  bikes,
  empty,
  docks
FROM cyclehire_processed_cleansed_canonical
WHERE
  id == 1
LIMIT 50;

