--
-- Cyclehire Processed Schema Rewrite Parquet
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:cyclehire.table.name} (
  polled BIGINT,
  updated BIGINT,
  id SMALLINT,
  name STRING,
  terminal STRING,
  lattitude FLOAT,
  longitude FLOAT,
  is_installed BOOLEAN,
  is_locked BOOLEAN,
  installed BIGINT,
  removed BIGINT,
  temporary BOOLEAN,
  bikes SMALLINT,
  empty SMALLINT,
  docks SMALLINT,
  source STRING
)
COMMENT 'TFL Cyclehire processed data'
PARTITIONED BY (
  year SMALLINT,
  month TINYINT
)
STORED AS PARQUET
LOCATION '${hivevar:cyclehire.table.location}';

INSERT OVERWRITE TABLE ${hivevar:cyclehire.table.name}
PARTITION (year, month)
SELECT *
FROM cyclehire_processed_cleansed_canonical
WHERE year='${hivevar:cyclehire.table.partition.year}' AND month='${hivevar:cyclehire.table.partition.month}';
