--
-- Cyclehire Processed Schema Rewrite Parquet
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:cyclehire.table.name} (
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
LOCATION '${hiveconf:cyclehire.table.location}';

INSERT OVERWRITE TABLE ${hiveconf:cyclehire.table.name}
PARTITION (year, month)
SELECT *
FROM cyclehire_processed_cleansed_canonical
WHERE year='${hiveconf:cyclehire.table.partition.year}' AND month='${hiveconf:cyclehire.table.partition.month}';
