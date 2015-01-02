--
-- Cyclehire Partitioned Schema Create
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:cyclehire.table.name} (
  source STRING,
  batch STRING,
  record STRING
)
COMMENT 'TFL Cyclehire raw partitioned data'
PARTITIONED BY (
  year STRING,
  month STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:cyclehire.table.location}';

MSCK REPAIR TABLE ${hiveconf:cyclehire.table.name};
