--
-- Cyclehire Partitioned Schema Create
--

CREATE EXTERNAL TABLE IF NOT EXISTS cyclehire_raw_partitioned_${hiveconf:cyclehire.table.modifier} (
  source STRING,
  batch STRING,
  record STRING
)
COMMENT 'TFL Cyclehire raw partitioned data (${hiveconf:cyclehire.table.modifier})'
PARTITIONED BY (
  year STRING,
  month STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:cyclehire.table.location}';

MSCK REPAIR TABLE cyclehire_raw_partitioned_${hiveconf:cyclehire.table.modifier};
