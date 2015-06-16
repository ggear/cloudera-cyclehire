--
-- Cyclehire Partitioned Schema Create
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:cyclehire.table.name} (
  source STRING,
  batch STRING,
  record STRING
)
COMMENT 'TFL Cyclehire raw partitioned data'
PARTITIONED BY (
  year SMALLINT,
  month TINYINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS SEQUENCEFILE
LOCATION '${hivevar:cyclehire.table.location}';

MSCK REPAIR TABLE ${hivevar:cyclehire.table.name};
