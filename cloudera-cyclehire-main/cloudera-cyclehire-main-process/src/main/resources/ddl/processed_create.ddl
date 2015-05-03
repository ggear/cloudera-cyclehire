--
-- Cyclehire Processed Schema Create
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
  year STRING,
  month STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS SEQUENCEFILE
LOCATION '${hivevar:cyclehire.table.location}';

MSCK REPAIR TABLE ${hivevar:cyclehire.table.name};
