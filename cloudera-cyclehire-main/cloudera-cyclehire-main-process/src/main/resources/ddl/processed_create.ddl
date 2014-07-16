--
-- Cyclehire Processed Schema Create
--

CREATE EXTERNAL TABLE IF NOT EXISTS cyclehire_processed_${hiveconf:cyclehire.table.modifier} (
  getDate BIGINT,
  updateDate BIGINT,
  id SMALLINT,
  name STRING,
  terminal STRING,
  lattitude FLOAT,
  longitude FLOAT,
  installed BOOLEAN,
  locked BOOLEAN,
  installDate BIGINT,
  removalDate BIGINT,
  temporary BOOLEAN,
  bikes SMALLINT,
  empty SMALLINT,
  docks SMALLINT,
  source STRING
)
COMMENT 'TFL Cyclehire processed data (${hiveconf:cyclehire.table.modifier})'
PARTITIONED BY (
  year STRING,
  month STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:cyclehire.table.location}';

MSCK REPAIR TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier};
