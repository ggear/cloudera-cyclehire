--
-- Cyclehire Processed Schema Rewrite Sequence
--

CREATE EXTERNAL TABLE IF NOT EXISTS cyclehire_processed_${hiveconf:cyclehire.table.modifier}_sequence_${hiveconf:cyclehire.table.codec} (
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

INSERT OVERWRITE TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier}_sequence_${hiveconf:cyclehire.table.codec}
PARTITION (year, month)
SELECT *
FROM cyclehire_processed_cleansed_canonical
WHERE year='${hiveconf:cyclehire.table.partition.year}' AND month='${hiveconf:cyclehire.table.partition.month}';
