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
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe'
LOCATION '${hiveconf:cyclehire.table.location}';

INSERT OVERWRITE TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier}_sequence_${hiveconf:cyclehire.table.codec} PARTITION (year, month)
SELECT * FROM cyclehire_processed_cleansed_canonical;

MSCK REPAIR TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier}_sequence_${hiveconf:cyclehire.table.codec};
