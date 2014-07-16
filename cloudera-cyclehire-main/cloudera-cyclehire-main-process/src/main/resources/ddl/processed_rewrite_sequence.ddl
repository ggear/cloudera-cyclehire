--
-- Cyclehire Processed Schema Rewrite Sequence
--

CREATE EXTERNAL TABLE IF NOT EXISTS cyclehire_processed_${hiveconf:cyclehire.table.modifier}_sequence_${hiveconf:cyclehire.table.codec}
LIKE cyclehire_processed_cleansed_canonical;
ALTER TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier}_sequence_${hiveconf:cyclehire.table.codec} SET
TBLPROPERTIES ('comment' = 'TFL Cyclehire processed data (${hiveconf:cyclehire.table.modifier})');
ALTER TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier}_sequence_${hiveconf:cyclehire.table.codec} SET
SERDE 'org.apache.hadoop.hive.serde2.lazybinary.LazyBinarySerDe';
ALTER TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier}_sequence_${hiveconf:cyclehire.table.codec} SET
LOCATION '${hiveconf:cyclehire.table.location}';

INSERT OVERWRITE TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier}_sequence_${hiveconf:cyclehire.table.codec} PARTITION (year, month)
SELECT * FROM cyclehire_processed_cleansed_canonical;

MSCK REPAIR TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier}_sequence_${hiveconf:cyclehire.table.codec};

