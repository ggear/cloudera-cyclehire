--
-- Cyclehire Processed Schema Create
--

--
-- Cyclehire Processed Delimited
--
CREATE EXTERNAL TABLE IF NOT EXISTS cyclehire_processed_${hiveconf:cyclehire.table.modifier} (
  id INT,
  name STRING,
  terminal STRING,
  lattitude FLOAT,
  longitude FLOAT,
  installed BOOLEAN,
  locked BOOLEAN,
  installDate STRING,
  removalDate STRING,
  temporary BOOLEAN,
  bikes INT,
  empty INT,
  docks INT
)
COMMENT 'TFL Cyclehire data (${hiveconf:cyclehire.table.modifier})'
PARTITIONED BY (
  year INT,
  month INT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001'
STORED AS SEQUENCEFILE
LOCATION '${hiveconf:cyclehire.table.location}';

MSCK REPAIR TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier};

