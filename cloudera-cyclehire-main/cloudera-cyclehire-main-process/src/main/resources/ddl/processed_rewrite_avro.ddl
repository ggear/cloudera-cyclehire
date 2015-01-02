--
-- Cyclehire Processed Schema Rewrite Avro
--

CREATE EXTERNAL TABLE IF NOT EXISTS cyclehire_processed_${hiveconf:cyclehire.table.modifier}_avro_${hiveconf:cyclehire.table.codec}
COMMENT 'TFL Cyclehire processed data (${hiveconf:cyclehire.table.modifier})'
PARTITIONED BY (
  year INT,
  month INT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${hiveconf:cyclehire.table.location}'
TBLPROPERTIES (
  'avro.schema.literal'='{
    "namespace" : "com.cloudera",
    "name" : "cylehire",
    "type" : "record",
    "fields":[
      { "name" : "polled", "type" : "long" },
      { "name" : "updated", "type" : "long" },
      { "name" : "id", "type" : "int" },
      { "name" : "name", "type" : "string" },
      { "name" : "terminal", "type" : "string" },
      { "name" : "lattitude", "type" : "float" },
      { "name" : "longitude", "type" : "float" },
      { "name" : "is_installed", "type" : "boolean" },
      { "name" : "is_locked", "type" : "boolean" },
      { "name" : "installed", "type": [ "null", "long" ], "default":null },
      { "name" : "removed", "type": [ "null", "long" ], "default":null },
      { "name" : "temporary", "type" : "boolean" },
      { "name" : "bikes", "type" : "int" },
      { "name" : "empty", "type" : "int" },
      { "name" : "docks", "type" : "int" },
      { "name" : "source", "type" : "string"
      }
    ]
  }'
);

INSERT OVERWRITE TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier}_avro_${hiveconf:cyclehire.table.codec}
PARTITION (year, month)
SELECT *
FROM cyclehire_processed_cleansed_canonical
WHERE year='${hiveconf:cyclehire.table.partition.year}' AND month='${hiveconf:cyclehire.table.partition.month}';
