--
-- Cyclehire Processed Schema Rewrite Avro
--

CREATE EXTERNAL TABLE IF NOT EXISTS cyclehire_processed_${hiveconf:cyclehire.table.modifier}_avro_${hiveconf:cyclehire.table.codec}
COMMENT 'TFL Cyclehire processed data (${hiveconf:cyclehire.table.modifier})'
PARTITIONED BY (
  year TINYINT,
  month TINYINT
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
      { "name" : "getDate", "type" : "int" },
      { "name" : "updateDate", "type" : "int" },
      { "name" : "id", "type" : "int" },
      { "name" : "name", "type" : "string" },
      { "name" : "terminal", "type" : "string" },
      { "name" : "lattitude", "type" : "float" },
      { "name" : "longitude", "type" : "float" },
      { "name" : "installed", "type" : "boolean" },
      { "name" : "locked", "type" : "boolean" },
      { "name" : "installDate", "type": [ "null", "long" ], "default":null },
      { "name" : "removalDate", "type": [ "null", "long" ], "default":null },
      { "name" : "temporary", "type" : "boolean" },
      { "name" : "bikes", "type" : "int" },
      { "name" : "empty", "type" : "int" },
      { "name" : "docks", "type" : "int" },
      { "name" : "source", "type" : "string"
      }
    ]
  }'
);

INSERT OVERWRITE TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier}_avro_${hiveconf:cyclehire.table.codec} PARTITION (year, month)
SELECT * FROM cyclehire_processed_cleansed_canonical;

MSCK REPAIR TABLE cyclehire_processed_${hiveconf:cyclehire.table.modifier}_avro_${hiveconf:cyclehire.table.codec};

