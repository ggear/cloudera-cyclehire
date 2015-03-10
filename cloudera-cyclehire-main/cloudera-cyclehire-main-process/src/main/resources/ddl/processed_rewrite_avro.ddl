--
-- Cyclehire Processed Schema Rewrite Avro
--

CREATE EXTERNAL TABLE IF NOT EXISTS ${hiveconf:cyclehire.table.name}
COMMENT 'TFL Cyclehire processed data'
PARTITIONED BY (
  year SMALLINT,
  month TINYINT
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'
STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
LOCATION '${hiveconf:cyclehire.table.location}'
TBLPROPERTIES (
  'avro.schema.literal'='{
    "namespace": "com.cloudera",
    "name": "cylehire",
    "type": "record",
    "fields":[
      { "name": "polled", "type": "long" },
      { "name": "updated", "type": "long" },
      { "name": "id", "type": "int" },
      { "name": "name", "type": "string" },
      { "name": "terminal", "type": "string" },
      { "name": "lattitude", "type": "float" },
      { "name": "longitude", "type": "float" },
      { "name": "is_installed", "type": "boolean" },
      { "name": "is_locked", "type": "boolean" },
      { "name": "installed", "type": [ "null", "long" ], "default": null },
      { "name": "removed", "type": [ "null", "long" ], "default": null },
      { "name": "temporary", "type": "boolean" },
      { "name": "bikes", "type": "int" },
      { "name": "empty", "type": "int" },
      { "name": "docks", "type": "int" },
      { "name": "source", "type": ["null", "string"], "default": null }
    ]
  }'
);

INSERT OVERWRITE TABLE ${hiveconf:cyclehire.table.name}
PARTITION (year, month)
SELECT *
FROM cyclehire_processed_cleansed_canonical
WHERE year='${hiveconf:cyclehire.table.partition.year}' AND month='${hiveconf:cyclehire.table.partition.month}';
