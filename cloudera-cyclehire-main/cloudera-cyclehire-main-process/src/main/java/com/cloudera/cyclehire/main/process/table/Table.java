package com.cloudera.cyclehire.main.process.table;

public interface Table {

  public static final String DDL_LOCATION = "/ddl";

  public static final String DDL_LOCATION_PARTITIONED_CREATE = "partitioned_create.ddl";
  public static final String DDL_LOCATION_PROCESSED_CREATE = "processed_create.ddl";

  public static final String DDL_LOCATION_PROCESSED_REWRITE_AVRO = "processed_rewrite_avro.ddl";
  public static final String DDL_LOCATION_PROCESSED_REWRITE_PARQUET = "processed_rewrite_parquet.ddl";
  public static final String DDL_LOCATION_PROCESSED_REWRITE_SEQUENCE = "processed_rewrite_sequence.ddl";

  public static final String[] DDL_LOCATION_PROCESSED_REWRITE_FORMATS = new String[] {
      "avro/none", "parquet/dict", "sequence/snappy" };

  public static final String DDL_CONFIG_TABLE_PARTITION_YEAR = "cyclehire.table.partition.year";
  public static final String DDL_CONFIG_TABLE_PARTITION_MONTH = "cyclehire.table.partition.month";

  public static final String DDL_CONFIG_TABLE_CODEC = "cyclehire.table.codec";
  public static final String DDL_CONFIG_TABLE_MODIFIER = "cyclehire.table.modifier";
  public static final String DDL_CONFIG_TABLE_LOCATION = "cyclehire.table.location";

}
