package com.cloudera.cyclehire.main.process.table;

public interface Table {

  public static final String DDL_LOCATION = "/com/cloudera/cyclehire/main/process/table";
  public static final String DDL_LOCATION_PROCESSED_CREATE = "processed_create.sql";
  public static final String DDL_LOCATION_PROCESSED_DROP = "processed_drop.sql";

  public static final String DDL_CONFIG_TABLE_MODIFIER = "cyclehire.table.modifier";
  public static final String DDL_CONFIG_TABLE_LOCATION = "cyclehire.table.location";

}
