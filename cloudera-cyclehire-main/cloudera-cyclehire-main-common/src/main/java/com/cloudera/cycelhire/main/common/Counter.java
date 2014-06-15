package com.cloudera.cycelhire.main.common;

public enum Counter {

  // File counters
  FILES, FILES_PENDING, FILES_SUCCESSFUL, FILES_SKIPPED, FILES_FAILED,

  // Partition counters
  PARTITIONS, PARTITIONS_SUCCESSFUL, PARTITIONS_SKIPPED, PARTITIONS_FAILED,

  // Record counters
  RECORDS, RECORDS_VALID("cleansed/"), RECORDS_MALFORMED("erroneous/malformed/"), RECORDS_DUPLICATE(
      "erroneous/duplicate/");

  private String path;

  Counter() {
  }

  Counter(String path) {
    this.path = path;
  }

  public String getPath() {
    return path == null ? "" : path;
  }

}
