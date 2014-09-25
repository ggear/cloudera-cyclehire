package com.cloudera.cyclehire.main.common;

public enum Counter {

  // File counters
  FILES, FILES_SUCCESSFUL, FILES_SKIPPED, FILES_FAILED,

  // Batch counters
  BATCHES, BATCHES_SUCCESSFUL("valid"), BATCHES_SKIPPED, BATCHES_FAILED(
      "invalid"),

  // Partition counters
  PARTITIONS, PARTITIONS_SUCCESSFUL, PARTITIONS_SKIPPED, PARTITIONS_FAILED,

  // Record counters
  RECORDS, RECORDS_CLEANSED("cleansed/canonical"), RECORDS_MALFORMED(
      "erroneous/malformed"), RECORDS_DUPLICATE("erroneous/duplicate"), RECORDS_REWRITE(
      "cleansed/rewrite");

  private String path = "";

  Counter() {
  }

  Counter(String path) {
    this.path = path;
  }

  public String getPath() {
    return path;
  }

}
