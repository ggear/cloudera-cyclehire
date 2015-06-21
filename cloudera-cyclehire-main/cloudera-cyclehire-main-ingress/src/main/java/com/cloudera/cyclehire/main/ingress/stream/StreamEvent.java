package com.cloudera.cyclehire.main.ingress.stream;

public interface StreamEvent {

  public static String HEADER_HOST = "ch_host";
  public static String HEADER_TYPE = "ch_type";
  public static String HEADER_BATCH = "ch_batch";
  public static String HEADER_INDEX = "ch_index";
  public static String HEADER_TOTAL = "ch_total";
  public static String HEADER_TIMESTAMP = "ch_timestamp";

  public enum Type {
    POLL, TICK
  };

}
