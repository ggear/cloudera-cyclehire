package com.cloudera.cyclehire.main.ingress.stream;

public interface StreamEvent {

  public static String HEADER_HOST = "host";
  public static String HEADER_TYPE = "type";
  public static String HEADER_BATCH = "batch";
  public static String HEADER_INDEX = "index";
  public static String HEADER_TOTAL = "total";
  public static String HEADER_TIMESTAMP = "timestamp";

  public enum Type {
    POLL, TICK
  };

}
