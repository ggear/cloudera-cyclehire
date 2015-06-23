package com.cloudera.cyclehire.main.ingress.stream;

import java.util.UUID;

public class StreamEvent {

  public static final String HEADER_TYPE = "ch_type";
  public static final String HEADER_BATCH = "ch_batch";
  public static final String HEADER_INDEX = "ch_index";
  public static final String HEADER_TOTAL = "ch_total";
  public static final String HEADER_AGENT_ID = "ch_agent_id";
  public static final String HEADER_TIMESTAMP = "ch_timestamp";

  public static final String AGENT_ID = UUID.randomUUID().toString();

  public enum Type {
    POLL, TICK
  };

}
