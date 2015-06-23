package com.cloudera.cyclehire.main.ingress.stream;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamInterceptor implements Interceptor {

  private static final Logger LOG = LoggerFactory
      .getLogger(StreamInterceptor.class);

  @Override
  public void initialize() {
    if (LOG.isInfoEnabled()) {
      LOG.info("Stream Interceptor initialised, Agent ID ["
          + StreamEvent.AGENT_ID + "]");
    }
  }

  @Override
  public Event intercept(Event event) {
    long timestamp = System.currentTimeMillis();
    return getEventWithHeaders(event,
        getEventBatchHeader(event, event, timestamp), 1, 1, timestamp);
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    long timestamp = System.currentTimeMillis();
    String batch = getEventBatchHeader(events.get(0),
        events.get(events.size() - 1), timestamp);
    for (int i = 0; i < events.size(); i++) {
      getEventWithHeaders(events.get(i), batch, i + 1, events.size(), timestamp);
    }
    return events;
  }

  @Override
  public void close() {
  }

  private String getEventBatchHeader(Event first, Event last, long timestamp) {
    return putHeader(first, StreamEvent.HEADER_TIMESTAMP, "" + timestamp / 1000)
        + "_"
        + putHeader(last, StreamEvent.HEADER_TIMESTAMP, "" + timestamp / 1000);
  }

  private Event getEventWithHeaders(Event event, String batch, int index,
      int total, long timestamp) {
    putHeader(event, StreamEvent.HEADER_AGENT_ID, StreamEvent.AGENT_ID);
    putHeader(event, StreamEvent.HEADER_TYPE, StreamEvent.Type.POLL.toString()
        .toLowerCase());
    putHeader(event, StreamEvent.HEADER_TIMESTAMP, "" + timestamp / 1000);
    putHeader(event, StreamEvent.HEADER_BATCH, batch, true);
    putHeader(event, StreamEvent.HEADER_INDEX, String.format("%03d", index),
        true);
    putHeader(event, StreamEvent.HEADER_TOTAL, String.format("%03d", total),
        true);
    return event;
  }

  private static String putHeader(Event event, String key, String value) {
    return putHeader(event, key, value, false);
  }

  private static String putHeader(Event event, String key, String value,
      boolean force) {
    String valuePrevious = event.getHeaders().get(key);
    if (force || valuePrevious == null) {
      event.getHeaders().put(key, value);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding event header ["
            + key
            + " ] with value ["
            + value
            + "]"
            + (valuePrevious == null ? "" : " overwriting previous value ["
                + valuePrevious + "]"));
      }
    }
    return force || valuePrevious == null ? value : valuePrevious;
  }

  public static class Builder implements Interceptor.Builder {

    @Override
    public void configure(Context context) {
    }

    @Override
    public Interceptor build() {
      return new StreamInterceptor();
    }

  }

}
