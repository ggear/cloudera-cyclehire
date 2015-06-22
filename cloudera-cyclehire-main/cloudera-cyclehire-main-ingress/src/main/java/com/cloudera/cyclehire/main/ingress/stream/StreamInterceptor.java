package com.cloudera.cyclehire.main.ingress.stream;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamInterceptor implements Interceptor, StreamEvent {

  private static final Logger LOG = LoggerFactory
      .getLogger(StreamInterceptor.class);

  private String host = null;

  @Override
  public void initialize() {
    try {
      host = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME")
          : InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException exception) {
      throw new RuntimeException("Could not determine local hostname",
          exception);
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Stream Interceptor initialised");
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
    return putHeader(first, HEADER_TIMESTAMP, "" + (timestamp / 1000)) + "_"
        + putHeader(last, HEADER_TIMESTAMP, "" + (timestamp / 1000));
  }

  private Event getEventWithHeaders(Event event, String batch, int index,
      int total, long timestamp) {
    putHeader(event, HEADER_HOST, host);
    putHeader(event, HEADER_TYPE, Type.POLL.toString().toLowerCase());
    putHeader(event, HEADER_TIMESTAMP, "" + timestamp / 1000);
    putHeader(event, HEADER_BATCH, batch, true);
    putHeader(event, HEADER_INDEX, String.format("%03d", index), true);
    putHeader(event, HEADER_TOTAL, String.format("%03d", total), true);
    return event;
  }

  private static String putHeader(Event event, String key, String value) {
    return putHeader(event, key, value, false);
  }

  private static String putHeader(Event event, String key, String value,
      boolean force) {
    if (force || !event.getHeaders().containsKey(key)) {
      String valuePrevious = event.getHeaders().put(key, value);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Adding event header ["
            + key
            + " ] with value ["
            + value
            + "]"
            + (valuePrevious == null ? "" : (" overwriting previous value ["
                + valuePrevious + "]")));
      }
    }
    return value;
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
