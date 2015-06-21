package com.cloudera.cyclehire.main.ingress.stream;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class StreamInterceptor implements Interceptor, StreamEvent {

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
    return putHeaderIfAbsent(first, HEADER_TIMESTAMP, "" + (timestamp / 1000))
        + "_"
        + putHeaderIfAbsent(last, HEADER_TIMESTAMP, "" + (timestamp / 1000));
  }

  private Event getEventWithHeaders(Event event, String batch, int index,
      int total, long timestamp) {
    putHeaderIfAbsent(event, HEADER_HOST, host);
    putHeaderIfAbsent(event, HEADER_TYPE, Type.POLL.toString().toLowerCase());
    putHeaderIfAbsent(event, HEADER_TIMESTAMP, "" + timestamp / 1000);
    putHeaderIfAbsent(event, HEADER_BATCH, batch);
    putHeaderIfAbsent(event, HEADER_INDEX, String.format("%03d", index + 1));
    putHeaderIfAbsent(event, HEADER_TOTAL, String.format("%03d", total));
    return event;
  }

  private static String putHeaderIfAbsent(Event event, String key, String value) {
    if (!event.getHeaders().containsKey(key)) {
      event.getHeaders().put(key, value);
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
