package com.cloudera.cyclehire.main.ingress.stream;

import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

public class StreamInterceptor implements Interceptor, StreamEvent {

  @Override
  public void initialize() {
  }

  @Override
  public Event intercept(Event event) {
    return addEventBatchHeaders(
        event,
        getEventBatchAddTimestampHeaders(event, event,
            System.currentTimeMillis()), 1, 1);
  }

  @Override
  public List<Event> intercept(List<Event> events) {
    long timestamp = System.currentTimeMillis();
    String batch = getEventBatchAddTimestampHeaders(events.get(0),
        events.get(events.size() - 1), timestamp);
    for (int i = 0; i < events.size(); i++) {
      getEventTimestampAddEventTimestampHeaders(events.get(0), timestamp);
      addEventBatchHeaders(events.get(i), batch, i + 1, events.size());
    }
    return events;
  }

  @Override
  public void close() {
  }

  private String getEventBatchAddTimestampHeaders(Event first, Event last,
      long timestamp) {
    return getEventTimestampAddEventTimestampHeaders(first, timestamp) + "_"
        + getEventTimestampAddEventTimestampHeaders(last, timestamp);
  }

  private String getEventTimestampAddEventTimestampHeaders(Event event,
      long timestamp) {
    String timestampHeader = event.getHeaders().get(HEADER_TIMESTAMP);
    if (timestampHeader == null) {
      event.getHeaders().put(HEADER_TIMESTAMP,
          timestampHeader = "" + timestamp / 1000);
    }
    return timestampHeader;
  }

  private Event addEventBatchHeaders(Event event, String batch, int index,
      int total) {
    event.getHeaders().put(HEADER_BATCH, batch);
    event.getHeaders().put(HEADER_INDEX, String.format("%03d", index + 1));
    event.getHeaders().put(HEADER_TOTAL, String.format("%03d", total));
    return event;
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
