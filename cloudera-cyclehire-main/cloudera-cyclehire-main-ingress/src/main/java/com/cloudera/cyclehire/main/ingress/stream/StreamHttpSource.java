package com.cloudera.cyclehire.main.ingress.stream;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.Charsets;
import org.apache.commons.lang.StringUtils;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.instrumentation.SourceCounter;
import org.apache.flume.source.AbstractSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamHttpSource extends AbstractSource implements Configurable,
    PollableSource {

  public static final String PROPERTY_HTTP_URL = "httpUrl";
  public static final String PROPERTY_POLL_MS = "pollMs";
  public static final String PROPERTY_POLL_TICKS = "pollTicks";
  public static final String PROPERTY_BATCH_SIZE = "batchSize";

  public static String HEADER_HOST = "host";
  public static String HEADER_TYPE = "type";
  public static String HEADER_BATCH = "batch";
  public static String HEADER_INDEX = "index";
  public static String HEADER_TOTAL = "total";
  public static String HEADER_TIMESTAMP = "timestamp";

  public enum Type {
    POLL, TICK
  };

  private static final Logger LOG = LoggerFactory
      .getLogger(StreamHttpSource.class);

  private String httpUrl = "http://www.tfl.gov.uk/tfl/"
      + "syndication/feeds/cycle-hire/livecyclehireupdates.xml";
  private int pollMs = 1000;
  private int pollTicks = 0;
  private int batchSize = 1;

  private String host = null;
  private HttpClient httpClient;
  private String eventBodyCache;
  private List<Event> eventBatch;
  private SourceCounter sourceCounter;

  @Override
  public void configure(Context context) {
    httpUrl = context.getString(PROPERTY_HTTP_URL, httpUrl);
    if (StringUtils.isEmpty(httpUrl)) {
      throw new IllegalArgumentException("Source [" + getName()
          + "] has illegal paramater [" + PROPERTY_HTTP_URL + "] value ["
          + httpUrl + "]");
    }
    pollMs = context.getInteger(PROPERTY_POLL_MS, pollMs);
    if (pollMs <= 0) {
      throw new IllegalArgumentException("Source [" + getName()
          + "] has illegal paramater [" + PROPERTY_POLL_MS + "] value ["
          + pollMs + "]");
    }
    pollTicks = context.getInteger(PROPERTY_POLL_TICKS, pollTicks);
    if (pollTicks < 0) {
      throw new IllegalArgumentException("Source [" + getName()
          + "] has illegal paramater [" + PROPERTY_POLL_TICKS + "] value ["
          + pollTicks + "]");
    }
    batchSize = context.getInteger(PROPERTY_BATCH_SIZE, batchSize);
    if (batchSize < 1) {
      throw new IllegalArgumentException("Source [" + getName()
          + "] has illegal paramater [" + PROPERTY_BATCH_SIZE + "] value ["
          + batchSize + "]");
    }
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Source [" + getName() + "] configured with context [" + context
          + "]");
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    try {
      host = System.getenv("HOSTNAME") != null ? System.getenv("HOSTNAME")
          : InetAddress.getLocalHost().getCanonicalHostName();
    } catch (UnknownHostException exception) {
      throw new RuntimeException("Could not determine local hostname",
          exception);
    }
    httpClient = new HttpClient();
    httpClient.getHttpConnectionManager().getParams()
        .setConnectionTimeout(pollMs);
    httpClient.getParams().setParameter("http.protocol.content-charset",
        "UTF-8");
    eventBodyCache = null;
    eventBatch = new ArrayList<Event>();
    sourceCounter.start();
    if (LOG.isInfoEnabled()) {
      LOG.info("Source [" + getName() + "] started");
    }
  }

  @Override
  public synchronized void stop() {
    processEvent(null, true);
    sourceCounter.stop();
    super.stop();
    if (LOG.isInfoEnabled()) {
      LOG.info("Source [" + getName() + "] stopped, metrics [" + sourceCounter
          + "]");
    }
  }

  private Map<String, String> getEventHeader(long timestamp, Type type) {
    Map<String, String> header = new HashMap<String, String>();
    header.put(HEADER_HOST, host);
    header.put(HEADER_TYPE, type.toString().toLowerCase());
    header.put(HEADER_TIMESTAMP, "" + timestamp / 1000);
    header.put(HEADER_BATCH, "" + timestamp / 1000);
    return header;
  }

  private Map<String, String> setEventHeader(Event event, String batch,
      int index, int total) {
    event.getHeaders().put(HEADER_BATCH, batch);
    event.getHeaders().put(HEADER_INDEX, String.format("%03d", index + 1));
    event.getHeaders().put(HEADER_TOTAL, String.format("%03d", total));
    return event.getHeaders();
  }

  private synchronized void processEvent(Event event, boolean flush) {
    if (event != null) {
      eventBatch.add(event);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] batched event, buffered events ["
            + eventBatch.size() + "]");
      }
    }
    if (eventBatch.size() > 0 && (flush || eventBatch.size() == batchSize)) {
      String batch = eventBatch.get(0).getHeaders().get(HEADER_TIMESTAMP)
          + "_"
          + eventBatch.get(eventBatch.size() - 1).getHeaders()
              .get(HEADER_TIMESTAMP);
      for (int i = 0; i < eventBatch.size(); i++) {
        setEventHeader(eventBatch.get(i), batch, i, eventBatch.size());
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName()
            + "] pending commit, buffered events [" + eventBatch.size() + "]");
      }
      getChannelProcessor().processEventBatch(eventBatch);
      eventBatch.clear();
      sourceCounter.addToEventAcceptedCount(eventBatch.size());
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] post commit, buffered events ["
            + eventBatch.size() + "]");
      }
    }
  }

  @Override
  public Status process() throws EventDeliveryException {
    long time = System.currentTimeMillis();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Source [" + getName() + "] process started");
    }
    Status status = Status.BACKOFF;
    GetMethod httpClientGet = new GetMethod(httpUrl);
    try {
      int httpClientResponseCode = -1;
      long httpMs = System.currentTimeMillis();
      if ((httpClientResponseCode = httpClient.executeMethod(httpClientGet)) != HttpStatus.SC_OK) {
        throw new HttpException("Unexpected HTTP code returned ["
            + httpClientResponseCode + "]");
      }
      long httpClientGetTimestamp = System.currentTimeMillis();
      String httpClientGetResponse = httpClientGet.getResponseBodyAsString();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] polled HTTP source, ms ["
            + (System.currentTimeMillis() - httpMs) + "]");
      }
      if (eventBodyCache == null
          || !eventBodyCache.equals(httpClientGetResponse)) {
        sourceCounter.incrementEventReceivedCount();
        processEvent(
            EventBuilder.withBody(httpClientGetResponse,
                Charset.forName(Charsets.UTF_8.name()),
                getEventHeader(httpClientGetTimestamp, Type.POLL)), false);
        eventBodyCache = httpClientGetResponse;
      }
      int tickMs = pollMs / (pollTicks + 1);
      int sleepMs = 0;
      for (int i = 0; i <= pollTicks; i++) {
        if (pollTicks > 0 && i < pollTicks) {
          sourceCounter.incrementEventReceivedCount();
          processEvent(
              EventBuilder.withBody(httpClientGetResponse,
                  Charset.forName(Charsets.UTF_8.name()),
                  getEventHeader(System.currentTimeMillis(), Type.TICK)), false);
          if (i < pollTicks - 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Source [" + getName()
                  + "] sleeping for next tick, ms [" + tickMs + "]");
            }
          }
        }
        if (i == pollTicks) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source [" + getName() + "] sleeping for next poll, ms ["
                + tickMs + "]");
          }
        }
        sleepMs += tickMs;
        Thread.sleep(tickMs);
        if (pollTicks > 0 && i < pollTicks - 1) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source [" + getName()
                + "] waking up for next tick, ms [" + tickMs + "]");
          }
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] waking up for next poll, ms ["
            + sleepMs + "]");
      }
      status = Status.READY;
    } catch (InterruptedException interruptedException) {
      if (LOG.isInfoEnabled()) {
        LOG.info("Source [" + getName() + "] interupted");
      }
    } catch (Error error) {
      throw error;
    } catch (Exception exception) {
      if (LOG.isErrorEnabled()) {
        LOG.error("Source [" + getName()
            + "] encountered exception processing event, "
            + "backing off and retrying", exception);
      }
    } finally {
      httpClientGet.releaseConnection();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Source [" + getName() + "] process stopped, ms ["
          + (System.currentTimeMillis() - time) + "]");
    }
    return status;
  }
}
