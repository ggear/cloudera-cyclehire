package com.cloudera.cyclehire.main.ingress.stream;

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

public class StreamSource extends AbstractSource implements Configurable, PollableSource {

  public static final String PROPERTY_HTTP_URL = "httpUrl";
  public static final String PROPERTY_POLL_MS = "pollMs";
  public static final String PROPERTY_POLL_TICKS = "pollTicks";
  public static final String PROPERTY_BATCH_SIZE = "batchSize";

  private static final Logger LOG = LoggerFactory.getLogger(StreamSource.class);

  private static final int HTTP_REPONSE_MAX_BYTES = 500000;

  private String httpUrl = "http://www.tfl.gov.uk/tfl/" + "syndication/feeds/cycle-hire/livecyclehireupdates.xml";
  private int pollMs = 1000;
  private int pollTicks = 0;
  private int batchSize = 1;

  private HttpClient httpClient;
  private String eventBodyCache;
  private List<Event> eventBatch;
  private SourceCounter sourceCounter;

  @Override
  public void configure(Context context) {
    httpUrl = context.getString(PROPERTY_HTTP_URL, httpUrl);
    if (StringUtils.isEmpty(httpUrl)) {
      throw new IllegalArgumentException("Source [" + getName() + "] has illegal paramater [" + PROPERTY_HTTP_URL
          + "] value [" + httpUrl + "]");
    }
    pollMs = context.getInteger(PROPERTY_POLL_MS, pollMs);
    if (pollMs <= 0) {
      throw new IllegalArgumentException("Source [" + getName() + "] has illegal paramater [" + PROPERTY_POLL_MS
          + "] value [" + pollMs + "]");
    }
    pollTicks = context.getInteger(PROPERTY_POLL_TICKS, pollTicks);
    if (pollTicks < 0) {
      throw new IllegalArgumentException("Source [" + getName() + "] has illegal paramater [" + PROPERTY_POLL_TICKS
          + "] value [" + pollTicks + "]");
    }
    batchSize = context.getInteger(PROPERTY_BATCH_SIZE, batchSize);
    if (batchSize < 1) {
      throw new IllegalArgumentException("Source [" + getName() + "] has illegal paramater [" + PROPERTY_BATCH_SIZE
          + "] value [" + batchSize + "]");
    }
    if (sourceCounter == null) {
      sourceCounter = new SourceCounter(getName());
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("Source [" + getName() + "] configured, Agent ID [" + StreamEvent.AGENT_ID + "], context [" + context
          + "]");
    }
  }

  @Override
  public synchronized void start() {
    super.start();
    httpClient = new HttpClient();
    httpClient.getHttpConnectionManager().getParams().setConnectionTimeout(pollMs);
    httpClient.getParams().setParameter("http.protocol.content-charset", "UTF-8");
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
      LOG.info("Source [" + getName() + "] stopped, metrics [" + sourceCounter + "]");
    }
  }

  private Map<String, String> getEventHeader(long timestamp, StreamEvent.Type type) {
    Map<String, String> header = new HashMap<String, String>();
    header.put(StreamEvent.HEADER_AGENT_ID, StreamEvent.AGENT_ID);
    header.put(StreamEvent.HEADER_TYPE, type.toString().toLowerCase());
    header.put(StreamEvent.HEADER_TIMESTAMP, "" + timestamp / 1000);
    return header;
  }

  private synchronized void processEvent(Event event, boolean flush) {
    if (event != null) {
      eventBatch.add(event);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] batched event, buffered events [" + eventBatch.size() + "]");
      }
    }
    if (eventBatch.size() > 0 && (flush || eventBatch.size() == batchSize)) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] pending commit, buffered events [" + eventBatch.size() + "]");
      }
      if (batchSize == 1 && eventBatch.size() == 1) {
        sourceCounter.incrementAppendReceivedCount();
        sourceCounter.incrementEventReceivedCount();
        getChannelProcessor().processEvent(eventBatch.get(0));
        sourceCounter.incrementAppendAcceptedCount();
        sourceCounter.incrementEventAcceptedCount();
      } else {
        sourceCounter.incrementAppendBatchReceivedCount();
        sourceCounter.addToEventReceivedCount(eventBatch.size());
        getChannelProcessor().processEventBatch(eventBatch);
        sourceCounter.incrementAppendBatchAcceptedCount();
        sourceCounter.addToEventAcceptedCount(eventBatch.size());
      }
      eventBatch.clear();
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] post commit, buffered events [" + eventBatch.size() + "]");
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
        throw new HttpException("Unexpected HTTP code returned [" + httpClientResponseCode + "]");
      }
      long httpClientGetTimestamp = System.currentTimeMillis();
      String httpClientGetResponse = httpClientGet.getResponseBodyAsString(HTTP_REPONSE_MAX_BYTES);
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] polled HTTP source, ms [" + (System.currentTimeMillis() - httpMs) + "]");
      }
      boolean tickRequired = true;
      if (eventBodyCache == null || !eventBodyCache.equals(httpClientGetResponse)) {
        tickRequired = false;
        processEvent(
            EventBuilder.withBody(httpClientGetResponse, Charset.forName(Charsets.UTF_8.name()),
                getEventHeader(httpClientGetTimestamp, StreamEvent.Type.POLL)), false);
        eventBodyCache = httpClientGetResponse;
      }
      int tickMs = pollMs / (pollTicks + 1);
      int sleepMs = 0;
      for (int i = 0; i <= pollTicks; i++) {
        if (pollTicks > 0 && i < pollTicks) {
          if (tickRequired) {
            processEvent(
                EventBuilder.withBody(httpClientGetResponse, Charset.forName(Charsets.UTF_8.name()),
                    getEventHeader(System.currentTimeMillis(), StreamEvent.Type.TICK)), false);
          } else {
            tickRequired = true;
          }
          if (i < pollTicks - 1) {
            if (LOG.isDebugEnabled()) {
              LOG.debug("Source [" + getName() + "] sleeping for next tick, ms [" + tickMs + "]");
            }
          }
        }
        if (i == pollTicks) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source [" + getName() + "] sleeping for next poll, ms [" + tickMs + "]");
          }
        }
        sleepMs += tickMs;
        Thread.sleep(tickMs);
        if (pollTicks > 0 && i < pollTicks - 1) {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Source [" + getName() + "] waking up for next tick, ms [" + tickMs + "]");
          }
        }
      }
      if (LOG.isDebugEnabled()) {
        LOG.debug("Source [" + getName() + "] waking up for next poll, ms [" + sleepMs + "]");
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
        LOG.error("Source [" + getName() + "] encountered exception processing event, " + "backing off and retrying",
            exception);
      }
    } finally {
      httpClientGet.releaseConnection();
    }
    if (LOG.isDebugEnabled()) {
      LOG.debug("Source [" + getName() + "] process stopped, ms [" + (System.currentTimeMillis() - time) + "]");
    }
    return status;
  }
}
