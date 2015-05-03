package com.cloudera.cyclehire.main.ingress;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.commons.io.Charsets;
import org.apache.commons.io.IOUtils;
import org.apache.flume.Channel;
import org.apache.flume.ChannelSelector;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.Transaction;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.channel.MemoryChannel;
import org.apache.flume.channel.ReplicatingChannelSelector;
import org.apache.flume.conf.Configurables;
import org.apache.flume.sink.hdfs.HDFSEventSink;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.DefaultHandler;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.handler.ResourceHandler;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.cloudera.cyclehire.data.DataConstants;
import com.cloudera.cyclehire.main.common.model.PartitionKey;
import com.cloudera.cyclehire.main.ingress.stream.StreamHttpSource;
import com.cloudera.cyclehire.main.test.BaseTestCase;
import com.cloudera.cyclehire.main.test.EmbeddedCoreTestCase;
import com.google.common.collect.ImmutableMap;

public class StreamTest extends EmbeddedCoreTestCase {

  private static final int HTTP_PORT = 10901;
  private static final String HTTP_URL_BASE = "http://127.0.0.1:" + HTTP_PORT
      + "/";
  private static final String HTTP_WEBROOT = PATH_LOCAL_WORKING_DIR_TARGET_DATA
      + "/cyclehire/test/xml/" + DataConstants.PATH_LOCAL_XMLS[0] + "/";
  private static final String HTTP_FILE_1 = DataConstants.PATH_LOCAL_XMLSS[0][10];
  private static final String HTTP_FILE_2 = DataConstants.PATH_LOCAL_XMLSS[0][11];
  private static final String HTTP_FILE_EMPTY = DataConstants.PATH_LOCAL_XMLSS[0][17];
  private static final String HTTP_URL_1 = HTTP_URL_BASE + HTTP_FILE_1;
  private static final String HTTP_URL_2 = HTTP_URL_BASE + HTTP_FILE_2;
  private static final String HTTP_URL_EMPTY = HTTP_URL_BASE + HTTP_FILE_EMPTY;

  private static Server server;
  private static String httpFile1;
  private static String httpFile2;

  public StreamTest() throws IOException {
    super();
  }

  @Override
  @BeforeClass
  public void setUpClass() throws Exception {
    server = new Server();
    server.setStopAtShutdown(true);
    SelectChannelConnector connector = new SelectChannelConnector();
    connector.setPort(HTTP_PORT);
    connector.setMaxIdleTime(Integer.MAX_VALUE);    
    server.setConnectors(new Connector[] { connector });
    ResourceHandler serverResourceHandler = new ResourceHandler();
    serverResourceHandler.setResourceBase(HTTP_WEBROOT);
    HandlerList serverResourceHandlers = new HandlerList();
    serverResourceHandlers.setHandlers(new Handler[] { serverResourceHandler,
        new DefaultHandler() });
    server.setHandler(serverResourceHandlers);
    server.start();
    Thread.sleep(250);
    httpFile1 = IOUtils.toString(new URL(HTTP_URL_1), Charsets.UTF_8.name());
    httpFile2 = IOUtils.toString(new URL(HTTP_URL_2), Charsets.UTF_8.name());
  }

  @Override
  @AfterClass
  public void tearDownClass() throws Exception {
    server.stop();
  }

  @Test
  public void testStreamHttpSourceInvalid() throws Exception {
    assertEquals(
        -1,
        processStreamHttpSource(new String[] { "" }, new String[] { "" },
            "250", "0", "1", 1, true, null));
    assertEquals(
        0,
        processStreamHttpSource(
            new String[] { "http://some-non-existant-host-891237081231.com/test.xml" },
            new String[] { "" }, "250", "0", "1", 1, true, null));
    assertEquals(
        0,
        processStreamHttpSource(new String[] { HTTP_URL_1
            + "-some-non-existant-resource" }, new String[] { "" }, "250", "0",
            "1", 1, true, null));
  }

  @Test
  public void testStreamHttpSourceSingle() throws Exception {
    assertEquals(
        1,
        processStreamHttpSource(new String[] { HTTP_URL_EMPTY },
            new String[] { "" }, "250", "0", "1", 1, true, null));
    assertEquals(
        1,
        processStreamHttpSource(new String[] { HTTP_URL_1 },
            new String[] { httpFile1 }, "250", "0", "1", 3, true, null));
  }

  @Test
  public void testStreamHttpSourceBatch() throws Exception {
    assertEquals(
        0,
        processStreamHttpSource(new String[] { HTTP_URL_1 },
            new String[] { httpFile1 }, "250", "0", "3", 3, true, null));
    assertEquals(
        4,
        processStreamHttpSource(new String[] { HTTP_URL_1 },
            new String[] { httpFile1 }, "250", "1", "1", 3, true, null));
    assertEquals(
        3,
        processStreamHttpSource(new String[] { HTTP_URL_1 },
            new String[] { httpFile1 }, "250", "1", "3", 3, true, null));
    assertEquals(
        28,
        processStreamHttpSource(new String[] { HTTP_URL_1 },
            new String[] { httpFile1 }, "250", "9", "1", 3, true, null));
    assertEquals(
        27,
        processStreamHttpSource(new String[] { HTTP_URL_1 },
            new String[] { httpFile1 }, "250", "9", "3", 3, true, null));
  }

  @Test
  public void testStreamHttpSourceBatchPoll() throws Exception {
    assertEquals(
        3,
        processStreamHttpSource(new String[] { HTTP_URL_1, HTTP_URL_2 },
            new String[] { httpFile1, httpFile2 }, "250", "0", "1", 3, true,
            null));
    assertEquals(
        3,
        processStreamHttpSource(new String[] { HTTP_URL_1, HTTP_URL_2 },
            new String[] { httpFile1, httpFile2 }, "250", "0", "3", 3, true,
            null));
    assertEquals(
        6,
        processStreamHttpSource(new String[] { HTTP_URL_1, HTTP_URL_2 },
            new String[] { httpFile1, httpFile2 }, "250", "1", "1", 3, true,
            null));
    assertEquals(
        6,
        processStreamHttpSource(new String[] { HTTP_URL_1, HTTP_URL_2 },
            new String[] { httpFile1, httpFile2 }, "250", "1", "3", 3, true,
            null));
    assertEquals(
        30,
        processStreamHttpSource(new String[] { HTTP_URL_1, HTTP_URL_2 },
            new String[] { httpFile1, httpFile2 }, "250", "9", "1", 3, true,
            null));
    assertEquals(
        30,
        processStreamHttpSource(new String[] { HTTP_URL_1, HTTP_URL_2 },
            new String[] { httpFile1, httpFile2 }, "250", "9", "3", 3, true,
            null));
  }

  @Test
  public void testHdfsSinkSingle() throws Exception {
    Assert.assertEquals(3, processHdfsSink(3, 1));
  }

  @Test
  public void testHdfsSinkBatch() throws Exception {
    Assert.assertEquals(12, processHdfsSink(3, 4));
  }

  private int processStreamHttpSource(String[] httpUrls,
      String[] httpResponses, String pollMs, String pollTicks,
      String batchSize, int iterations, boolean validate, Channel channel)
      throws Exception {
    int eventCount = 0;
    if (validate) {
      channel = new MemoryChannel();
      channel.setName("simple-memory-channel");
      Configurables.configure(channel,
          new Context(ImmutableMap.of("keep-alive", "1")));
      channel.start();
    }
    List<Channel> channels = new ArrayList<Channel>(1);
    channels.add(channel);
    ChannelSelector channelSelector = new ReplicatingChannelSelector();
    channelSelector.setChannels(channels);
    try {
      Context context = new Context();
      context.put(StreamHttpSource.PROPERTY_POLL_MS, pollMs);
      context.put(StreamHttpSource.PROPERTY_POLL_TICKS, pollTicks);
      context.put(StreamHttpSource.PROPERTY_BATCH_SIZE, batchSize);
      StreamHttpSource source = new StreamHttpSource();
      source.setName("simple-stream-source");
      source.setChannelProcessor(new ChannelProcessor(channelSelector));
      source.start();
      long transactionTimestamp = System.currentTimeMillis() / 1000;
      for (int i = 0; i < iterations; i++) {
        context.put(StreamHttpSource.PROPERTY_HTTP_URL, httpUrls[i
            % httpUrls.length]);
        Configurables.configure(source, context);
        source.process();
        if (validate) {
          Transaction transaction = channel.getTransaction();
          transaction.begin();
          Event event = null;
          while ((event = channel.take()) != null) {
            if (event != null) {
              Assert.assertNotNull(event.getHeaders().get(
                  StreamHttpSource.HEADER_TIMESTAMP));
              Assert.assertTrue(Long.parseLong(event.getHeaders().get(
                  StreamHttpSource.HEADER_TIMESTAMP)) <= System
                  .currentTimeMillis() / 1000);
              Assert.assertTrue(Long.parseLong(event.getHeaders().get(
                  StreamHttpSource.HEADER_TIMESTAMP)) >= transactionTimestamp);
              Assert.assertNotNull(event.getBody());
              if (Integer.parseInt(batchSize) == 1) {
                Assert.assertEquals(httpResponses[i % httpUrls.length],
                    new String(event.getBody(), Charsets.UTF_8.name()));
              }
              eventCount++;
            }
          }
          transaction.commit();
          transaction.close();
        }
      }
      source.stop();
    } catch (Exception exception) {
      eventCount = -1;
    }
    if (validate) {
      channel.stop();
    }
    return eventCount;
  }

  private int processHdfsSink(int batchCount, int batchSize) throws Exception {
    Channel channel = new MemoryChannel();
    channel.setName("simple-memory-channel");
    Configurables.configure(channel,
        new Context(ImmutableMap.of("keep-alive", "1")));
    channel.start();
    Context context = new Context();
    context.put("hdfs.path", BaseTestCase.PATH_HDFS_DIR_RAW_LANDED
        + "/xml/none/%{batch}_livecyclehireupdates-%{host}.xml");
    context.put("hdfs.filePrefix",
        "%t_livecyclehireupdates-%{index}-of-%{total}");
    context.put("hdfs.fileSuffix", ".xml");
    context.put("hdfs.inUsePrefix", "_");
    context.put("hdfs.inUseSuffix", "");
    context.put("hdfs.rollCount", "1");
    context.put("hdfs.batchSize", "" + batchSize);
    context.put("hdfs.writeFormat", "Text");
    context.put("hdfs.useRawLocalFileSystem", Boolean.toString(true));
    context.put("hdfs.fileType", "DataStream");
    HDFSEventSink sink = new HDFSEventSink();
    Configurables.configure(sink, context);
    sink.setChannel(channel);
    sink.setName("simple-hdfs-sink");
    sink.start();
    for (int i = 0; i < batchCount; i++) {
      processStreamHttpSource(new String[] { HTTP_URL_1, HTTP_URL_2 },
          new String[] { httpFile1, httpFile2 }, "250", "" + (batchSize - 1),
          "" + batchSize, 1, false, channel);
      sink.process();
    }
    sink.stop();
    int fileCount = 0;
    RemoteIterator<LocatedFileStatus> paths = getFileSystem().listFiles(
        new Path(BaseTestCase.PATH_HDFS_DIR_RAW_LANDED), true);
    while (paths.hasNext()) {
      Path path = paths.next().getPath();
      PartitionKey partitionKey = new PartitionKey().batch(
          path.getParent().getName()).record(path.getName());
      Assert.assertTrue(partitionKey.isValid());
      fileCount++;
    }
    channel.stop();
    return fileCount;
  }

  public static junit.framework.Test suite() throws Exception {
    return new StreamTest().getTestSuiteWithClassLifecycleMethods();
  }

}