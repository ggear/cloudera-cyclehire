package com.cloudera.cycelhire.main.common.model;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Matcher;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.cyclehire.data.DataConstants;

public class PartitionKeyTest {

  private static String TAR_CODEC = "gz";
  private static String TAR_EXTENSION = "tar";
  private static String TAR_REPO = DataConstants.PATH_LOCAL_TARS[0];

  private static String XML_CODEC = PartitionKey.CODEC_NONE_LABEL;
  private static String XML_EXTENSION = "xml";
  private static String XML_REPO = DataConstants.PATH_LOCAL_XMLS[0];

  private static Long EPOCH_GET;
  private static Long EPOCH_UPDATE;
  private static String[] PARTITION;
  private static String[] PARTITIONS;
  private static long PARTITION_EPOCH_MIN;
  private static long PARTITION_EPOCH_MAX;
  private static String FILE = DataConstants.PATH_LOCAL_XMLSS[0][0];
  static {
    Matcher matcherFile = PartitionKey.REGEX_RECORD.matcher(FILE);
    matcherFile.matches();
    EPOCH_GET = 1000 * Long.parseLong(matcherFile.group(1));
    EPOCH_UPDATE = EPOCH_GET - 50000;
    Calendar calendar = PartitionKey.getCalendarMonth(EPOCH_GET);
    PARTITION = new String[] { calendar.get(Calendar.YEAR) + "/" + (calendar.get(Calendar.MONTH) + 1) };
    List<String> partitions = new ArrayList<String>();
    Matcher matcherBatch = PartitionKey.REGEX_BATCH.matcher(XML_REPO);
    matcherBatch.matches();
    PARTITION_EPOCH_MIN = 1000 * Long.parseLong(matcherBatch.group(1));
    PARTITION_EPOCH_MAX = 1000 * Long.parseLong(matcherBatch.group(2));
    Calendar calendarMin = PartitionKey.getCalendarMonth(PARTITION_EPOCH_MIN);
    Calendar calendarMax = PartitionKey.getCalendarMonth(PARTITION_EPOCH_MAX);
    while (calendarMin.before(calendarMax)) {
      partitions.add("" + calendarMin.get(Calendar.YEAR) + "/" + (calendarMin.get(Calendar.MONTH) + 1));
      calendarMin.add(Calendar.MONTH, 1);
    }
    PARTITIONS = partitions.toArray(new String[partitions.size()]);
  }

  @Test
  public void testInvalid() {
    Assert.assertEquals(false, new PartitionKey().isValid());
    Assert.assertEquals(false, new PartitionKey().record(FILE).isValid());
    Assert.assertEquals(false, new PartitionKey().epochGet(EPOCH_GET).isValid());
    Assert.assertEquals(false, new PartitionKey().epochUpdate(EPOCH_UPDATE).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(null).isValid());
    Assert.assertEquals(false, new PartitionKey().batch("").isValid());
    Assert.assertEquals(false, new PartitionKey().batch("some-rubbish").isValid());
    Assert.assertEquals(false, new PartitionKey().batch(FILE).isValid());
    Assert.assertEquals(false, new PartitionKey().batch("some-rubbish" + TAR_REPO).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO.substring(1, TAR_REPO.length())).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO).record("some-rubbish").isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO).record("some-rubbish" + FILE).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO).record(FILE.substring(1, FILE.length())).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO).record("2" + FILE.substring(1, FILE.length()))
        .isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO).epochGet(1L).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO).epochGet(PARTITION_EPOCH_MIN - 1).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO).epochGet(PARTITION_EPOCH_MAX + 1).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO).record(FILE).epochGet(EPOCH_GET + 1).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO).record(FILE).epochGet(EPOCH_GET - 1).isValid());
  }

  @Test
  public void testBatchTar() {
    PartitionKey partitionKey = new PartitionKey().batch(TAR_REPO).build();
    Assert.assertEquals("", partitionKey.getRecord());
    Assert.assertEquals(TAR_REPO, partitionKey.getBatch());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
    Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
    Assert.assertEquals(Arrays.asList(PARTITIONS), partitionKey.getPartitions());
  }

  @Test
  public void testBatchXML() {
    PartitionKey partitionKey = new PartitionKey().batch(XML_REPO).build();
    Assert.assertEquals("", partitionKey.getRecord());
    Assert.assertEquals(XML_REPO, partitionKey.getBatch());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
    Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
    Assert.assertEquals(Arrays.asList(PARTITIONS), partitionKey.getPartitions());
  }

  @Test
  public void testRecordBatchTar() {
    PartitionKey partitionKey = new PartitionKey().batch(TAR_REPO).record(FILE).build();
    Assert.assertEquals(FILE, partitionKey.getRecord());
    Assert.assertEquals(TAR_REPO, partitionKey.getBatch());
    Assert.assertEquals(EPOCH_GET, partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
    Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
    Assert.assertEquals(Arrays.asList(PARTITION), partitionKey.getPartitions());
  }

  @Test
  public void testRecordBatchXML() {
    PartitionKey partitionKey = new PartitionKey().batch(XML_REPO).record(FILE).build();
    Assert.assertEquals(FILE, partitionKey.getRecord());
    Assert.assertEquals(XML_REPO, partitionKey.getBatch());
    Assert.assertEquals(EPOCH_GET, partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
    Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
    Assert.assertEquals(Arrays.asList(PARTITION), partitionKey.getPartitions());
  }

  @Test
  public void testRecordBatchUpdateTar() {
    PartitionKey partitionKey = new PartitionKey().batch(TAR_REPO).record(FILE).epochUpdate(EPOCH_UPDATE).build();
    Assert.assertEquals(FILE, partitionKey.getRecord());
    Assert.assertEquals(TAR_REPO, partitionKey.getBatch());
    Assert.assertEquals(EPOCH_GET, partitionKey.getEpochGet());
    Assert.assertEquals(EPOCH_UPDATE, partitionKey.getEpochUpdate());
    Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
    Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
    Assert.assertEquals(Arrays.asList(PARTITION), partitionKey.getPartitions());
  }

  @Test
  public void testRecordBatchUpdateXML() {
    PartitionKey partitionKey = new PartitionKey().batch(XML_REPO).record(FILE).epochUpdate(EPOCH_UPDATE).build();
    Assert.assertEquals(FILE, partitionKey.getRecord());
    Assert.assertEquals(XML_REPO, partitionKey.getBatch());
    Assert.assertEquals(EPOCH_GET, partitionKey.getEpochGet());
    Assert.assertEquals(EPOCH_UPDATE, partitionKey.getEpochUpdate());
    Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
    Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
    Assert.assertEquals(Arrays.asList(PARTITION), partitionKey.getPartitions());
  }

}