package com.cloudera.cyclehire.main.common.model;

import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.cyclehire.data.DataConstants;
import com.cloudera.cyclehire.main.common.mapreduce.MapReduceUtil;

public class PartitionKeyTest {

  private static String SEQ_CODEC = "snappy";
  private static String SEQ_EXTENSION = "sequence";

  private static String TAR_CODEC = "gz";
  private static String TAR_EXTENSION = "tar";
  private static String TAR_REPO = DataConstants.PATH_LOCAL_TARS[0];

  private static String XML_CODEC = MapReduceUtil.CODEC_NONE;
  private static String XML_EXTENSION = "xml";
  private static String XML_REPO = DataConstants.PATH_LOCAL_XMLS[0];

  private static Long EPOCH_POLL;
  private static Long EPOCH_UPDATE;
  private static long PARTITION_EPOCH_MIN;
  private static long PARTITION_EPOCH_MAX;
  private static Set<String> PARTITION = new TreeSet<String>();
  private static Set<String> PARTITIONS = new TreeSet<String>();
  private static Set<String> PARTITIONS_UNKNOWN = new TreeSet<String>(
      Arrays.asList(new String[] { PartitionFlag._UNKNOWN.toString() }));
  private static String PATH_UNKNOWN_PARENT = '/'
      + PartitionFlag._UNKNOWN.toString() + '/'
      + PartitionFlag._UNKNOWN.toString() + '/'
      + PartitionFlag._UNKNOWN.toString();
  private static String PATH_UNKNOWN = PATH_UNKNOWN_PARENT + '/'
      + PartitionFlag._UNKNOWN.toString() + '/'
      + PartitionFlag._UNKNOWN.toString();
  private static String FILE = DataConstants.PATH_LOCAL_XMLSS[0][50];
  static {
    Matcher matcherFile = PartitionKey.REGEX_RECORD.matcher(FILE);
    matcherFile.matches();
    EPOCH_POLL = 1000 * Long.parseLong(matcherFile.group(1));
    EPOCH_UPDATE = EPOCH_POLL - 50000;
    Calendar calendar = PartitionKey.getCalendarMonth(EPOCH_POLL);
    PARTITION.add("year=" + calendar.get(Calendar.YEAR) + "/month="
        + String.format("%02d", calendar.get(Calendar.MONTH) + 1));
    Matcher matcherBatch = PartitionKey.REGEX_BATCH.matcher(XML_REPO);
    matcherBatch.matches();
    PARTITION_EPOCH_MIN = 1000 * Long.parseLong(matcherBatch.group(1));
    PARTITION_EPOCH_MAX = 1000 * Long.parseLong(matcherBatch.group(2));
    Calendar calendarMin = PartitionKey.getCalendarMonth(PARTITION_EPOCH_MIN);
    Calendar calendarMax = PartitionKey.getCalendarMonth(PARTITION_EPOCH_MAX);
    while (calendarMin.before(calendarMax)) {
      PARTITIONS.add("year=" + calendarMin.get(Calendar.YEAR) + "/month="
          + String.format("%02d", calendarMin.get(Calendar.MONTH) + 1));
      calendarMin.add(Calendar.MONTH, 1);
    }
  }

  @Test
  public void testInvalid() {
    Assert.assertEquals(false, new PartitionKey().isValid());
    Assert.assertEquals(false, new PartitionKey().record(FILE).isValid());
    Assert.assertEquals(false, new PartitionKey().epochPoll(EPOCH_POLL)
        .isValid());
    Assert.assertEquals(false, new PartitionKey().epochUpdate(EPOCH_UPDATE)
        .isValid());
    Assert.assertEquals(false, new PartitionKey().batch(null).isValid());
    Assert.assertEquals(false, new PartitionKey().batch("").isValid());
    Assert.assertEquals(false, new PartitionKey().batch("some-rubbish")
        .isValid());
    Assert.assertEquals(false, new PartitionKey().batch(FILE).isValid());
    Assert.assertEquals(false,
        new PartitionKey().batch("some-rubbish" + TAR_REPO).isValid());
    Assert.assertEquals(false,
        new PartitionKey().batch(TAR_REPO.substring(1, TAR_REPO.length()))
            .isValid());
    Assert.assertEquals(
        false,
        new PartitionKey().batch(
            TAR_REPO.replace("" + PARTITION_EPOCH_MIN / 1000, ""
                + (PARTITION_EPOCH_MAX / 1000 + 1))).isValid());
    Assert.assertEquals(false,
        new PartitionKey().batch(TAR_REPO).record("some-rubbish").isValid());
    Assert.assertEquals(false,
        new PartitionKey().batch(TAR_REPO).record("some-rubbish" + FILE)
            .isValid());
    Assert.assertEquals(
        false,
        new PartitionKey().batch(TAR_REPO)
            .record(FILE.substring(1, FILE.length())).isValid());
    Assert.assertEquals(
        false,
        new PartitionKey().batch(TAR_REPO)
            .record("9" + FILE.substring(1, FILE.length())).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO).epochPoll(1L)
        .isValid());
    Assert.assertEquals(false,
        new PartitionKey().batch(TAR_REPO).epochPoll(PARTITION_EPOCH_MIN - 1)
            .isValid());
    Assert.assertEquals(false,
        new PartitionKey().batch(TAR_REPO).epochPoll(PARTITION_EPOCH_MAX + 1)
            .isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO).record(FILE)
        .epochPoll(EPOCH_POLL * 2).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(TAR_REPO).record(FILE)
        .epochPoll(EPOCH_POLL * -1L).isValid());
    Assert.assertEquals(false,
        new PartitionKey().batch(TAR_REPO).partition("some-rubbish").isValid());
    Assert.assertEquals(
        false,
        new PartitionKey().batch(TAR_REPO)
            .partition(PARTITION.iterator().next().replace("20", "80"))
            .isValid());
  }

  @Test
  public void testInvalidType() {
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .record(FILE).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .epochPoll(EPOCH_POLL).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .epochUpdate(EPOCH_UPDATE).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(null).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch("").getType());
    Assert.assertEquals(
        TAR_EXTENSION,
        new PartitionKey().batch(
            TAR_REPO.replace("" + PARTITION_EPOCH_MIN / 1000, ""
                + (PARTITION_EPOCH_MAX / 1000 + 1))).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch("some-rubbish").getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(FILE).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch("some-rubbish" + TAR_REPO).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(TAR_REPO.substring(1, TAR_REPO.length())).getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(TAR_REPO)
        .record("some-rubbish").getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(TAR_REPO)
        .record("some-rubbish" + FILE).getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(TAR_REPO)
        .record(FILE.substring(1, FILE.length())).getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(TAR_REPO)
        .record("9" + FILE.substring(1, FILE.length())).getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(TAR_REPO)
        .epochPoll(1L).getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(TAR_REPO)
        .epochPoll(PARTITION_EPOCH_MIN - 1).getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(TAR_REPO)
        .epochPoll(PARTITION_EPOCH_MAX + 1).getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(TAR_REPO)
        .record(FILE).epochPoll(EPOCH_POLL + 1).getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(TAR_REPO)
        .record(FILE).epochPoll(EPOCH_POLL - 1).getType());
  }

  @Test
  public void testInvalidCodec() {
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .record(FILE).getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .epochPoll(EPOCH_POLL).getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .epochUpdate(EPOCH_UPDATE).getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(null).getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch("").getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch("some-rubbish").getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(FILE).getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch("some-rubbish" + TAR_REPO).getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(TAR_REPO.substring(1, TAR_REPO.length())).getCodec());
    Assert.assertEquals(
        TAR_CODEC,
        new PartitionKey().batch(
            TAR_REPO.replace("" + PARTITION_EPOCH_MIN / 1000, ""
                + (PARTITION_EPOCH_MAX / 1000 + 1))).getCodec());
    Assert.assertEquals(TAR_CODEC,
        new PartitionKey().batch(TAR_REPO).record("some-rubbish").getCodec());
    Assert.assertEquals(TAR_CODEC,
        new PartitionKey().batch(TAR_REPO).record("some-rubbish" + FILE)
            .getCodec());
    Assert.assertEquals(
        TAR_CODEC,
        new PartitionKey().batch(TAR_REPO)
            .record(FILE.substring(1, FILE.length())).getCodec());
    Assert.assertEquals(
        TAR_CODEC,
        new PartitionKey().batch(TAR_REPO)
            .record("9" + FILE.substring(1, FILE.length())).getCodec());
    Assert.assertEquals(TAR_CODEC, new PartitionKey().batch(TAR_REPO)
        .epochPoll(1L).getCodec());
    Assert.assertEquals(TAR_CODEC, new PartitionKey().batch(TAR_REPO)
        .epochPoll(PARTITION_EPOCH_MIN - 1).getCodec());
    Assert.assertEquals(TAR_CODEC, new PartitionKey().batch(TAR_REPO)
        .epochPoll(PARTITION_EPOCH_MAX + 1).getCodec());
    Assert.assertEquals(
        TAR_CODEC,
        new PartitionKey().batch(TAR_REPO).record(FILE)
        .epochPoll(EPOCH_POLL + 1).getCodec());
    Assert.assertEquals(
        TAR_CODEC,
        new PartitionKey().batch(TAR_REPO).record(FILE)
        .epochPoll(EPOCH_POLL - 1).getCodec());
  }

  @Test
  public void testInvalidPartition() {
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .record(FILE).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .epochPoll(EPOCH_POLL).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .epochUpdate(EPOCH_UPDATE).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(null).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch("").getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch("some-rubbish").getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(FILE).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch("some-rubbish" + TAR_REPO).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(TAR_REPO.substring(1, TAR_REPO.length())).getPartition());
    Assert.assertEquals(
        PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().batch(
            TAR_REPO.replace("" + PARTITION_EPOCH_MIN / 1000, ""
                + (PARTITION_EPOCH_MAX / 1000 + 1))).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(TAR_REPO).record("some-rubbish").getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(TAR_REPO).record("some-rubbish" + FILE).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(TAR_REPO).record(FILE.substring(1, FILE.length()))
        .getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(TAR_REPO).record("9" + FILE.substring(1, FILE.length()))
        .getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(TAR_REPO).epochPoll(1L).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(TAR_REPO).epochPoll(PARTITION_EPOCH_MIN - 1).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey()
        .batch(TAR_REPO).epochPoll(PARTITION_EPOCH_MAX + 1).getPartition());
    Assert.assertEquals(
        PARTITION.iterator().next(),
        new PartitionKey().batch(TAR_REPO).record(FILE)
        .epochPoll(EPOCH_POLL + 1).getPartition());
    Assert.assertEquals(
        PARTITION.iterator().next(),
        new PartitionKey().batch(TAR_REPO).record(FILE)
        .epochPoll(EPOCH_POLL - 1).getPartition());
  }

  @Test
  public void testInvalidPartitions() {
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().record(FILE)
        .getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN,
        new PartitionKey().epochPoll(EPOCH_POLL).getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN,
        new PartitionKey().epochUpdate(EPOCH_UPDATE).getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch(null)
        .getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch("")
        .getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN,
        new PartitionKey().batch("some-rubbish").getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch(FILE)
        .getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN,
        new PartitionKey().batch("some-rubbish" + TAR_REPO).getPartitions());
    Assert.assertEquals(
        new TreeSet<String>(Arrays.asList(new String[] { PartitionFlag._UNKNOWN
            .toString() })),
        new PartitionKey().batch(TAR_REPO.substring(1, TAR_REPO.length()))
            .getPartitions());
    Assert.assertEquals(
        PARTITIONS_UNKNOWN,
        new PartitionKey().batch(
            TAR_REPO.replace("" + PARTITION_EPOCH_MIN / 1000, ""
                + (PARTITION_EPOCH_MAX / 1000 + 1))).getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch(TAR_REPO)
        .record("some-rubbish").getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch(TAR_REPO)
        .record("some-rubbish" + FILE).getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch(TAR_REPO)
        .record(FILE.substring(1, FILE.length())).getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch(TAR_REPO)
        .record("9" + FILE.substring(1, FILE.length())).getPartitions());
    Assert.assertEquals(PARTITIONS, new PartitionKey().batch(TAR_REPO)
        .epochPoll(1L).getPartitions());
    Assert.assertEquals(PARTITIONS, new PartitionKey().batch(TAR_REPO)
        .epochPoll(PARTITION_EPOCH_MIN - 1).getPartitions());
    Assert.assertEquals(PARTITIONS, new PartitionKey().batch(TAR_REPO)
        .epochPoll(PARTITION_EPOCH_MAX + 1).getPartitions());
    Assert.assertEquals(
        PARTITION,
        new PartitionKey().batch(TAR_REPO).record(FILE)
        .epochPoll(EPOCH_POLL + 1).getPartitions());
    Assert.assertEquals(
        PARTITION,
        new PartitionKey().batch(TAR_REPO).record(FILE)
        .epochPoll(EPOCH_POLL - 1).getPartitions());
  }

  @Test
  public void testInvalidPath() {
    Assert.assertEquals(false, new PartitionKey().path(null).isValid());
    Assert.assertEquals(PATH_UNKNOWN, new PartitionKey().path(null).getPath());
    Assert.assertEquals(PATH_UNKNOWN_PARENT, new PartitionKey().path(null)
        .getPathPartition());
    Assert.assertEquals(false, new PartitionKey().path("").isValid());
    Assert.assertEquals(PATH_UNKNOWN, new PartitionKey().path("").getPath());
    Assert.assertEquals(PATH_UNKNOWN_PARENT, new PartitionKey().path("")
        .getPathPartition());
    Assert.assertEquals(false, new PartitionKey().path("some/rubbish/path")
        .isValid());
    Assert.assertEquals(PATH_UNKNOWN,
        new PartitionKey().path("some/rubbish/path").getPath());
    Assert.assertEquals(PATH_UNKNOWN_PARENT,
        new PartitionKey().path("some/rubbish/path").getPathPartition());
    Assert.assertEquals(PATH_UNKNOWN, new PartitionKey().path(PATH_UNKNOWN)
        .getPath());

    Assert.assertEquals(
        '/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
            + PARTITIONS.iterator().next() + '/' + TAR_REPO + '/' + TAR_REPO,
        new PartitionKey().path(
            "file:///some/dir/" + TAR_EXTENSION + '/'
                + PartitionFlag._UNKNOWN.toString() + '/'
                + PARTITIONS.iterator().next() + '/' + TAR_REPO + '/'
                + TAR_REPO + "-some-extra-extension").getPath());
    Assert.assertEquals(
        '/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
            + PARTITIONS.iterator().next() + '/' + TAR_REPO + '/' + TAR_REPO,
        new PartitionKey().path(
            "file:///some/dir/" + PartitionFlag._UNKNOWN.toString() + '/'
                + TAR_CODEC + '/' + PARTITIONS.iterator().next() + '/'
                + TAR_REPO + '/' + TAR_REPO + "-some-extra-extension")
            .getPath());
    Assert.assertEquals(
        '/' + TAR_EXTENSION + '/' + TAR_CODEC + "/some-rubbish/" + TAR_REPO
            + '/' + TAR_REPO,
        new PartitionKey().path(
            "file:///some/dir/" + PartitionFlag._UNKNOWN.toString() + '/'
                + TAR_CODEC + "/some-rubbish/" + TAR_REPO + '/' + TAR_REPO
                + "-some-extra-extension").getPath());
    Assert.assertEquals(
        '/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
            + PartitionFlag._UNKNOWN.toString() + '/' + TAR_REPO + '/'
            + TAR_REPO,
        new PartitionKey().path(
            "file:///some/dir/" + PartitionFlag._UNKNOWN.toString() + '/'
                + TAR_CODEC + '/' + PartitionFlag._UNKNOWN.toString() + '/'
                + TAR_REPO + '/' + TAR_REPO + "-some-extra-extension")
            .getPath());
    Assert.assertEquals(
        '/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
            + PartitionFlag._UNKNOWN.toString() + '/' + TAR_REPO + '/'
            + TAR_REPO,
        new PartitionKey().path(
            "file:///some/dir/" + PATH_UNKNOWN_PARENT + '/' + TAR_REPO + '/'
                + TAR_REPO + "-some-extra-extension").getPath());
    Assert.assertEquals(
        '/' + PartitionFlag._UNKNOWN.toString() + '/'
            + PartitionFlag._UNKNOWN.toString() + '/'
            + PartitionFlag._UNKNOWN.toString() + '/'
            + "some-rubbish.extension" + '/' + "some-rubbish.extension",
        new PartitionKey().path(
            "file:///some/dir/" + PATH_UNKNOWN_PARENT + '/'
                + "some-rubbish.extension" + '/' + "some-rubbish.extension"
                + "-some-extra-extension").getPath());
  }

  @Test
  public void testKeysTar() {
    Assert.assertEquals(PARTITIONS.size(),
        PartitionKey.getKeys(TAR_REPO, TAR_REPO).size());
    Iterator<String> partitionsIterator = PARTITIONS.iterator();
    for (PartitionKey partitionKey : PartitionKey.getKeys(TAR_REPO, TAR_REPO)) {
      String partition = partitionsIterator.next();
      Assert.assertEquals(TAR_REPO, partitionKey.getRecord());
      Assert.assertEquals(TAR_REPO, partitionKey.getBatch());
      Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
      Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
      Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
      Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
      Assert.assertEquals(partition, partitionKey.getPartition());
      Assert.assertEquals(
          new TreeSet<String>(Arrays.asList(new String[] { partition })),
          partitionKey.getPartitions());
      Assert
          .assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partition
              + '/' + TAR_REPO + '/' + TAR_REPO, partitionKey.getPath());
      Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
          + partition, partitionKey.getPathPartition());
    }
    partitionsIterator = PARTITIONS.iterator();
    for (PartitionKey partitionKey : PartitionKey.getKeys(TAR_REPO)) {
      String partition = partitionsIterator.next();
      Assert.assertEquals(TAR_REPO, partitionKey.getRecord());
      Assert.assertEquals(TAR_REPO, partitionKey.getBatch());
      Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
      Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
      Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
      Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
      Assert.assertEquals(partition, partitionKey.getPartition());
      Assert.assertEquals(
          new TreeSet<String>(Arrays.asList(new String[] { partition })),
          partitionKey.getPartitions());
      Assert
          .assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partition
              + '/' + TAR_REPO + '/' + TAR_REPO, partitionKey.getPath());
      Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
          + partition, partitionKey.getPathPartition());
    }
  }

  @Test
  public void testKeysXML() {
    Assert.assertEquals(PARTITIONS.size(), PartitionKey.getKeys(XML_REPO, FILE)
        .size());
    Iterator<String> partitionsIterator = PARTITIONS.iterator();
    for (PartitionKey partitionKey : PartitionKey.getKeys(XML_REPO, FILE)) {
      String partition = partitionsIterator.next();
      if (FILE.equals(partitionKey.getRecord())) {
        Assert.assertEquals(FILE, partitionKey.getRecord());
        Assert.assertEquals(XML_REPO, partitionKey.getBatch());
        Assert.assertEquals(EPOCH_POLL, partitionKey.getEpochGet());
        Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
        Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
        Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
        Assert.assertEquals(partition, partitionKey.getPartition());
        Assert.assertEquals(
            new TreeSet<String>(Arrays.asList(new String[] { partition })),
            partitionKey.getPartitions());
        Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
            + partition + '/' + XML_REPO + '/' + FILE, partitionKey.getPath());
        Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
            + partition, partitionKey.getPathPartition());
      } else {
        Assert.assertEquals(XML_REPO, partitionKey.getRecord());
        Assert.assertEquals(XML_REPO, partitionKey.getBatch());
        Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
        Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
        Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
        Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
        Assert.assertEquals(partition, partitionKey.getPartition());
        Assert.assertEquals(
            new TreeSet<String>(Arrays.asList(new String[] { partition })),
            partitionKey.getPartitions());
        Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
            + partition + '/' + XML_REPO + '/' + XML_REPO,
            partitionKey.getPath());
        Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
            + partition, partitionKey.getPathPartition());
      }
    }
    partitionsIterator = PARTITIONS.iterator();
    for (PartitionKey partitionKey : PartitionKey.getKeys(XML_REPO)) {
      String partition = partitionsIterator.next();
      Assert.assertEquals(XML_REPO, partitionKey.getRecord());
      Assert.assertEquals(XML_REPO, partitionKey.getBatch());
      Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
      Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
      Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
      Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
      Assert.assertEquals(partition, partitionKey.getPartition());
      Assert.assertEquals(
          new TreeSet<String>(Arrays.asList(new String[] { partition })),
          partitionKey.getPartitions());
      Assert
          .assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition
              + '/' + XML_REPO + '/' + XML_REPO, partitionKey.getPath());
      Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
          + partition, partitionKey.getPathPartition());
    }
  }

  @Test
  public void testBatchTar() {
    PartitionKey partitionKey = new PartitionKey().batch(TAR_REPO).record(
        TAR_REPO);
    Assert.assertEquals(TAR_REPO, partitionKey.getRecord());
    Assert.assertEquals(TAR_REPO, partitionKey.getBatch());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
    Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
    Assert.assertEquals(PARTITIONS, partitionKey.getPartitions());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
        + PARTITIONS_UNKNOWN.iterator().next() + '/' + TAR_REPO + '/'
        + TAR_REPO, partitionKey.getPath());
    Assert
        .assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
            + PARTITIONS_UNKNOWN.iterator().next(),
            partitionKey.getPathPartition());
  }

  @Test
  public void testBatchXML() {
    PartitionKey partitionKey = new PartitionKey().batch(XML_REPO);
    Assert.assertEquals(XML_REPO, partitionKey.getRecord());
    Assert.assertEquals(XML_REPO, partitionKey.getBatch());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
    Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
    Assert.assertEquals(PARTITIONS, partitionKey.getPartitions());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
        + PARTITIONS_UNKNOWN.iterator().next() + '/' + XML_REPO + '/'
        + XML_REPO, partitionKey.getPath());
    Assert
        .assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
            + PARTITIONS_UNKNOWN.iterator().next(),
            partitionKey.getPathPartition());
  }

  @Test
  public void testRecordBatchTar() {
    PartitionKey partitionKey = new PartitionKey().batch(TAR_REPO).record(FILE);
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertEquals(FILE, partitionKey.getRecord());
    Assert.assertEquals(TAR_REPO, partitionKey.getBatch());
    Assert.assertEquals(EPOCH_POLL, partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
    Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
    Assert.assertEquals(PARTITION.iterator().next(),
        partitionKey.getPartition());
    Assert.assertEquals(PARTITION, partitionKey.getPartitions());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
        + PARTITION.iterator().next() + '/' + TAR_REPO + '/' + FILE,
        partitionKey.getPath());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
        + PARTITION.iterator().next(), partitionKey.getPathPartition());
  }

  @Test
  public void testRecordBatchXML() {
    PartitionKey partitionKey = new PartitionKey().batch(XML_REPO).record(FILE);
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertEquals(FILE, partitionKey.getRecord());
    Assert.assertEquals(XML_REPO, partitionKey.getBatch());
    Assert.assertEquals(EPOCH_POLL, partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
    Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
    Assert.assertEquals(PARTITION.iterator().next(),
        partitionKey.getPartition());
    Assert.assertEquals(PARTITION, partitionKey.getPartitions());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
        + PARTITION.iterator().next() + '/' + XML_REPO + '/' + FILE,
        partitionKey.getPath());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
        + PARTITION.iterator().next(), partitionKey.getPathPartition());
  }

  @Test
  public void testRecordBatchUpdateTar() {
    PartitionKey partitionKey = new PartitionKey().batch(TAR_REPO).record(FILE)
        .epochUpdate(EPOCH_UPDATE);
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertEquals(FILE, partitionKey.getRecord());
    Assert.assertEquals(TAR_REPO, partitionKey.getBatch());
    Assert.assertEquals(EPOCH_POLL, partitionKey.getEpochGet());
    Assert.assertEquals(EPOCH_UPDATE, partitionKey.getEpochUpdate());
    Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
    Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
    Assert.assertEquals(PARTITION.iterator().next(),
        partitionKey.getPartition());
    Assert.assertEquals(PARTITION, partitionKey.getPartitions());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
        + PARTITION.iterator().next() + '/' + TAR_REPO + '/' + FILE,
        partitionKey.getPath());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
        + PARTITION.iterator().next(), partitionKey.getPathPartition());
  }

  @Test
  public void testRecordBatchUpdateXML() {
    PartitionKey partitionKey = new PartitionKey().batch(XML_REPO).record(FILE)
        .epochUpdate(EPOCH_UPDATE);
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertEquals(FILE, partitionKey.getRecord());
    Assert.assertEquals(XML_REPO, partitionKey.getBatch());
    Assert.assertEquals(EPOCH_POLL, partitionKey.getEpochGet());
    Assert.assertEquals(EPOCH_UPDATE, partitionKey.getEpochUpdate());
    Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
    Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
    Assert.assertEquals(PARTITION.iterator().next(),
        partitionKey.getPartition());
    Assert.assertEquals(PARTITION, partitionKey.getPartitions());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
        + PARTITION.iterator().next() + '/' + XML_REPO + '/' + FILE,
        partitionKey.getPath());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
        + PARTITION.iterator().next(), partitionKey.getPathPartition());
  }

  @Test
  public void testPathTar() {
    PartitionKey partitionKey = new PartitionKey().path("file:///some/dir/"
        + TAR_EXTENSION + '/' + TAR_CODEC + '/' + PARTITION.iterator().next()
        + '/' + TAR_REPO + '/' + TAR_REPO + "-some-extension");
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertEquals(TAR_REPO, partitionKey.getRecord());
    Assert.assertEquals(TAR_REPO, partitionKey.getBatch());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
    Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
    Assert.assertEquals(PARTITION.iterator().next(),
        partitionKey.getPartition());
    Assert.assertEquals(PARTITION, partitionKey.getPartitions());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
        + PARTITION.iterator().next() + '/' + TAR_REPO + '/' + TAR_REPO,
        partitionKey.getPath());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/'
        + PARTITION.iterator().next(), partitionKey.getPathPartition());
  }

  @Test
  public void testPathXML() {
    PartitionKey partitionKey = new PartitionKey().path("file:///some/dir/"
        + XML_EXTENSION + '/' + XML_CODEC + '/' + PARTITION.iterator().next()
        + '/' + XML_REPO + '/' + FILE + "-some-extension");
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertEquals(FILE, partitionKey.getRecord());
    Assert.assertEquals(XML_REPO, partitionKey.getBatch());
    Assert.assertEquals(EPOCH_POLL, partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
    Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
    Assert.assertEquals(PARTITION.iterator().next(),
        partitionKey.getPartition());
    Assert.assertEquals(PARTITION, partitionKey.getPartitions());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
        + PARTITION.iterator().next() + '/' + XML_REPO + '/' + FILE,
        partitionKey.getPath());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/'
        + PARTITION.iterator().next(), partitionKey.getPathPartition());
  }

  @Test
  public void testTypeCodecTar() {
    PartitionKey partitionKey = new PartitionKey().batch(TAR_REPO)
        .record(TAR_REPO).type(SEQ_EXTENSION).codec(SEQ_CODEC);
    Assert.assertEquals(TAR_REPO, partitionKey.getRecord());
    Assert.assertEquals(TAR_REPO, partitionKey.getBatch());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(SEQ_EXTENSION, partitionKey.getType());
    Assert.assertEquals(SEQ_CODEC, partitionKey.getCodec());
    Assert.assertEquals(PARTITIONS, partitionKey.getPartitions());
    Assert.assertEquals('/' + SEQ_EXTENSION + '/' + SEQ_CODEC + '/'
        + PARTITIONS_UNKNOWN.iterator().next() + '/' + TAR_REPO + '/'
        + TAR_REPO, partitionKey.getPath());
    Assert
        .assertEquals('/' + SEQ_EXTENSION + '/' + SEQ_CODEC + '/'
            + PARTITIONS_UNKNOWN.iterator().next(),
            partitionKey.getPathPartition());
  }

  @Test
  public void testTypeCodecXML() {
    PartitionKey partitionKey = new PartitionKey().batch(XML_REPO)
        .type(SEQ_EXTENSION).codec(SEQ_CODEC);
    Assert.assertEquals(XML_REPO, partitionKey.getRecord());
    Assert.assertEquals(XML_REPO, partitionKey.getBatch());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(SEQ_EXTENSION, partitionKey.getType());
    Assert.assertEquals(SEQ_CODEC, partitionKey.getCodec());
    Assert.assertEquals(PARTITIONS, partitionKey.getPartitions());
    Assert.assertEquals('/' + SEQ_EXTENSION + '/' + SEQ_CODEC + '/'
        + PARTITIONS_UNKNOWN.iterator().next() + '/' + XML_REPO + '/'
        + XML_REPO, partitionKey.getPath());
    Assert
        .assertEquals('/' + SEQ_EXTENSION + '/' + SEQ_CODEC + '/'
            + PARTITIONS_UNKNOWN.iterator().next(),
            partitionKey.getPathPartition());
  }

}
