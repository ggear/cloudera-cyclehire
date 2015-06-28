package com.cloudera.cyclehire.main.common.model;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Iterator;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.cyclehire.main.common.MrUtil;
import com.cloudera.cyclehire.main.test.TestConstants;

@RunWith(value = Parameterized.class)
public class PartitionKeyTest implements TestConstants {

  @Parameters()
  public static Iterable<Object[]> paramaters() {
    return Arrays.asList(new Object[][] {
        {
            //
            FILES_DS.get(DIR_DS_TAR_NOID).get(DIR_DSS_SINGLERECORDS).get(0).getName(),
            FILES_DS.get(DIR_DS_XML_NOID).get(DIR_DSS_SINGLERECORDS).get(0).getParentFile().getName(),
            FILES_DS.get(DIR_DS_XML_NOID).get(DIR_DSS_SINGLERECORDS).get(0).getName() //
        },
        {
            //
            FILES_DS.get(DIR_DS_TAR_NOID).get(DIR_DSS_SINGLERECORDS).get(0).getName(),
            FILES_DS.get(DIR_DS_XML_UUID).get(DIR_DSS_SINGLERECORDS).get(0).getParentFile().getName(),
            FILES_DS.get(DIR_DS_XML_UUID).get(DIR_DSS_SINGLERECORDS).get(0).getName() //
        },
        {
            //
            FILES_DS.get(DIR_DS_TAR_NOID).get(DIR_DSS_MULTIRECORDS).get(0).getName(),
            FILES_DS.get(DIR_DS_XML_NOID).get(DIR_DSS_MULTIRECORDS).get(0).getParentFile().getName(),
            FILES_DS.get(DIR_DS_XML_NOID).get(DIR_DSS_MULTIRECORDS).get(0).getName() //
        },
        {
            //
            FILES_DS.get(DIR_DS_TAR_NOID).get(DIR_DSS_MULTIRECORDS).get(0).getName(),
            FILES_DS.get(DIR_DS_XML_UUID).get(DIR_DSS_MULTIRECORDS).get(0).getParentFile().getName(),
            FILES_DS.get(DIR_DS_XML_UUID).get(DIR_DSS_MULTIRECORDS).get(0).getName() //
        },
        { //
        FILES_DS.get(DIR_DS_TAR_NOID).get(DIR_DSS_MULTIPARTITIONS).get(0).getName(),
            FILES_DS.get(DIR_DS_XML_NOID).get(DIR_DSS_MULTIPARTITIONS).get(0).getParentFile().getName(),
            FILES_DS.get(DIR_DS_XML_NOID).get(DIR_DSS_MULTIPARTITIONS).get(0).getName() //
        },
        {
            //
            FILES_DS.get(DIR_DS_TAR_NOID).get(DIR_DSS_MULTIPARTITIONS).get(0).getName(),
            FILES_DS.get(DIR_DS_XML_UUID).get(DIR_DSS_MULTIPARTITIONS).get(0).getParentFile().getName(),
            FILES_DS.get(DIR_DS_XML_UUID).get(DIR_DSS_MULTIPARTITIONS).get(0).getName() //
        } //
        });
  }

  private static final String SEQ_CODEC = "snappy";
  private static final String SEQ_EXTENSION = "sequence";

  private static final String TAR_CODEC = "gz";
  private static final String TAR_EXTENSION = "tar";

  private static final String XML_CODEC = MrUtil.CODEC_NONE;
  private static final String XML_EXTENSION = "xml";

  private Set<String> PARTITIONS_UNKNOWN = new TreeSet<String>(Arrays.asList(new String[] { PartitionFlag._UNKNOWN
      .toString() }));
  private String PATH_UNKNOWN_PARENT = '/' + PartitionFlag._UNKNOWN.toString() + '/'
      + PartitionFlag._UNKNOWN.toString() + '/' + PartitionFlag._UNKNOWN.toString();
  private String PATH_UNKNOWN = PATH_UNKNOWN_PARENT + '/' + PartitionFlag._UNKNOWN.toString() + '/'
      + PartitionFlag._UNKNOWN.toString();

  private String tarFileName;
  private String xmlDirName;
  private String xmlFileName;

  private Long epochPoll;
  private Long epochUpdate;
  private long partitionEpochMin;
  private long paritionEpochMax;
  private Set<String> partition;
  private Set<String> partitions;

  public PartitionKeyTest(String tarFileName, String xmlDirName, String xmlFileName)
      throws UnsupportedEncodingException, IOException {
    this.tarFileName = tarFileName;
    this.xmlDirName = xmlDirName;
    this.xmlFileName = xmlFileName;
    partition = new TreeSet<String>();
    partitions = new TreeSet<String>();
    Matcher matcherFile = PartitionKey.REGEX_RECORD.matcher(xmlFileName);
    matcherFile.matches();
    epochPoll = 1000 * Long.parseLong(matcherFile.group(1));
    epochUpdate = epochPoll - 50000;
    Calendar calendar = PartitionKey.getCalendarMonth(epochPoll);
    partition.add("year=" + calendar.get(Calendar.YEAR) + "/month="
        + String.format("%d", calendar.get(Calendar.MONTH) + 1));
    Matcher matcherBatch = PartitionKey.REGEX_BATCH.matcher(xmlDirName);
    matcherBatch.matches();
    partitionEpochMin = 1000 * Long.parseLong(matcherBatch.group(1));
    paritionEpochMax = 1000 * Long.parseLong(matcherBatch.group(2));
    Calendar calendarMin = PartitionKey.getCalendarMonth(partitionEpochMin);
    Calendar calendarMax = PartitionKey.getCalendarMonth(paritionEpochMax);
    calendarMax.set(Calendar.DAY_OF_MONTH, calendarMax.getActualMaximum(Calendar.DAY_OF_MONTH));
    calendarMax.set(Calendar.HOUR, 23);
    calendarMax.set(Calendar.MINUTE, 59);
    calendarMax.set(Calendar.SECOND, 59);
    calendarMax.set(Calendar.MILLISECOND, 999);
    while (calendarMin.before(calendarMax)) {
      partitions.add("year=" + calendarMin.get(Calendar.YEAR) + "/month="
          + String.format("%d", calendarMin.get(Calendar.MONTH) + 1));
      calendarMin.add(Calendar.MONTH, 1);
    }
  }

  @Test
  public void testInvalid() {
    Assert.assertEquals(false, new PartitionKey().isValid());
    Assert.assertEquals(false, new PartitionKey().record(xmlFileName).isValid());
    Assert.assertEquals(false, new PartitionKey().epochPoll(epochPoll).isValid());
    Assert.assertEquals(false, new PartitionKey().epochUpdate(epochUpdate).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(null).isValid());
    Assert.assertEquals(false, new PartitionKey().batch("").isValid());
    Assert.assertEquals(false, new PartitionKey().batch("some-rubbish").isValid());
    Assert.assertEquals(false, new PartitionKey().batch(xmlFileName).isValid());
    Assert.assertEquals(false, new PartitionKey().batch("some-rubbish" + tarFileName).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(tarFileName.substring(1, tarFileName.length())).isValid());
    Assert.assertEquals(
        false,
        new PartitionKey().batch(
            tarFileName.replaceFirst("" + partitionEpochMin / 1000, "" + (paritionEpochMax / 1000 + 1))).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(tarFileName).record("some-rubbish").isValid());
    Assert.assertEquals(false, new PartitionKey().batch(tarFileName).record("some-rubbish" + xmlFileName).isValid());
    Assert.assertEquals(false,
        new PartitionKey().batch(tarFileName).record(xmlFileName.substring(1, xmlFileName.length())).isValid());
    Assert.assertEquals(false,
        new PartitionKey().batch(tarFileName).record("9" + xmlFileName.substring(1, xmlFileName.length())).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(tarFileName).epochPoll(1L).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(tarFileName).epochPoll(partitionEpochMin - 1).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(tarFileName).epochPoll(paritionEpochMax + 1).isValid());
    Assert.assertEquals(false, new PartitionKey().batch(tarFileName).record(xmlFileName).epochPoll(epochPoll * 2)
        .isValid());
    Assert.assertEquals(false, new PartitionKey().batch(tarFileName).record(xmlFileName).epochPoll(epochPoll * -1L)
        .isValid());
    Assert.assertEquals(false, new PartitionKey().batch(tarFileName).partition("some-rubbish").isValid());
    Assert.assertEquals(false,
        new PartitionKey().batch(tarFileName).partition(partition.iterator().next().replace("20", "80")).isValid());
  }

  @Test
  public void testInvalidType() {
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().record(xmlFileName).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().epochPoll(epochPoll).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().epochUpdate(epochUpdate).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch(null).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch("").getType());
    Assert.assertEquals(TAR_EXTENSION,
        new PartitionKey()
            .batch(tarFileName.replace("" + partitionEpochMin / 1000, "" + (paritionEpochMax / 1000 + 1))).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch("some-rubbish").getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch(xmlFileName).getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch("some-rubbish" + tarFileName)
        .getType());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().batch(tarFileName.substring(1, tarFileName.length())).getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(tarFileName).record("some-rubbish").getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(tarFileName).record("some-rubbish" + xmlFileName)
        .getType());
    Assert.assertEquals(TAR_EXTENSION,
        new PartitionKey().batch(tarFileName).record(xmlFileName.substring(1, xmlFileName.length())).getType());
    Assert.assertEquals(TAR_EXTENSION,
        new PartitionKey().batch(tarFileName).record("9" + xmlFileName.substring(1, xmlFileName.length())).getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(tarFileName).epochPoll(1L).getType());
    Assert
        .assertEquals(TAR_EXTENSION, new PartitionKey().batch(tarFileName).epochPoll(partitionEpochMin - 1).getType());
    Assert.assertEquals(TAR_EXTENSION, new PartitionKey().batch(tarFileName).epochPoll(paritionEpochMax + 1).getType());
    Assert.assertEquals(TAR_EXTENSION,
        new PartitionKey().batch(tarFileName).record(xmlFileName).epochPoll(epochPoll + 1).getType());
    Assert.assertEquals(TAR_EXTENSION,
        new PartitionKey().batch(tarFileName).record(xmlFileName).epochPoll(epochPoll - 1).getType());
  }

  @Test
  public void testInvalidCodec() {
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().record(xmlFileName).getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().epochPoll(epochPoll).getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().epochUpdate(epochUpdate).getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch(null).getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch("").getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch("some-rubbish").getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch(xmlFileName).getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch("some-rubbish" + tarFileName)
        .getCodec());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().batch(tarFileName.substring(1, tarFileName.length())).getCodec());
    Assert.assertEquals(TAR_CODEC,
        new PartitionKey()
            .batch(tarFileName.replace("" + partitionEpochMin / 1000, "" + (paritionEpochMax / 1000 + 1))).getCodec());
    Assert.assertEquals(TAR_CODEC, new PartitionKey().batch(tarFileName).record("some-rubbish").getCodec());
    Assert.assertEquals(TAR_CODEC, new PartitionKey().batch(tarFileName).record("some-rubbish" + xmlFileName)
        .getCodec());
    Assert.assertEquals(TAR_CODEC,
        new PartitionKey().batch(tarFileName).record(xmlFileName.substring(1, xmlFileName.length())).getCodec());
    Assert.assertEquals(TAR_CODEC,
        new PartitionKey().batch(tarFileName).record("9" + xmlFileName.substring(1, xmlFileName.length())).getCodec());
    Assert.assertEquals(TAR_CODEC, new PartitionKey().batch(tarFileName).epochPoll(1L).getCodec());
    Assert.assertEquals(TAR_CODEC, new PartitionKey().batch(tarFileName).epochPoll(partitionEpochMin - 1).getCodec());
    Assert.assertEquals(TAR_CODEC, new PartitionKey().batch(tarFileName).epochPoll(paritionEpochMax + 1).getCodec());
    Assert.assertEquals(TAR_CODEC, new PartitionKey().batch(tarFileName).record(xmlFileName).epochPoll(epochPoll + 1)
        .getCodec());
    Assert.assertEquals(TAR_CODEC, new PartitionKey().batch(tarFileName).record(xmlFileName).epochPoll(epochPoll - 1)
        .getCodec());
  }

  @Test
  public void testInvalidPartition() {
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().record(xmlFileName).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().epochPoll(epochPoll).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().epochUpdate(epochUpdate).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch(null).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch("").getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch("some-rubbish").getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch(xmlFileName).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch("some-rubbish" + tarFileName)
        .getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().batch(tarFileName.substring(1, tarFileName.length())).getPartition());
    Assert
        .assertEquals(
            PartitionFlag._UNKNOWN.toString(),
            new PartitionKey().batch(
                tarFileName.replaceFirst("" + partitionEpochMin / 1000, "" + (paritionEpochMax / 1000 + 1)))
                .getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(), new PartitionKey().batch(tarFileName).record("some-rubbish")
        .getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().batch(tarFileName).record("some-rubbish" + xmlFileName).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().batch(tarFileName).record(xmlFileName.substring(1, xmlFileName.length())).getPartition());
    Assert.assertEquals(PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().batch(tarFileName).record("9" + xmlFileName.substring(1, xmlFileName.length()))
            .getPartition());
    Assert.assertEquals(partitions.size() == 1 ? partitions.iterator().next() : PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().batch(tarFileName).epochPoll(1L).getPartition());
    Assert.assertEquals(partitions.size() == 1 ? partitions.iterator().next() : PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().batch(tarFileName).epochPoll(partitionEpochMin - 1).getPartition());
    Assert.assertEquals(partitions.size() == 1 ? partitions.iterator().next() : PartitionFlag._UNKNOWN.toString(),
        new PartitionKey().batch(tarFileName).epochPoll(paritionEpochMax + 1).getPartition());
    Assert.assertEquals(partition.iterator().next(), new PartitionKey().batch(tarFileName).record(xmlFileName)
        .epochPoll(epochPoll + 1).getPartition());
    Assert.assertEquals(partition.iterator().next(), new PartitionKey().batch(tarFileName).record(xmlFileName)
        .epochPoll(epochPoll - 1).getPartition());
  }

  @Test
  public void testInvalidPartitions() {
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().record(xmlFileName).getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().epochPoll(epochPoll).getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().epochUpdate(epochUpdate).getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch(null).getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch("").getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch("some-rubbish").getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch(xmlFileName).getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch("some-rubbish" + tarFileName).getPartitions());
    Assert.assertEquals(new TreeSet<String>(Arrays.asList(new String[] { PartitionFlag._UNKNOWN.toString() })),
        new PartitionKey().batch(tarFileName.substring(1, tarFileName.length())).getPartitions());
    Assert.assertEquals(
        PARTITIONS_UNKNOWN,
        new PartitionKey().batch(
            tarFileName.replaceFirst("" + partitionEpochMin / 1000, "" + (paritionEpochMax / 1000 + 1)))
            .getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch(tarFileName).record("some-rubbish")
        .getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN, new PartitionKey().batch(tarFileName).record("some-rubbish" + xmlFileName)
        .getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN,
        new PartitionKey().batch(tarFileName).record(xmlFileName.substring(1, xmlFileName.length())).getPartitions());
    Assert.assertEquals(PARTITIONS_UNKNOWN,
        new PartitionKey().batch(tarFileName).record("9" + xmlFileName.substring(1, xmlFileName.length()))
            .getPartitions());
    Assert.assertEquals(partitions, new PartitionKey().batch(tarFileName).epochPoll(1L).getPartitions());
    Assert.assertEquals(partitions, new PartitionKey().batch(tarFileName).epochPoll(partitionEpochMin - 1)
        .getPartitions());
    Assert.assertEquals(partitions, new PartitionKey().batch(tarFileName).epochPoll(paritionEpochMax + 1)
        .getPartitions());
    Assert.assertEquals(partition, new PartitionKey().batch(tarFileName).record(xmlFileName).epochPoll(epochPoll + 1)
        .getPartitions());
    Assert.assertEquals(partition, new PartitionKey().batch(tarFileName).record(xmlFileName).epochPoll(epochPoll - 1)
        .getPartitions());
  }

  @Test
  public void testInvalidPath() {
    Assert.assertEquals(false, new PartitionKey().path(null).isValid());
    Assert.assertEquals(PATH_UNKNOWN, new PartitionKey().path(null).getPath());
    Assert.assertEquals(PATH_UNKNOWN_PARENT, new PartitionKey().path(null).getPathPartition());
    Assert.assertEquals(false, new PartitionKey().path("").isValid());
    Assert.assertEquals(PATH_UNKNOWN, new PartitionKey().path("").getPath());
    Assert.assertEquals(PATH_UNKNOWN_PARENT, new PartitionKey().path("").getPathPartition());
    Assert.assertEquals(false, new PartitionKey().path("some/rubbish/path").isValid());
    Assert.assertEquals(PATH_UNKNOWN, new PartitionKey().path("some/rubbish/path").getPath());
    Assert.assertEquals(PATH_UNKNOWN_PARENT, new PartitionKey().path("some/rubbish/path").getPathPartition());
    Assert.assertEquals(PATH_UNKNOWN, new PartitionKey().path(PATH_UNKNOWN).getPath());

    Assert.assertEquals(
        '/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partitions.iterator().next() + '/' + tarFileName + '/'
            + tarFileName,
        new PartitionKey().path(
            "file:///some/dir/" + TAR_EXTENSION + '/' + PartitionFlag._UNKNOWN.toString() + '/'
                + partitions.iterator().next() + '/' + tarFileName + '/' + tarFileName + "-some-extra-extension")
            .getPath());
    Assert.assertEquals(
        '/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partitions.iterator().next() + '/' + tarFileName + '/'
            + tarFileName,
        new PartitionKey().path(
            "file:///some/dir/" + PartitionFlag._UNKNOWN.toString() + '/' + TAR_CODEC + '/'
                + partitions.iterator().next() + '/' + tarFileName + '/' + tarFileName + "-some-extra-extension")
            .getPath());
    Assert.assertEquals(
        '/' + TAR_EXTENSION + '/' + TAR_CODEC + "/some-rubbish/" + tarFileName + '/' + tarFileName,
        new PartitionKey().path(
            "file:///some/dir/" + PartitionFlag._UNKNOWN.toString() + '/' + TAR_CODEC + "/some-rubbish/" + tarFileName
                + '/' + tarFileName + "-some-extra-extension").getPath());
    Assert.assertEquals(
        '/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + PartitionFlag._UNKNOWN.toString() + '/' + tarFileName + '/'
            + tarFileName,
        new PartitionKey().path(
            "file:///some/dir/" + PartitionFlag._UNKNOWN.toString() + '/' + TAR_CODEC + '/'
                + PartitionFlag._UNKNOWN.toString() + '/' + tarFileName + '/' + tarFileName + "-some-extra-extension")
            .getPath());
    Assert.assertEquals(
        '/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + PartitionFlag._UNKNOWN.toString() + '/' + tarFileName + '/'
            + tarFileName,
        new PartitionKey()
            .path(
                "file:///some/dir/" + PATH_UNKNOWN_PARENT + '/' + tarFileName + '/' + tarFileName
                    + "-some-extra-extension").getPath());
    Assert.assertEquals(
        '/' + PartitionFlag._UNKNOWN.toString() + '/' + PartitionFlag._UNKNOWN.toString() + '/'
            + PartitionFlag._UNKNOWN.toString() + '/' + "some-rubbish.extension" + '/' + "some-rubbish.extension",
        new PartitionKey().path(
            "file:///some/dir/" + PATH_UNKNOWN_PARENT + '/' + "some-rubbish.extension" + '/' + "some-rubbish.extension"
                + "-some-extra-extension").getPath());
  }

  @Test
  public void testKeysTar() {
    Assert.assertEquals(partitions.size(), PartitionKey.getKeys(tarFileName, tarFileName).size());
    Iterator<String> partitionsIterator = partitions.iterator();
    for (PartitionKey partitionKey : PartitionKey.getKeys(tarFileName, tarFileName)) {
      String partition = partitionsIterator.next();
      Assert.assertEquals(tarFileName, partitionKey.getRecord());
      Assert.assertEquals(tarFileName, partitionKey.getBatch());
      Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
      Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
      Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
      Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
      Assert.assertEquals(partition, partitionKey.getPartition());
      Assert.assertEquals(new TreeSet<String>(Arrays.asList(new String[] { partition })), partitionKey.getPartitions());
      Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partition + '/' + tarFileName + '/'
          + tarFileName, partitionKey.getPath());
      Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partition, partitionKey.getPathPartition());
    }
    partitionsIterator = partitions.iterator();
    for (PartitionKey partitionKey : PartitionKey.getKeys(tarFileName)) {
      String partition = partitionsIterator.next();
      Assert.assertEquals(tarFileName, partitionKey.getRecord());
      Assert.assertEquals(tarFileName, partitionKey.getBatch());
      Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
      Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
      Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
      Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
      Assert.assertEquals(partition, partitionKey.getPartition());
      Assert.assertEquals(new TreeSet<String>(Arrays.asList(new String[] { partition })), partitionKey.getPartitions());
      Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partition + '/' + tarFileName + '/'
          + tarFileName, partitionKey.getPath());
      Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partition, partitionKey.getPathPartition());
    }
  }

  @Test
  public void testKeysXML() {
    Assert.assertEquals(partitions.size(), PartitionKey.getKeys(xmlDirName, xmlFileName).size());
    Iterator<String> partitionsIterator = partitions.iterator();
    for (PartitionKey partitionKey : PartitionKey.getKeys(xmlDirName, xmlFileName)) {
      String partition = partitionsIterator.next();
      if (xmlFileName.equals(partitionKey.getRecord())) {
        Assert.assertEquals(xmlFileName, partitionKey.getRecord());
        Assert.assertEquals(xmlDirName, partitionKey.getBatch());
        Assert.assertEquals(epochPoll, partitionKey.getEpochGet());
        Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
        Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
        Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
        Assert.assertEquals(partition, partitionKey.getPartition());
        Assert.assertEquals(new TreeSet<String>(Arrays.asList(new String[] { partition })),
            partitionKey.getPartitions());
        Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition + '/' + xmlDirName + '/'
            + xmlFileName, partitionKey.getPath());
        Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition, partitionKey.getPathPartition());
      } else {
        Assert.assertEquals(xmlDirName, partitionKey.getRecord());
        Assert.assertEquals(xmlDirName, partitionKey.getBatch());
        Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
        Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
        Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
        Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
        Assert.assertEquals(partition, partitionKey.getPartition());
        Assert.assertEquals(new TreeSet<String>(Arrays.asList(new String[] { partition })),
            partitionKey.getPartitions());
        Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition + '/' + xmlDirName + '/'
            + xmlDirName, partitionKey.getPath());
        Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition, partitionKey.getPathPartition());
      }
    }
    partitionsIterator = partitions.iterator();
    for (PartitionKey partitionKey : PartitionKey.getKeys(xmlDirName)) {
      String partition = partitionsIterator.next();
      Assert.assertEquals(xmlDirName, partitionKey.getRecord());
      Assert.assertEquals(xmlDirName, partitionKey.getBatch());
      Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
      Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
      Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
      Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
      Assert.assertEquals(partition, partitionKey.getPartition());
      Assert.assertEquals(new TreeSet<String>(Arrays.asList(new String[] { partition })), partitionKey.getPartitions());
      Assert.assertEquals(
          '/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition + '/' + xmlDirName + '/' + xmlDirName,
          partitionKey.getPath());
      Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition, partitionKey.getPathPartition());
    }
  }

  @Test
  public void testBatchTar() {
    PartitionKey partitionKey = new PartitionKey().batch(tarFileName).record(tarFileName);
    Assert.assertEquals(tarFileName, partitionKey.getRecord());
    Assert.assertEquals(tarFileName, partitionKey.getBatch());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
    Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
    Assert.assertEquals(partitions, partitionKey.getPartitions());
    Assert.assertEquals('/'
        + TAR_EXTENSION
        + '/'
        + TAR_CODEC
        + '/'
        + (partitionKey.getPartitions().size() == 1 ? partitionKey.getPartitions().iterator().next()
            : PARTITIONS_UNKNOWN.iterator().next()) + '/' + tarFileName + '/' + tarFileName, partitionKey.getPath());
    Assert.assertEquals('/'
        + TAR_EXTENSION
        + '/'
        + TAR_CODEC
        + '/'
        + (partitionKey.getPartitions().size() == 1 ? partitionKey.getPartitions().iterator().next()
            : PARTITIONS_UNKNOWN.iterator().next()), partitionKey.getPathPartition());
  }

  @Test
  public void testBatchXML() {
    PartitionKey partitionKey = new PartitionKey().batch(xmlDirName);
    Assert.assertEquals(xmlDirName, partitionKey.getRecord());
    Assert.assertEquals(xmlDirName, partitionKey.getBatch());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
    Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
    Assert.assertEquals(partitions, partitionKey.getPartitions());
    Assert.assertEquals('/'
        + XML_EXTENSION
        + '/'
        + XML_CODEC
        + '/'
        + (partitionKey.getPartitions().size() == 1 ? partitionKey.getPartitions().iterator().next()
            : PARTITIONS_UNKNOWN.iterator().next()) + '/' + xmlDirName + '/' + xmlDirName, partitionKey.getPath());
    Assert.assertEquals('/'
        + XML_EXTENSION
        + '/'
        + XML_CODEC
        + '/'
        + (partitionKey.getPartitions().size() == 1 ? partitionKey.getPartitions().iterator().next()
            : PARTITIONS_UNKNOWN.iterator().next()), partitionKey.getPathPartition());
  }

  @Test
  public void testRecordBatchTar() {
    PartitionKey partitionKey = new PartitionKey().batch(tarFileName).record(xmlFileName);
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertEquals(xmlFileName, partitionKey.getRecord());
    Assert.assertEquals(tarFileName, partitionKey.getBatch());
    Assert.assertEquals(epochPoll, partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
    Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
    Assert.assertEquals(partition.iterator().next(), partitionKey.getPartition());
    Assert.assertEquals(partition, partitionKey.getPartitions());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partition.iterator().next() + '/' + tarFileName
        + '/' + xmlFileName, partitionKey.getPath());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partition.iterator().next(),
        partitionKey.getPathPartition());
  }

  @Test
  public void testRecordBatchXML() {
    PartitionKey partitionKey = new PartitionKey().batch(xmlDirName).record(xmlFileName);
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertEquals(xmlFileName, partitionKey.getRecord());
    Assert.assertEquals(xmlDirName, partitionKey.getBatch());
    Assert.assertEquals(epochPoll, partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
    Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
    Assert.assertEquals(partition.iterator().next(), partitionKey.getPartition());
    Assert.assertEquals(partition, partitionKey.getPartitions());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition.iterator().next() + '/' + xmlDirName
        + '/' + xmlFileName, partitionKey.getPath());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition.iterator().next(),
        partitionKey.getPathPartition());
  }

  @Test
  public void testRecordBatchUpdateTar() {
    PartitionKey partitionKey = new PartitionKey().batch(tarFileName).record(xmlFileName).epochUpdate(epochUpdate);
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertEquals(xmlFileName, partitionKey.getRecord());
    Assert.assertEquals(tarFileName, partitionKey.getBatch());
    Assert.assertEquals(epochPoll, partitionKey.getEpochGet());
    Assert.assertEquals(epochUpdate, partitionKey.getEpochUpdate());
    Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
    Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
    Assert.assertEquals(partition.iterator().next(), partitionKey.getPartition());
    Assert.assertEquals(partition, partitionKey.getPartitions());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partition.iterator().next() + '/' + tarFileName
        + '/' + xmlFileName, partitionKey.getPath());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partition.iterator().next(),
        partitionKey.getPathPartition());
  }

  @Test
  public void testRecordBatchUpdateXML() {
    PartitionKey partitionKey = new PartitionKey().batch(xmlDirName).record(xmlFileName).epochUpdate(epochUpdate);
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertEquals(xmlFileName, partitionKey.getRecord());
    Assert.assertEquals(xmlDirName, partitionKey.getBatch());
    Assert.assertEquals(epochPoll, partitionKey.getEpochGet());
    Assert.assertEquals(epochUpdate, partitionKey.getEpochUpdate());
    Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
    Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
    Assert.assertEquals(partition.iterator().next(), partitionKey.getPartition());
    Assert.assertEquals(partition, partitionKey.getPartitions());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition.iterator().next() + '/' + xmlDirName
        + '/' + xmlFileName, partitionKey.getPath());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition.iterator().next(),
        partitionKey.getPathPartition());
  }

  @Test
  public void testPathTar() {
    PartitionKey partitionKey = new PartitionKey().path("file:///some/dir/" + TAR_EXTENSION + '/' + TAR_CODEC + '/'
        + partition.iterator().next() + '/' + tarFileName + '/' + tarFileName + "-some-extension");
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertEquals(tarFileName, partitionKey.getRecord());
    Assert.assertEquals(tarFileName, partitionKey.getBatch());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(TAR_EXTENSION, partitionKey.getType());
    Assert.assertEquals(TAR_CODEC, partitionKey.getCodec());
    Assert.assertEquals(partition.iterator().next(), partitionKey.getPartition());
    Assert.assertEquals(partition, partitionKey.getPartitions());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partition.iterator().next() + '/' + tarFileName
        + '/' + tarFileName, partitionKey.getPath());
    Assert.assertEquals('/' + TAR_EXTENSION + '/' + TAR_CODEC + '/' + partition.iterator().next(),
        partitionKey.getPathPartition());
  }

  @Test
  public void testPathXML() {
    PartitionKey partitionKey = new PartitionKey().path("file:///some/dir/" + XML_EXTENSION + '/' + XML_CODEC + '/'
        + partition.iterator().next() + '/' + xmlDirName + '/' + xmlFileName + "-some-extension");
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertEquals(xmlFileName, partitionKey.getRecord());
    Assert.assertEquals(xmlDirName, partitionKey.getBatch());
    Assert.assertEquals(epochPoll, partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(XML_EXTENSION, partitionKey.getType());
    Assert.assertEquals(XML_CODEC, partitionKey.getCodec());
    Assert.assertEquals(partition.iterator().next(), partitionKey.getPartition());
    Assert.assertEquals(partition, partitionKey.getPartitions());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition.iterator().next() + '/' + xmlDirName
        + '/' + xmlFileName, partitionKey.getPath());
    Assert.assertEquals('/' + XML_EXTENSION + '/' + XML_CODEC + '/' + partition.iterator().next(),
        partitionKey.getPathPartition());
  }

  @Test
  public void testTypeCodecTar() {
    PartitionKey partitionKey = new PartitionKey().batch(tarFileName).record(tarFileName).type(SEQ_EXTENSION)
        .codec(SEQ_CODEC);
    Assert.assertEquals(tarFileName, partitionKey.getRecord());
    Assert.assertEquals(tarFileName, partitionKey.getBatch());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(SEQ_EXTENSION, partitionKey.getType());
    Assert.assertEquals(SEQ_CODEC, partitionKey.getCodec());
    Assert.assertEquals(partitions, partitionKey.getPartitions());
    Assert.assertEquals('/'
        + SEQ_EXTENSION
        + '/'
        + SEQ_CODEC
        + '/'
        + (partitionKey.getPartitions().size() == 1 ? partitionKey.getPartitions().iterator().next()
            : PARTITIONS_UNKNOWN.iterator().next()) + '/' + tarFileName + '/' + tarFileName, partitionKey.getPath());
    Assert.assertEquals('/'
        + SEQ_EXTENSION
        + '/'
        + SEQ_CODEC
        + '/'
        + (partitionKey.getPartitions().size() == 1 ? partitionKey.getPartitions().iterator().next()
            : PARTITIONS_UNKNOWN.iterator().next()), partitionKey.getPathPartition());
  }

  @Test
  public void testTypeCodecXML() {
    PartitionKey partitionKey = new PartitionKey().batch(xmlDirName).type(SEQ_EXTENSION).codec(SEQ_CODEC);
    Assert.assertEquals(xmlDirName, partitionKey.getRecord());
    Assert.assertEquals(xmlDirName, partitionKey.getBatch());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochGet());
    Assert.assertEquals(new Long(0L), partitionKey.getEpochUpdate());
    Assert.assertEquals(SEQ_EXTENSION, partitionKey.getType());
    Assert.assertEquals(SEQ_CODEC, partitionKey.getCodec());
    Assert.assertEquals(partitions, partitionKey.getPartitions());
    Assert.assertEquals('/'
        + SEQ_EXTENSION
        + '/'
        + SEQ_CODEC
        + '/'
        + (partitionKey.getPartitions().size() == 1 ? partitionKey.getPartitions().iterator().next()
            : PARTITIONS_UNKNOWN.iterator().next()) + '/' + xmlDirName + '/' + xmlDirName, partitionKey.getPath());
    Assert.assertEquals('/'
        + SEQ_EXTENSION
        + '/'
        + SEQ_CODEC
        + '/'
        + (partitionKey.getPartitions().size() == 1 ? partitionKey.getPartitions().iterator().next()
            : PARTITIONS_UNKNOWN.iterator().next()), partitionKey.getPathPartition());
  }

}
