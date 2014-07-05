package com.cloudera.cyclehire.main.common.model;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.cyclehire.data.DataConstants;
import com.cloudera.cyclehire.main.common.model.PartitionKey;
import com.cloudera.cyclehire.main.common.model.PartitionRecord;
import com.cloudera.cyclehire.main.test.BaseTestCase;

public class PartitionRecordTest {

  private static final File FILE = new File(BaseTestCase.PATH_LOCAL_DIRS_XML[0]
      + '/' + DataConstants.PATH_LOCAL_XMLSS[0][50]);
  private static final PartitionKey KEY = new PartitionKey().batch(
      FILE.getParentFile().getName()).record(FILE.getName());
  private static String XML = "";
  static {
    try {
      XML = new String(Files.readAllBytes(Paths.get(FILE.toString())), "UTF-8");
    } catch (IOException e) {
    }
  }
  private static int XML_RECORDS = XML.split("<id>").length - 1;

  @Test
  public void testInvalid() {
    Assert.assertEquals(false, new PartitionRecord().isValid());
    Assert.assertEquals(false, new PartitionRecord().key(KEY).isValid());
    Assert.assertEquals(false, new PartitionRecord().xml(XML).isValid());
    Assert.assertEquals(false, new PartitionRecord().key(null).xml(XML)
        .isValid());
    Assert.assertEquals(false, new PartitionRecord().key(KEY).xml(null)
        .isValid());
    Assert
        .assertEquals(false, new PartitionRecord().key(KEY).xml("").isValid());
  }

  @Test
  public void testUpdate() {
    Assert.assertEquals(
        KEY.getEpochGet(),
        new PartitionRecord()
            .key(
                new PartitionKey().batch(KEY.getBatch())
                    .record(KEY.getRecord())).epochUpdate(null).getKey()
            .getEpochUpdate());
    Assert.assertEquals(
        KEY.getEpochGet(),
        new PartitionRecord()
            .key(
                new PartitionKey().batch(KEY.getBatch())
                    .record(KEY.getRecord())).epochUpdate("some-rubbish")
            .getKey().getEpochUpdate());
    Assert.assertEquals(
        KEY.getEpochGet(),
        new PartitionRecord()
            .key(
                new PartitionKey().batch(KEY.getBatch())
                    .record(KEY.getRecord())).epochUpdate("some-rubbish" + XML)
            .getKey().getEpochUpdate());
    Assert.assertEquals(
        KEY.getEpochGet(),
        new PartitionRecord()
            .key(
                new PartitionKey().batch(KEY.getBatch())
                    .record(KEY.getRecord())).epochUpdate(XML.substring(0, 70))
            .getKey().getEpochUpdate());
    Assert.assertNotSame(
        KEY.getEpochGet(),
        new PartitionRecord()
            .key(
                new PartitionKey().batch(KEY.getBatch())
                    .record(KEY.getRecord()))
            .epochUpdate(XML.substring(0, 200)).getKey().getEpochUpdate());
    Assert.assertNotSame(
        KEY.getEpochGet(),
        new PartitionRecord()
            .key(
                new PartitionKey().batch(KEY.getBatch())
                    .record(KEY.getRecord())).epochUpdate(XML + "some-rubbish")
            .getKey().getEpochUpdate());
    Assert.assertNotSame(
        KEY.getEpochGet(),
        new PartitionRecord()
            .key(
                new PartitionKey().batch(KEY.getBatch())
                    .record(KEY.getRecord())).epochUpdate(XML).getKey()
            .getEpochUpdate());
  }

  @Test
  public void testTable() {
    Assert.assertEquals(0, new PartitionRecord().key(KEY).xml(null).getTable()
        .size());
    Assert.assertEquals(0, new PartitionRecord().key(KEY).xml("").getTable()
        .size());
    Assert.assertEquals(0, new PartitionRecord().key(KEY).xml("some-rubbish")
        .getTable().size());
    Assert.assertEquals(0,
        new PartitionRecord().key(KEY).xml(XML + "some-rubbish").getTable()
            .size());
    Assert.assertEquals(0,
        new PartitionRecord().key(KEY).xml("some-rubbish" + XML).getTable()
            .size());
    Assert.assertEquals(0,
        new PartitionRecord().key(KEY).xml(XML.substring(0, 70)).getTable()
            .size());
    Assert.assertEquals(0,
        new PartitionRecord().key(KEY).xml(XML.substring(0, 200)).getTable()
            .size());
    Assert.assertEquals(XML_RECORDS, new PartitionRecord().key(KEY).xml(XML)
        .getTable().size());
    Assert.assertEquals(PartitionRecord.COLUMNS_RECORD_DETAIL.size(),
        new PartitionRecord().key(KEY).xml(XML).getTable().get(0).size());
    Assert.assertEquals(
        XML_RECORDS,
        new PartitionRecord()
            .key(KEY)
            .xml(
                XML.replace("<id>1</id>",
                    "<id>1</id><some-tag>some-value</some-tag>")).getTable()
            .size());
    Assert.assertEquals(
        PartitionRecord.COLUMNS_RECORD_DETAIL.size(),
        new PartitionRecord()
            .key(KEY)
            .xml(
                XML.replace("<id>1</id>",
                    "<id>1</id><some-tag>some-value</some-tag>")).getTable()
            .get(0).size());
  }

  @Test
  public void testRepeat() {
    PartitionRecord partitionKey = new PartitionRecord().key(KEY).xml(XML);
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertTrue(!partitionKey.getTable().isEmpty());
    Assert.assertTrue(!partitionKey.getTable().isEmpty());
  }

}
