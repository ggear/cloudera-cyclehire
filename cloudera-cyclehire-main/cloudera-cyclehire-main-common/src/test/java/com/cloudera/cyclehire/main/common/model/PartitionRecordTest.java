package com.cloudera.cyclehire.main.common.model;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.cyclehire.main.test.TestConstants;

@RunWith(value = Parameterized.class)
public class PartitionRecordTest implements TestConstants {

  @Parameters()
  public static Iterable<Object[]> paramaters() {
    return Arrays.asList(new Object[][] {
        //
        { FILES_DS.get(DIR_DS_XML_NOID).get(DIR_DSS_SINGLERECORDS).get(0) },
        { FILES_DS.get(DIR_DS_XML_NOID).get(DIR_DSS_MULTIRECORDS).get(0) },
        { FILES_DS.get(DIR_DS_XML_NOID).get(DIR_DSS_MULTIPARTITIONS).get(0) },
        { FILES_DS.get(DIR_DS_XML_UUID).get(DIR_DSS_SINGLERECORDS).get(0) },
        { FILES_DS.get(DIR_DS_XML_UUID).get(DIR_DSS_MULTIRECORDS).get(0) },
        { FILES_DS.get(DIR_DS_XML_UUID).get(DIR_DSS_MULTIPARTITIONS).get(0) } });
  }

  private static final List<List<String>> TABLE_EMPTY = new ArrayList<>();
  static {
    TABLE_EMPTY.add(PartitionRecord.XML_RECORD_EMPTY);
  }

  private String xml;
  private int xmlRecords;
  private PartitionKey key;

  public PartitionRecordTest(File file) throws UnsupportedEncodingException, IOException {
    key = new PartitionKey().batch(file.getParentFile().getName()).record(file.getName());
    xml = new String(Files.readAllBytes(Paths.get(file.toString())), "UTF-8");
    xmlRecords = xml.split("<id>").length - 1;
  }

  @Test
  public void testInvalid() {
    Assert.assertEquals(false, new PartitionRecord().isValid());
    Assert.assertEquals(false, new PartitionRecord().key(key).isValid());
    Assert.assertEquals(false, new PartitionRecord().xml(xml).isValid());
    Assert.assertEquals(false, new PartitionRecord().key(null).xml(xml).isValid());
    Assert.assertEquals(false, new PartitionRecord().key(key).xml(null).isValid());
    Assert.assertEquals(false, new PartitionRecord().key(key).xml("").isValid());
  }

  @Test
  public void testUpdate() {
    Assert.assertEquals(key.getEpochGet(),
        new PartitionRecord().key(new PartitionKey().batch(key.getBatch()).record(key.getRecord())).epochUpdate(null)
            .getKey().getEpochUpdate());
    Assert.assertEquals(
        key.getEpochGet(),
        new PartitionRecord().key(new PartitionKey().batch(key.getBatch()).record(key.getRecord()))
            .epochUpdate("some-rubbish").getKey().getEpochUpdate());
    Assert.assertEquals(
        key.getEpochGet(),
        new PartitionRecord().key(new PartitionKey().batch(key.getBatch()).record(key.getRecord()))
            .epochUpdate("some-rubbish" + xml).getKey().getEpochUpdate());
    Assert.assertEquals(
        key.getEpochGet(),
        new PartitionRecord().key(new PartitionKey().batch(key.getBatch()).record(key.getRecord()))
            .epochUpdate(xml.substring(0, 70)).getKey().getEpochUpdate());
    Assert.assertNotSame(
        key.getEpochGet(),
        new PartitionRecord().key(new PartitionKey().batch(key.getBatch()).record(key.getRecord()))
            .epochUpdate(xml.substring(0, 200)).getKey().getEpochUpdate());
    Assert.assertNotSame(
        key.getEpochGet(),
        new PartitionRecord().key(new PartitionKey().batch(key.getBatch()).record(key.getRecord()))
            .epochUpdate(xml + "some-rubbish").getKey().getEpochUpdate());
    Assert.assertNotSame(key.getEpochGet(),
        new PartitionRecord().key(new PartitionKey().batch(key.getBatch()).record(key.getRecord())).epochUpdate(xml)
            .getKey().getEpochUpdate());
  }

  @Test
  public void testTable() {
    Assert.assertEquals(TABLE_EMPTY, new PartitionRecord().key(key).xml(null).getTable());
    Assert.assertEquals(TABLE_EMPTY, new PartitionRecord().key(key).xml("").getTable());
    Assert.assertEquals(TABLE_EMPTY, new PartitionRecord().key(key).xml("some-rubbish").getTable());
    Assert.assertEquals(TABLE_EMPTY, new PartitionRecord().key(key).xml(xml + "some-rubbish").getTable());
    Assert.assertEquals(TABLE_EMPTY, new PartitionRecord().key(key).xml("some-rubbish" + xml).getTable());
    Assert.assertEquals(TABLE_EMPTY, new PartitionRecord().key(key).xml(xml.substring(0, 70)).getTable());
    Assert.assertEquals(TABLE_EMPTY, new PartitionRecord().key(key).xml(xml.substring(0, 200)).getTable());
    Assert.assertEquals(xmlRecords, new PartitionRecord().key(key).xml(xml).getTable().size());
    Assert.assertEquals(PartitionRecord.XML_RECORD_COLUMNS.size(), new PartitionRecord().key(key).xml(xml).getTable()
        .get(0).size());
    Assert.assertEquals(xmlRecords,
        new PartitionRecord().key(key).xml(xml.replace("<id>1</id>", "<id>1</id><some-tag>some-value</some-tag>"))
            .getTable().size());
    Assert.assertEquals(PartitionRecord.XML_RECORD_COLUMNS.size(),
        new PartitionRecord().key(key).xml(xml.replace("<id>1</id>", "<id>1</id><some-tag>some-value</some-tag>"))
            .getTable().get(0).size());
  }

  @Test
  public void testRepeat() {
    PartitionRecord partitionKey = new PartitionRecord().key(key).xml(xml);
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertTrue(partitionKey.isValid());
    Assert.assertTrue(!partitionKey.getTable().isEmpty());
    Assert.assertTrue(!partitionKey.getTable().isEmpty());
  }

}
