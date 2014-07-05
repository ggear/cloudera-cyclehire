package com.cloudera.cyclehire.main.process;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.Driver;
import com.cloudera.cyclehire.main.process.cleanse.CleanseDriver;
import com.cloudera.cyclehire.main.process.partition.PartitionDriver;
import com.cloudera.cyclehire.main.process.stage.StageDriver;
import com.cloudera.cyclehire.main.test.BaseTestCase;

public class ProcessTest extends BaseTest {

  protected Driver processDriver;

  public ProcessTest() throws IOException {
    super();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    processDriver = new ProcessDriver(getFileSystem().getConf());
  }

  @Test
  public void testProcessInvalid() {
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        processDriver.runner(new String[0]));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, processDriver
        .runner(new String[] { BaseTestCase.PATH_HDFS_DIR_RAW_STAGED }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        processDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_NON_EXISTANT,
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED_CLEANSED }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        processDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_LANDED,
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED_CLEANSED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED_CLEANSED }));
  }

  @Test
  public void testProcessValid() throws FileNotFoundException,
      IllegalArgumentException, IOException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        processDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_LANDED,
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED_CLEANSED }));

    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.FILES_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.FILES_SUCCESSFUL));
    Assert.assertTrue(processDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.FILES_SKIPPED) > 0);
    Assert.assertTrue(processDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.FILES_FAILED) > 0);
    Assert.assertTrue(processDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL) > 0);
    Assert.assertTrue(processDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.FILES) > 0);
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertTrue(processDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED) > 0);
    Assert.assertTrue(processDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.BATCHES_FAILED) > 0);
    Assert.assertTrue(processDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(processDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(processDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED) > 0);
    Assert.assertTrue(processDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED) > 0);
    Assert
        .assertTrue(processDriver.getCounter(
            StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(processDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);

    Assert.assertEquals(
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + processDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertTrue(processDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        .longValue() > 0);
    Assert.assertTrue(processDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED) > 0);
    Assert
        .assertTrue(processDriver.getCounter(
            PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(processDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(processDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
        .longValue() > 0);
    Assert
        .assertTrue(processDriver.getCounter(
            PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED) > 0);
    Assert
        .assertTrue(processDriver.getCounter(
            PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(processDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);
    Assert.assertTrue(processDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.RECORDS) > 0);

    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertTrue(processDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        .longValue() > 0);
    Assert.assertEquals(new Long(0), processDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED));
    Assert
        .assertTrue(processDriver.getCounter(
            CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(processDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(processDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
        .longValue() > 0);
    Assert.assertEquals(new Long(0), processDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED));
    Assert
        .assertTrue(processDriver.getCounter(
            CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(processDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.RECORDS).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.RECORDS_MALFORMED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.RECORDS_DUPLICATE)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.RECORDS_CLEANSED));
    Assert.assertTrue(processDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED)
        .longValue() > 0);
    Assert.assertTrue(processDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE) > 0);
    Assert.assertTrue(processDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED) > 0);
    Assert.assertTrue(processDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS) > 0);

  }

}
