package com.cloudera.cyclehire.main.process;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.Driver;
import com.cloudera.cyclehire.main.common.hdfs.HDFSClientUtil;
import com.cloudera.cyclehire.main.process.cleanse.CleanseDriver;
import com.cloudera.cyclehire.main.process.partition.PartitionDriver;
import com.cloudera.cyclehire.main.process.stage.StageDriver;
import com.cloudera.cyclehire.main.test.BaseTestCase;

public class CleanseTest extends BaseTest {

  protected Driver stageDriver;
  protected Driver cleanseDriver;
  protected Driver partitionDriver;

  public CleanseTest() throws IOException {
    super();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    stageDriver = new StageDriver(getFileSystem().getConf());
    partitionDriver = new PartitionDriver(getFileSystem().getConf());
    cleanseDriver = new CleanseDriver(getFileSystem().getConf());
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        stageDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_LANDED,
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED }));
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        partitionDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED }));
  }

  @Test
  public void testCleanseInvalid() {
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        cleanseDriver.runner(new String[0]));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, cleanseDriver
        .runner(new String[] { BaseTestCase.PATH_HDFS_DIR_RAW_STAGED }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        cleanseDriver.runner(new String[] {
            BaseTestCase.PATH_LOCAL_DIR_NON_EXISTANT,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        cleanseDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_NON_EXISTANT,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        cleanseDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED }));
  }

  @Test
  public void testCleanseValid() throws FileNotFoundException,
      IllegalArgumentException, IOException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        cleanseDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED }));
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testCleanseValidRinseRepeat() throws FileNotFoundException,
      IllegalArgumentException, IOException, InterruptedException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        cleanseDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED }));
    long batchesCount = cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.BATCHES).longValue();
    long partitionsCount = cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue();
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertTrue(cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        .longValue() > 0);
    Assert.assertEquals(new Long(0), cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED));
    Assert
        .assertTrue(cleanseDriver.getCounter(
            CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
        .longValue() > 0);
    Assert.assertEquals(new Long(0), cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED));
    Assert
        .assertTrue(cleanseDriver.getCounter(
            CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.RECORDS).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.RECORDS_MALFORMED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.RECORDS_DUPLICATE)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.RECORDS_CLEANSED));
    Assert.assertTrue(cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED)
        .longValue() > 0);
    Assert.assertTrue(cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE) > 0);
    Assert.assertTrue(cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED) > 0);
    Assert.assertTrue(cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS) > 0);

    cleanseDriver.reset();

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        cleanseDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED }));
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        batchesCount,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES_FAILED).longValue());
    Assert.assertEquals(
        0L,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        batchesCount,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue());
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        partitionsCount,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_FAILED).longValue());
    Assert.assertEquals(
        0L,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL).longValue());
    Assert.assertEquals(
        partitionsCount,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue());
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.RECORDS).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.RECORDS_MALFORMED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.RECORDS_DUPLICATE)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.RECORDS_CLEANSED));
    Assert.assertEquals(new Long(0), cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED));
    Assert.assertEquals(new Long(0), cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE));
    Assert.assertEquals(new Long(0), cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED));
    assertEquals(new Long(0), cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS));

    cleanseDriver.reset();

    List<Path> stagedPaths = HDFSClientUtil.listFiles(getFileSystem(),
        new Path(BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            Counter.BATCHES_SUCCESSFUL.getPath()), true);
    Collections.sort(stagedPaths);
    Path stagedPathToDelete = stagedPaths.get(1).getParent();
    getFileSystem().delete(stagedPathToDelete, true);
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        stageDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_LANDED,
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED }));
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        partitionDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED }));

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        cleanseDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED }));
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        batchesCount - 1,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES_FAILED).longValue());
    Assert.assertEquals(
        1L,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        batchesCount,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue());
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        partitionsCount - 1,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_FAILED).longValue());
    Assert.assertEquals(
        1L,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL).longValue());
    Assert.assertEquals(
        partitionsCount,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue());
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.RECORDS).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
            Counter.RECORDS_MALFORMED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.RECORDS_DUPLICATE)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
                Counter.RECORDS_CLEANSED));
    Assert.assertEquals(new Long(0), cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED));
    Assert.assertTrue(cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE) > 0);
    Assert.assertEquals(new Long(1), cleanseDriver.getCounter(
        CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED));
    assertTrue(cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(),
        Counter.RECORDS) > 0);

  }

}
