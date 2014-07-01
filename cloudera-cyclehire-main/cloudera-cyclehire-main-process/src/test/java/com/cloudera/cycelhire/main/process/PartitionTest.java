package com.cloudera.cycelhire.main.process;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.cycelhire.main.common.Counter;
import com.cloudera.cycelhire.main.common.Driver;
import com.cloudera.cycelhire.main.process.partition.PartitionDriver;
import com.cloudera.cycelhire.main.process.stage.StageDriver;
import com.cloudera.cyclehire.main.common.hdfs.HDFSClientUtil;
import com.cloudera.cyclehire.main.test.BaseTestCase;

public class PartitionTest extends BaseTest {

  protected Driver stageDriver;
  protected Driver partitionDriver;

  public PartitionTest() throws IOException {
    super();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    stageDriver = new StageDriver(getFileSystem().getConf());
    partitionDriver = new PartitionDriver(getFileSystem().getConf());
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        stageDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_LANDED,
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED }));
  }

  @Test
  public void testPartitionInvalid() {
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        partitionDriver.runner(new String[0]));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, partitionDriver
        .runner(new String[] { BaseTestCase.PATH_HDFS_DIR_RAW_STAGED }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        partitionDriver.runner(new String[] {
            BaseTestCase.PATH_LOCAL_DIR_NON_EXISTANT,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        partitionDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED }));
  }

  @Test
  public void testPartitionValid() throws FileNotFoundException,
      IllegalArgumentException, IOException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        partitionDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED }));
    Assert.assertEquals(
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        .longValue() > 0);
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED) > 0);
    Assert
        .assertTrue(partitionDriver.getCounter(
            PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
        .longValue() > 0);
    Assert
        .assertTrue(partitionDriver.getCounter(
            PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED) > 0);
    Assert
        .assertTrue(partitionDriver.getCounter(
            PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.RECORDS) > 0);

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testPartitionValidRinseRepeat() throws FileNotFoundException,
      IllegalArgumentException, IOException, InterruptedException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        partitionDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED }));
    long batchesCount = partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.BATCHES).longValue();
    long partitionsCount = partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS)
        .longValue();
    Assert.assertEquals(
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        .longValue() > 0);
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED) > 0);
    Assert
        .assertTrue(partitionDriver.getCounter(
            PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
        .longValue() > 0);
    Assert
        .assertTrue(partitionDriver.getCounter(
            PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED) > 0);
    Assert
        .assertTrue(partitionDriver.getCounter(
            PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.RECORDS) > 0);

    partitionDriver.reset();

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        partitionDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED }));
    Assert.assertEquals(
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        batchesCount,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_FAILED).longValue());
    Assert.assertEquals(
        0L,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        batchesCount,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue());
    Assert.assertEquals(
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        partitionsCount,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_FAILED).longValue());
    Assert.assertEquals(
        0L,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL).longValue());
    Assert.assertEquals(
        partitionsCount,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue());
    Assert.assertEquals(new Long(0), partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.RECORDS));

    partitionDriver.reset();

    List<Path> stagedPaths = HDFSClientUtil.listFiles(getFileSystem(),
        new Path(BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            Counter.BATCHES_SUCCESSFUL.getPath()), true);
    Collections.sort(stagedPaths);
    Path stagedPathToDelete = stagedPaths.get(3).getParent();
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
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        batchesCount - 1,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_FAILED).longValue());
    Assert.assertEquals(
        1L,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        batchesCount,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue());
    Assert.assertEquals(
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + partitionDriver.getCounter(
                PartitionDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        partitionsCount - 1,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_FAILED).longValue());
    Assert.assertEquals(
        1L,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL).longValue());
    Assert.assertEquals(
        partitionsCount,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue());
    Assert.assertTrue(partitionDriver.getCounter(
        PartitionDriver.class.getCanonicalName(), Counter.RECORDS) > 0);

  }

}
