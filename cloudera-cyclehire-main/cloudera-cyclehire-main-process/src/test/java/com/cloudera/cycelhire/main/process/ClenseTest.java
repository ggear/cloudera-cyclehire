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
import com.cloudera.cycelhire.main.process.clense.ClenseDriver;
import com.cloudera.cycelhire.main.process.partition.PartitionDriver;
import com.cloudera.cycelhire.main.process.stage.StageDriver;
import com.cloudera.cyclehire.main.common.hdfs.HDFSClientUtil;
import com.cloudera.cyclehire.main.test.BaseTestCase;

public class ClenseTest extends BaseTest {

  protected Driver stageDriver;
  protected Driver clenseDriver;
  protected Driver partitionDriver;

  public ClenseTest() throws IOException {
    super();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    stageDriver = new StageDriver(getFileSystem().getConf());
    partitionDriver = new PartitionDriver(getFileSystem().getConf());
    clenseDriver = new ClenseDriver(getFileSystem().getConf());
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
  public void testClenseInvalid() {
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        clenseDriver.runner(new String[0]));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, clenseDriver
        .runner(new String[] { BaseTestCase.PATH_HDFS_DIR_RAW_STAGED }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        clenseDriver.runner(new String[] {
            BaseTestCase.PATH_LOCAL_DIR_NON_EXISTANT,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED_CLEANSED }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        clenseDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_NON_EXISTANT,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED_CLEANSED }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        clenseDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED_CLEANSED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED_CLEANSED }));
  }

  @Test
  public void testClenseValid() throws FileNotFoundException,
      IllegalArgumentException, IOException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        clenseDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED_CLEANSED }));
    // Assert.assertEquals(
    // clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
    // Counter.BATCHES).longValue(),
    // clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
    // Counter.BATCHES_SKIPPED)
    // + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
    // Counter.BATCHES_FAILED)
    // + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
    // Counter.BATCHES_SUCCESSFUL));
    // Assert.assertTrue(clenseDriver.getCounter(
    // ClenseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
    // .longValue() > 0);
    // Assert.assertTrue(clenseDriver.getCounter(
    // ClenseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED) > 0);
    // Assert.assertTrue(clenseDriver.getCounter(
    // ClenseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL) > 0);
    // Assert.assertTrue(clenseDriver.getCounter(
    // ClenseDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    // Assert.assertEquals(
    // clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
    // Counter.PARTITIONS).longValue(),
    // clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
    // Counter.PARTITIONS_SKIPPED)
    // + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
    // Counter.PARTITIONS_FAILED)
    // + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
    // Counter.PARTITIONS_SUCCESSFUL));
    // Assert.assertTrue(clenseDriver.getCounter(
    // ClenseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
    // .longValue() > 0);
    // Assert.assertTrue(clenseDriver.getCounter(
    // ClenseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED) > 0);
    // Assert
    // .assertTrue(clenseDriver.getCounter(
    // ClenseDriver.class.getCanonicalName(),
    // Counter.PARTITIONS_SUCCESSFUL) > 0);
    // Assert.assertTrue(clenseDriver.getCounter(
    // ClenseDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testClenseValidRinseRepeat() throws FileNotFoundException,
      IllegalArgumentException, IOException, InterruptedException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        clenseDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED_CLEANSED }));
    long batchesCount = clenseDriver.getCounter(
        ClenseDriver.class.getCanonicalName(), Counter.BATCHES).longValue();
    long partitionsCount = clenseDriver.getCounter(
        ClenseDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue();
    Assert.assertEquals(
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertTrue(clenseDriver.getCounter(
        ClenseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        .longValue() > 0);
    Assert.assertTrue(clenseDriver.getCounter(
        ClenseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED) > 0);
    Assert.assertTrue(clenseDriver.getCounter(
        ClenseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(clenseDriver.getCounter(
        ClenseDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(clenseDriver.getCounter(
        ClenseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
        .longValue() > 0);
    Assert.assertTrue(clenseDriver.getCounter(
        ClenseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED) > 0);
    Assert
        .assertTrue(clenseDriver.getCounter(
            ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(clenseDriver.getCounter(
        ClenseDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);

    clenseDriver.reset();

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        clenseDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED_CLEANSED }));
    Assert.assertEquals(
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        batchesCount,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES_FAILED).longValue());
    Assert.assertEquals(
        0L,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        batchesCount,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue());
    Assert.assertEquals(
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        partitionsCount,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_FAILED).longValue());
    Assert.assertEquals(
        0L,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL).longValue());
    Assert.assertEquals(
        partitionsCount,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue());

    clenseDriver.reset();

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
        clenseDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED_CLEANSED }));
    Assert.assertEquals(
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        batchesCount - 1,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES_FAILED).longValue());
    Assert.assertEquals(
        1L,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        batchesCount,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue());
    Assert.assertEquals(
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        partitionsCount - 1,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_FAILED).longValue());
    Assert.assertEquals(
        1L,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL).longValue());
    Assert.assertEquals(
        partitionsCount,
        clenseDriver.getCounter(ClenseDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue());

  }

}
