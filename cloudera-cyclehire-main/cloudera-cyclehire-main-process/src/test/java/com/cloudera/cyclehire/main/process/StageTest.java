package com.cloudera.cyclehire.main.process;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.Driver;
import com.cloudera.cyclehire.main.common.hdfs.HDFSClientUtil;
import com.cloudera.cyclehire.main.process.stage.StageDriver;
import com.cloudera.cyclehire.main.test.TestConstants;

public class StageTest extends BaseTest {

  protected Driver stageDriver;

  @Before
  public void setUpDriver() throws Exception {
    stageDriver = new StageDriver(getConf());
  }

  @Test
  public void testStageInvalid() {
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        stageDriver.runner(new String[0]));
    Assert
        .assertEquals(
            Driver.RETURN_FAILURE_RUNTIME,
            stageDriver
                .runner(new String[] { getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        stageDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_NON_EXISTANT),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        stageDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_STAGED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_PARTITIONED) }));
  }

  @Test
  public void testStageValid() throws FileNotFoundException,
      IllegalArgumentException, IOException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        stageDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_STAGED) }));
    Assert.assertEquals(
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES).longValue(),
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.FILES_FAILED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));

  }

  @Test
  @SuppressWarnings("unchecked")
  public void testStageValidRinseRepeat() throws FileNotFoundException,
      IllegalArgumentException, IOException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        stageDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_STAGED) }));
    long filesCount = stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.FILES).longValue();
    long batchesCount = stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.BATCHES).longValue();
    long partitionsCount = stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue();
    Assert.assertEquals(
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES).longValue(),
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.FILES_FAILED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.FILES_SUCCESSFUL));
    Assert.assertTrue(stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.FILES_SKIPPED) > 0);
    Assert.assertTrue(stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.FILES_FAILED) > 0);
    Assert.assertTrue(stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL) > 0);
    Assert.assertTrue(stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.FILES) > 0);
    Assert.assertEquals(
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertTrue(stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED) > 0);
    Assert.assertTrue(stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.BATCHES_FAILED) > 0);
    Assert.assertTrue(stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED) > 0);
    Assert.assertTrue(stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED) > 0);
    Assert
        .assertTrue(stageDriver.getCounter(
            StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(stageDriver.getCounter(
        StageDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);

    stageDriver.reset();

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        stageDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_STAGED) }));
    Assert.assertEquals(
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES).longValue(),
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.FILES_FAILED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(
        filesCount,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES_FAILED).longValue());
    Assert.assertEquals(
        0L,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        filesCount,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES).longValue());
    Assert.assertEquals(
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        batchesCount,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES_FAILED).longValue());
    Assert.assertEquals(
        0L,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        batchesCount,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue());
    Assert.assertEquals(
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        partitionsCount,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS_FAILED).longValue());
    Assert.assertEquals(
        0L,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL).longValue());
    Assert.assertEquals(
        partitionsCount,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue());

    stageDriver.reset();

    List<Path> stagedPaths = HDFSClientUtil.listFiles(getFileSystem(),
        new Path(getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_STAGED),
            Counter.BATCHES_SUCCESSFUL.getPath()), true);
    Collections.sort(stagedPaths);
    Path stagedPathToDelete = stagedPaths.get(16).getParent();
    long filesCountDeleted = HDFSClientUtil.listFiles(getFileSystem(),
        stagedPathToDelete, false).size() - 1;
    getFileSystem().delete(stagedPathToDelete, true);
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        stageDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_STAGED) }));
    Assert.assertEquals(
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES).longValue(),
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.FILES_FAILED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(
        filesCount - filesCountDeleted,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES_FAILED).longValue());
    Assert.assertEquals(
        filesCountDeleted,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        filesCount,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.FILES).longValue());
    Assert.assertEquals(
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        batchesCount - 1,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES_FAILED).longValue());
    Assert.assertEquals(
        1L,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        batchesCount,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue());
    Assert.assertEquals(
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + stageDriver.getCounter(StageDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        partitionsCount - 1,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS_FAILED).longValue());
    Assert.assertEquals(
        1L,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL).longValue());
    Assert.assertEquals(
        partitionsCount,
        stageDriver.getCounter(StageDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue());

  }

}
