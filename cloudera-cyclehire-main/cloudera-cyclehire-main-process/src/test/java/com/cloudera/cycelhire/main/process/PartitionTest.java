package com.cloudera.cycelhire.main.process;

import java.io.IOException;

import org.junit.Assert;
import org.junit.Test;

import com.cloudera.cycelhire.main.common.Driver;
import com.cloudera.cycelhire.main.process.partition.PartitionDriver;
import com.cloudera.cycelhire.main.process.stage.StageDriver;
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
            BaseTestCase.PATH_HDFS_DIR_RAW_LANDING,
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGING }));
  }

  @Test
  public void testPartitionInvalid() {
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        partitionDriver.runner(new String[0]));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, partitionDriver
        .runner(new String[] { BaseTestCase.PATH_HDFS_DIR_RAW_STAGING }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        partitionDriver.runner(new String[] {
            BaseTestCase.PATH_LOCAL_DIR_NON_EXISTANT,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONING }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        partitionDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGING,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONING,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONING }));
  }

  @Test
  public void testPartitionValid() {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        partitionDriver.runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGING,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONING }));

  }

}
