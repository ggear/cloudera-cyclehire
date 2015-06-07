package com.cloudera.cyclehire.main.process;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.Driver;
import com.cloudera.cyclehire.main.process.clean.CleanDriver;
import com.cloudera.cyclehire.main.test.TestConstants;

public class CleanTest extends BaseTest {

  protected Driver cleanDriver;
  protected Driver processDriver;

  @Before
  public void setUpDriver() throws Exception {
    cleanDriver = new CleanDriver(getConf());
    processDriver = new ProcessDriver(getConf());
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        processDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_STAGED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_PARTITIONED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_PROCESSED) }));
  }

  @Test
  public void testCleanInvalid() {
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        cleanDriver.runner(new String[0]));
    Assert
        .assertEquals(
            Driver.RETURN_FAILURE_RUNTIME,
            cleanDriver
                .runner(new String[] { getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        cleanDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_NON_EXISTANT),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        cleanDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_NON_EXISTANT) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        cleanDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_STAGED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_PARTITIONED) }));
  }

  @Test
  public void testCleanValid() throws FileNotFoundException,
      IllegalArgumentException, IOException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        cleanDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_STAGED) }));
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.FILES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));

  }

  @Test
  public void testCleanValidRinseRepeat() throws FileNotFoundException,
      IllegalArgumentException, IOException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        cleanDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_STAGED) }));
    long filesCount = cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
        .longValue();
    long batchesCount = cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        .longValue();
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.FILES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.FILES_SUCCESSFUL));
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.FILES_SKIPPED) > 0);
    Assert.assertEquals(new Long(0L), cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.FILES_FAILED));
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.FILES) > 0);
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED) > 0);
    Assert.assertEquals(new Long(0L), cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.BATCHES_FAILED));
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED) > 0);
    Assert.assertEquals(new Long(0L), cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED));
    Assert
        .assertTrue(cleanDriver.getCounter(
            CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);

    cleanDriver.reset();

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        cleanDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_STAGED) }));
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.FILES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(
        filesCount,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES_FAILED).longValue());
    Assert.assertEquals(
        0L,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        filesCount,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES).longValue());
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        batchesCount,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES_FAILED).longValue());
    Assert.assertEquals(
        0L,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        batchesCount,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue());
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED) > 0);
    Assert.assertEquals(
        0L,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.PARTITIONS_FAILED).longValue());
    Assert.assertEquals(
        0L,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SUCCESSFUL).longValue());
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);

    cleanDriver.reset();

    getFileSystem().delete(
        new Path(getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED)), true);
    getFileSystem().copyFromLocalFile(
        new Path(getPathLocal(TestConstants.PATH_LOCAL_DIR_XML)),
        new Path(getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED)));
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        cleanDriver.runner(new String[] {
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_STAGED) }));
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.FILES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(
        filesCount,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.FILES_FAILED).longValue());
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.FILES) > 0);
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.BATCHES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        batchesCount,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.BATCHES_FAILED).longValue());
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.PARTITIONS).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.PARTITIONS_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.PARTITIONS_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
                Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED) > 0);
    Assert.assertEquals(
        0L,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(),
            Counter.PARTITIONS_FAILED).longValue());
    Assert
        .assertTrue(cleanDriver.getCounter(
            CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanDriver.getCounter(
        CleanDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);

  }

}
