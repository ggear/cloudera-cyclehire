package com.cloudera.cyclehire.main.process;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.DfsUtil;
import com.cloudera.cyclehire.main.process.clean.CleanDriver;
import com.cloudera.cyclehire.main.process.cleanse.CleanseDriver;
import com.cloudera.cyclehire.main.process.partition.PartitionDriver;
import com.cloudera.cyclehire.main.process.stage.StageDriver;
import com.cloudera.cyclehire.main.test.TestConstants;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.BaseTest;
import com.cloudera.framework.main.test.LocalClusterDfsMrTest;

@RunWith(Parameterized.class)
public class ProcessTest extends LocalClusterDfsMrTest implements TestConstants {

  @Parameters()
  public static Iterable<Object[]> paramaters() {
    return Arrays.asList(new Object[][] {
    //
    {
        //
        new String[] {
            //
            BaseTest.REL_DIR_DATA, //
            BaseTest.REL_DIR_DATA, }, //
        new String[] {
            //
            DIR_RAW_LANDED_TAR, DIR_RAW_LANDED_XML, }, //
        new String[] {
            //
            DIR_DS, DIR_DS }, //
        new String[][] {
            //
            { DIR_DS_TAR_NOID }, //
            { DIR_DS_XML_NOID }, //
        }, //
        new String[][][] {
            //
            { { null }, }, //
            { { null }, }, //
        } }, //
    });
  }

  public ProcessTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels) {
    super(sources, destinations, datasets, subsets, labels);
  }

  protected Driver cleanDriver;
  protected Driver processDriver;

  @Before
  public void setUpDriver() throws Exception {
    cleanDriver = new CleanDriver(getConf());
    processDriver = new ProcessDriver(getConf());
  }

  @Test
  public void testProcessInvalid() {
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, processDriver.runner(new String[0]));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        processDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        processDriver.runner(new String[] { getPathDfs(DIR_NON_EXISTANT), getPathDfs(DIR_RAW_STAGED),
            getPathDfs(DIR_RAW_PARTITIONED), getPathDfs(DIR_PROCESSED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        processDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED),
            getPathDfs(DIR_RAW_PARTITIONED), getPathDfs(DIR_PROCESSED), getPathDfs(DIR_PROCESSED) }));
  }

  @Test
  public void testProcessValid() throws FileNotFoundException, IllegalArgumentException, IOException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        processDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED),
            getPathDfs(DIR_RAW_PARTITIONED), getPathDfs(DIR_PROCESSED) }));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED));

  }

  // TODO
  @Ignore
  @Test
  @SuppressWarnings("unchecked")
  public void testProcessValidRinseRepeat() throws FileNotFoundException, IllegalArgumentException, IOException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        processDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED),
            getPathDfs(DIR_RAW_PARTITIONED), getPathDfs(DIR_PROCESSED) }));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED));

    processDriver.reset();

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        processDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED),
            getPathDfs(DIR_RAW_PARTITIONED), getPathDfs(DIR_PROCESSED) }));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED));

    processDriver.reset();

    List<Path> stagedPaths = DfsUtil.listFiles(getFileSystem(), new Path(getPathDfs(DIR_RAW_STAGED),
        Counter.BATCHES_SUCCESSFUL.getPath()), true);
    Collections.sort(stagedPaths);
    Path stagedPathToDelete = stagedPaths.get(1).getParent();
    getFileSystem().delete(stagedPathToDelete, true);
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        processDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED),
            getPathDfs(DIR_RAW_PARTITIONED), getPathDfs(DIR_PROCESSED) }));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED));

    processDriver.reset();

    Assert.assertEquals(Driver.RETURN_SUCCESS,
        cleanDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED) }));
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        processDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED),
            getPathDfs(DIR_RAW_PARTITIONED), getPathDfs(DIR_PROCESSED) }));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS).longValue(),
        processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE)
            + processDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED));

  }

}
