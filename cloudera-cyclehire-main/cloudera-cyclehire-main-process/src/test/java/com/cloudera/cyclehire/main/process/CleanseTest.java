package com.cloudera.cyclehire.main.process;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

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
import com.cloudera.cyclehire.main.process.cleanse.CleanseDriver;
import com.cloudera.cyclehire.main.process.partition.PartitionDriver;
import com.cloudera.cyclehire.main.process.stage.StageDriver;
import com.cloudera.cyclehire.main.test.TestConstants;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.BaseTest;
import com.cloudera.framework.main.test.LocalClusterDfsMrTest;

@RunWith(Parameterized.class)
public class CleanseTest extends LocalClusterDfsMrTest implements TestConstants {

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
        }, //
        new Map[] {
            //
            Collections.emptyMap(), //
            Collections.emptyMap(), //
            Collections.emptyMap(), //
        }, //
    }, //
    });
  }

  public CleanseTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
      String[][][] labels, @SuppressWarnings("rawtypes") Map[] counters) {
    super(sources, destinations, datasets, subsets, labels, counters);
  }

  protected Driver stageDriver;
  protected Driver cleanseDriver;
  protected Driver partitionDriver;

  @Before
  public void setUpDriver() throws Exception {
    stageDriver = new StageDriver(getConf());
    partitionDriver = new PartitionDriver(getConf());
    cleanseDriver = new CleanseDriver(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        stageDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED) }));
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        partitionDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_RAW_PARTITIONED) }));
  }

  @Test
  public void testCleanseInvalid() {
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, cleanseDriver.runner(new String[0]));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        cleanseDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        cleanseDriver.runner(new String[] { getPathLocal(REL_DIR_NON_EXISTANT), getPathDfs(DIR_RAW_PARTITIONED),
            getPathDfs(DIR_PROCESSED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        cleanseDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_NON_EXISTANT),
            getPathDfs(DIR_PROCESSED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        cleanseDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_RAW_PARTITIONED),
            getPathDfs(DIR_PROCESSED), getPathDfs(DIR_RAW_PARTITIONED), getPathDfs(DIR_PROCESSED) }));
  }

  @Test
  public void testCleanseValid() throws FileNotFoundException, IllegalArgumentException, IOException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        cleanseDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_RAW_PARTITIONED),
            getPathDfs(DIR_PROCESSED) }));
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));

  }

  // TODO
  @Ignore
  @Test
  @SuppressWarnings("unchecked")
  public void testCleanseValidRinseRepeat() throws FileNotFoundException, IllegalArgumentException, IOException,
      InterruptedException {

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        cleanseDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_RAW_PARTITIONED),
            getPathDfs(DIR_PROCESSED) }));
    long batchesCount = cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES).longValue();
    long partitionsCount = cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS)
        .longValue();
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertTrue(cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        .longValue() > 0);
    Assert.assertEquals(new Long(0),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED));
    Assert.assertTrue(cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
        .longValue() > 0);
    Assert.assertEquals(new Long(0),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED));
    Assert
        .assertTrue(cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED));
    Assert.assertTrue(cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED)
        .longValue() > 0);
    Assert.assertTrue(cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE) > 0);
    Assert.assertTrue(cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED) > 0);
    Assert.assertTrue(cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS) > 0);

    cleanseDriver.reset();

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        cleanseDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_RAW_PARTITIONED),
            getPathDfs(DIR_PROCESSED) }));
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(batchesCount,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(0L, cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
        .longValue());
    Assert.assertEquals(0L, cleanseDriver
        .getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(batchesCount, cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES)
        .longValue());
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(partitionsCount,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED).longValue());
    Assert.assertEquals(0L, cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
        .longValue());
    Assert.assertEquals(0L,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL).longValue());
    Assert.assertEquals(partitionsCount,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue());
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED));
    Assert.assertEquals(new Long(0),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED));
    Assert.assertEquals(new Long(0),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE));
    Assert.assertEquals(new Long(0),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED));
    Assert.assertEquals(new Long(0), cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS));

    cleanseDriver.reset();

    List<Path> stagedPaths = DfsUtil.listFiles(getFileSystem(), new Path(getPathDfs(DIR_RAW_STAGED),
        Counter.BATCHES_SUCCESSFUL.getPath()), true);
    Collections.sort(stagedPaths);
    Path stagedPathToDelete = stagedPaths.get(1).getParent();
    getFileSystem().delete(stagedPathToDelete, true);
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        stageDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED) }));
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        partitionDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_RAW_PARTITIONED) }));

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        cleanseDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_RAW_PARTITIONED),
            getPathDfs(DIR_PROCESSED) }));
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(batchesCount,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(0L, cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
        .longValue());
    Assert.assertEquals(0L, cleanseDriver
        .getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(batchesCount, cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.BATCHES)
        .longValue());
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(partitionsCount,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED).longValue());
    Assert.assertEquals(0L, cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
        .longValue());
    Assert.assertEquals(0L,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL).longValue());
    Assert.assertEquals(partitionsCount,
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue());
    Assert.assertEquals(
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS).longValue(),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE)
            + cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED));
    Assert.assertEquals(new Long(0),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_MALFORMED));
    Assert.assertEquals(new Long(0),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_DUPLICATE));
    Assert.assertEquals(new Long(0),
        cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS_CLEANSED));
    Assert.assertEquals(new Long(0), cleanseDriver.getCounter(CleanseDriver.class.getCanonicalName(), Counter.RECORDS));

  }

}
