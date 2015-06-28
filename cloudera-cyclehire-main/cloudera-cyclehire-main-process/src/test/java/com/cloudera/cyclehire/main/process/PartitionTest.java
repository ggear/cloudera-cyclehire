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
import com.cloudera.cyclehire.main.process.partition.PartitionDriver;
import com.cloudera.cyclehire.main.process.stage.StageDriver;
import com.cloudera.cyclehire.main.test.TestConstants;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.BaseTest;
import com.cloudera.framework.main.test.LocalClusterDfsMrTest;

@RunWith(Parameterized.class)
public class PartitionTest extends LocalClusterDfsMrTest implements TestConstants {

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

  public PartitionTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets,
      String[][][] labels) {
    super(sources, destinations, datasets, subsets, labels);
  }

  protected Driver stageDriver;
  protected Driver partitionDriver;

  @Before
  public void setUpDriver() throws Exception {
    stageDriver = new StageDriver(getConf());
    partitionDriver = new PartitionDriver(getConf());
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        stageDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED) }));
  }

  @Test
  public void testPartitionInvalid() {
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, partitionDriver.runner(new String[0]));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        partitionDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED) }));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        partitionDriver.runner(new String[] { getPathLocal(REL_DIR_NON_EXISTANT), getPathDfs(DIR_RAW_PARTITIONED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        partitionDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_RAW_PARTITIONED),
            getPathDfs(DIR_RAW_PARTITIONED) }));
  }

  @Test
  public void testPartitionValid() throws FileNotFoundException, IllegalArgumentException, IOException {

    Assert.assertEquals(Driver.RETURN_SUCCESS,
        partitionDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_RAW_PARTITIONED) }));
    Assert.assertEquals(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES)
        .longValue(), partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS)
        .longValue(), partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));

  }

  // TODO
  @Ignore
  @Test
  @SuppressWarnings("unchecked")
  public void testPartitionValidRinseRepeat() throws FileNotFoundException, IllegalArgumentException, IOException,
      InterruptedException {

    Assert.assertEquals(Driver.RETURN_SUCCESS,
        partitionDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_RAW_PARTITIONED) }));
    long batchesCount = partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES)
        .longValue();
    long partitionsCount = partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS)
        .longValue();
    Assert.assertEquals(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES)
        .longValue(), partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertTrue(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        .longValue() > 0);
    Assert.assertTrue(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED) > 0);
    Assert
        .assertTrue(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS)
        .longValue(), partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
        .longValue() > 0);
    Assert
        .assertTrue(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED) > 0);
    Assert.assertTrue(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(),
        Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);
    Assert.assertTrue(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.RECORDS) > 0);

    partitionDriver.reset();

    Assert.assertEquals(Driver.RETURN_SUCCESS,
        partitionDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_RAW_PARTITIONED) }));
    Assert.assertEquals(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES)
        .longValue(), partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(batchesCount,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(0L, partitionDriver
        .getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED).longValue());
    Assert.assertEquals(0L,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(batchesCount,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES).longValue());
    Assert.assertEquals(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS)
        .longValue(), partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(partitionsCount,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED).longValue());
    Assert.assertEquals(0L,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED).longValue());
    Assert
        .assertEquals(0L,
            partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL)
                .longValue());
    Assert.assertEquals(partitionsCount,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue());
    Assert.assertEquals(new Long(0),
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.RECORDS));

    partitionDriver.reset();

    List<Path> stagedPaths = DfsUtil.listFiles(getFileSystem(), new Path(getPathDfs(DIR_RAW_STAGED),
        Counter.BATCHES_SUCCESSFUL.getPath()), true);
    Collections.sort(stagedPaths);
    Path stagedPathToDelete = stagedPaths.get(1).getParent();
    getFileSystem().delete(stagedPathToDelete, true);
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        stageDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED) }));
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        partitionDriver.runner(new String[] { getPathDfs(DIR_RAW_STAGED), getPathDfs(DIR_RAW_PARTITIONED) }));
    Assert.assertEquals(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES)
        .longValue(), partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(batchesCount - 1,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(1L, partitionDriver
        .getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_FAILED).longValue());
    Assert.assertEquals(0L,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL).longValue());
    Assert.assertEquals(batchesCount,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES).longValue());
    Assert.assertEquals(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS)
        .longValue(), partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
        + partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertEquals(partitionsCount - 1,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED).longValue());
    Assert.assertEquals(1L,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED).longValue());
    Assert
        .assertEquals(0L,
            partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL)
                .longValue());
    Assert.assertEquals(partitionsCount,
        partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue());
    Assert.assertTrue(partitionDriver.getCounter(PartitionDriver.class.getCanonicalName(), Counter.RECORDS) > 0);

  }

}
