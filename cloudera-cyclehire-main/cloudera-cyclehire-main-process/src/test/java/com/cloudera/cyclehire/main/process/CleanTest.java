package com.cloudera.cyclehire.main.process;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.process.clean.CleanDriver;
import com.cloudera.cyclehire.main.test.TestConstants;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.BaseTest;
import com.cloudera.framework.main.test.LocalClusterDfsMrTest;

@RunWith(Parameterized.class)
public class CleanTest extends LocalClusterDfsMrTest implements TestConstants {

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

  public CleanTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels) {
    super(sources, destinations, datasets, subsets, labels);
  }

  protected Driver cleanDriver;
  protected Driver processDriver;

  @Before
  public void setUpDriver() throws Exception {
    cleanDriver = new CleanDriver(getConf());
    processDriver = new ProcessDriver(getConf());
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        processDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED),
            getPathDfs(DIR_RAW_PARTITIONED), getPathDfs(DIR_PROCESSED) }));
  }

  @Test
  public void testCleanInvalid() {
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, cleanDriver.runner(new String[0]));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, cleanDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED) }));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        cleanDriver.runner(new String[] { getPathDfs(DIR_NON_EXISTANT), getPathDfs(DIR_RAW_LANDED) }));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        cleanDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_NON_EXISTANT) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        cleanDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED),
            getPathDfs(DIR_RAW_PARTITIONED) }));
  }

  @Test
  public void testCleanValid() throws FileNotFoundException, IllegalArgumentException, IOException {

    Assert.assertEquals(Driver.RETURN_SUCCESS,
        cleanDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED) }));
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));

  }

  // TODO
  @Ignore
  @Test
  public void testCleanValidRinseRepeat() throws FileNotFoundException, IllegalArgumentException, IOException {

    Assert.assertEquals(Driver.RETURN_SUCCESS,
        cleanDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED) }));
    long filesCount = cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SKIPPED).longValue();
    long batchesCount = cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
        .longValue();
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL));
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SKIPPED) > 0);
    Assert.assertEquals(new Long(0L),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_FAILED));
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES) > 0);
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED) > 0);
    Assert.assertEquals(new Long(0L),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_FAILED));
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED) > 0);
    Assert.assertEquals(new Long(0L),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED));
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);

    cleanDriver.reset();

    Assert.assertEquals(Driver.RETURN_SUCCESS,
        cleanDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED) }));
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(filesCount, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
        .longValue());
    Assert.assertEquals(0L, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_FAILED)
        .longValue());
    Assert.assertEquals(0L, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL)
        .longValue());
    Assert.assertEquals(filesCount, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES)
        .longValue());
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(batchesCount,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(0L, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
        .longValue());
    Assert.assertEquals(0L, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL)
        .longValue());
    Assert.assertEquals(batchesCount, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES)
        .longValue());
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED) > 0);
    Assert.assertEquals(0L, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
        .longValue());
    Assert.assertEquals(0L, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL)
        .longValue());
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);

    cleanDriver.reset();

    getFileSystem().delete(new Path(getPathDfs(DIR_RAW_LANDED)), true);
    getFileSystem().copyFromLocalFile(
        new Path(getPathLocal(FILES_DS.get(DIR_DS_XML_UUID).get(DIR_DSS_SINGLERECORDS).get(0).getParent())),
        new Path(getPathDfs(DIR_RAW_LANDED)));
    Assert.assertEquals(Driver.RETURN_SUCCESS,
        cleanDriver.runner(new String[] { getPathDfs(DIR_RAW_LANDED), getPathDfs(DIR_RAW_STAGED) }));
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL));
    Assert.assertEquals(filesCount, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
        .longValue());
    Assert.assertEquals(0L, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_FAILED)
        .longValue());
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.FILES) > 0);
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL));
    Assert.assertEquals(batchesCount,
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SKIPPED).longValue());
    Assert.assertEquals(0L, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_FAILED)
        .longValue());
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.BATCHES) > 0);
    Assert.assertEquals(
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS).longValue(),
        cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
            + cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL));
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED) > 0);
    Assert.assertEquals(0L, cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED)
        .longValue());
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL) > 0);
    Assert.assertTrue(cleanDriver.getCounter(CleanDriver.class.getCanonicalName(), Counter.PARTITIONS) > 0);

  }

}
