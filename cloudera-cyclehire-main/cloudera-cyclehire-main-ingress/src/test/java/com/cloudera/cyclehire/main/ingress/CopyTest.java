package com.cloudera.cyclehire.main.ingress;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.DfsUtil;
import com.cloudera.cyclehire.main.ingress.copy.CopyDriver;
import com.cloudera.cyclehire.main.test.TestConstants;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.BaseTest;
import com.cloudera.framework.main.test.LocalClusterDfsMrTest;

@RunWith(Parameterized.class)
public class CopyTest extends LocalClusterDfsMrTest implements TestConstants {

  @Parameters
  public static Iterable<Object[]> paramaters() {
    String[] THREAD_NUMBER = new String[] { null, "1", "3" };
    String[] THREAD_QUEUE = new String[] { null, CopyDriver.CONF_THREAD_QUEUE_FILE, CopyDriver.CONF_THREAD_QUEUE_DIR };
    List<Object[]> records = new ArrayList<Object[]>();
    for (int i = 0; i < THREAD_NUMBER.length; i++) {
      for (int j = 0; j < THREAD_QUEUE.length; j++) {
        List<String> record = new ArrayList<String>();
        record.add(THREAD_NUMBER[i]);
        record.add(THREAD_QUEUE[j]);
        records.add(record.toArray(new String[record.size()]));
      }
    }
    return records;
  }

  public static final String DIR_NONEXISTANT = BaseTest.ABS_DIR_DATA + "/some/non/existant/dir";
  public static final String DIR_TAR = FILES_DS.get(DIR_DS_TAR_NOID).get(DIR_DSS_MULTIRECORDS).get(0).getParent();
  public static final String DIR_XML = FILES_DS.get(DIR_DS_XML_UUID).get(DIR_DSS_MULTIRECORDS).get(0).getParent();

  protected String threadNumber;
  protected String threadQueue;

  protected Driver copyDriver;

  public CopyTest(String theadNumber, String threadQueue) throws IOException {
    super();
    this.threadNumber = theadNumber;
    this.threadQueue = threadQueue;
  }

  @Before
  public void setUpDriver() throws Exception {
    if (threadNumber != null) {
      getConf().set(CopyDriver.CONF_THREAD_NUMBER, threadNumber);
    }
    if (threadQueue != null) {
      getConf().set(CopyDriver.CONF_THREAD_QUEUE, threadQueue);
    }
    copyDriver = new CopyDriver(getConf());
  }

  @After
  public void tearDownDriver() throws Exception {
    if (threadNumber != null) {
      getConf().unset(CopyDriver.CONF_THREAD_NUMBER);
    }
    if (threadQueue != null) {
      getConf().unset(CopyDriver.CONF_THREAD_QUEUE);
    }
  }

  @Test
  public void testExecuteInvalid() throws Exception {
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, copyDriver.runner(new String[0]));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME, copyDriver.runner(new String[] { DIR_NONEXISTANT }));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        copyDriver.runner(new String[] { DIR_NONEXISTANT, getPathDfs(DIR_RAW_LANDED) }));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        copyDriver.runner(new String[] { DIR_NONEXISTANT, DIR_NONEXISTANT, getPathDfs(DIR_RAW_LANDED) }));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        copyDriver.runner(new String[] { DIR_NONEXISTANT, DIR_TAR, getPathDfs(DIR_RAW_LANDED) }));
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        copyDriver.runner(new String[] { DIR_TAR, DIR_TAR, getPathDfs(DIR_RAW_LANDED) }));
  }

  @Test
  public void testExecuteDirExclude() throws Exception {

    List<Path> fileOutputs = null;
    int fileCountInput = new File(DIR_TAR).list().length;

    Assert.assertEquals(Driver.RETURN_SUCCESS, copyDriver.runner(new String[] { DIR_TAR, getPathDfs(DIR_RAW_LANDED) }));
    fileOutputs = DfsUtil.listFiles(getFileSystem(), new Path(getPathDfs(DIR_RAW_LANDED)), true);
    Assert.assertEquals(fileCountInput * 2, fileOutputs.size());
    Assert.assertEquals(0L, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
        .longValue());
    Assert.assertEquals(0L, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_FAILED)
        .longValue());
    Assert.assertEquals(fileCountInput,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL).longValue());
    Assert.assertEquals(fileCountInput, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES)
        .longValue());

    copyDriver.reset();

    Assert.assertEquals(Driver.RETURN_SUCCESS, copyDriver.runner(new String[] { DIR_TAR, getPathDfs(DIR_RAW_LANDED) }));
    fileOutputs = DfsUtil.listFiles(getFileSystem(), new Path(getPathDfs(DIR_RAW_LANDED)), true);
    Assert.assertEquals(fileCountInput * 2, fileOutputs.size());
    Assert.assertEquals(fileCountInput,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_SKIPPED).longValue());
    Assert.assertEquals(0L, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_FAILED)
        .longValue());
    Assert.assertEquals(0L, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL)
        .longValue());
    Assert.assertEquals(fileCountInput, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES)
        .longValue());

    copyDriver.reset();

    Path dirTodo = fileOutputs.get(0).getParent();
    int fileCountSuccessful = DfsUtil.listFiles(getFileSystem(), dirTodo, true).size() / 2;
    int fileCountFailed = 2;
    int fileCountTodo = fileOutputs.size() / 2 - fileCountSuccessful - fileCountFailed;
    getFileSystem().delete(dirTodo, true);
    getFileSystem().delete(fileOutputs.get(fileOutputs.size() - 4), true);
    getFileSystem().delete(fileOutputs.get(fileOutputs.size() - 1), true);
    Assert.assertEquals(Driver.RETURN_SUCCESS, copyDriver.runner(new String[] { DIR_TAR, getPathDfs(DIR_RAW_LANDED) }));
    fileOutputs = DfsUtil.listFiles(getFileSystem(), new Path(getPathDfs(DIR_RAW_LANDED)), true);
    Assert.assertEquals(fileCountInput * 2 - fileCountFailed, fileOutputs.size());
    Assert.assertEquals(fileCountTodo, copyDriver
        .getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_SKIPPED).longValue());
    Assert.assertEquals(fileCountFailed,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_FAILED).longValue());
    Assert.assertEquals(fileCountSuccessful,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL).longValue());
    Assert.assertEquals(fileCountInput, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES)
        .longValue());

  }

  @Test
  public void testExecuteDirInclude() throws Exception {
    getConf().set(CopyDriver.CONF_DIR_INCLUDE, "true");
    try {

      List<Path> fileOutputs = null;
      int fileCountInput = new File(DIR_XML).list().length;

      Assert.assertEquals(Driver.RETURN_SUCCESS,
          copyDriver.runner(new String[] { DIR_XML, getPathDfs(DIR_RAW_LANDED) }));
      Assert.assertEquals(1, getFileSystem().listStatus(new Path(getPathDfs(DIR_RAW_LANDED))).length);
      fileOutputs = DfsUtil.listFiles(getFileSystem(), new Path(getPathDfs(DIR_RAW_LANDED)), true);
      Assert.assertEquals(fileCountInput * 2, fileOutputs.size());
      Assert.assertEquals(0L, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_SKIPPED)
          .longValue());
      Assert.assertEquals(0L, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_FAILED)
          .longValue());
      Assert.assertEquals(fileCountInput,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL).longValue());
      Assert.assertEquals(fileCountInput, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES)
          .longValue());

      copyDriver.reset();

      Assert.assertEquals(Driver.RETURN_SUCCESS,
          copyDriver.runner(new String[] { DIR_XML, getPathDfs(DIR_RAW_LANDED) }));
      Assert.assertEquals(1, getFileSystem().listStatus(new Path(getPathDfs(DIR_RAW_LANDED))).length);
      fileOutputs = DfsUtil.listFiles(getFileSystem(), new Path(getPathDfs(DIR_RAW_LANDED)), true);
      Assert.assertEquals(fileCountInput * 2, fileOutputs.size());
      Assert.assertEquals(fileCountInput,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_SKIPPED).longValue());
      Assert.assertEquals(0L, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_FAILED)
          .longValue());
      Assert.assertEquals(0L, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL)
          .longValue());
      Assert.assertEquals(fileCountInput, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES)
          .longValue());

      copyDriver.reset();

      Path dirTodo = getFileSystem().listStatus(
          getFileSystem().listStatus(new Path(getPathDfs(DIR_RAW_LANDED)))[0].getPath())[0].getPath();
      int fileCountSuccessful = DfsUtil.listFiles(getFileSystem(), dirTodo, true).size() / 2;
      int fileCountFailed = 2;
      int fileCountTodo = fileOutputs.size() / 2 - fileCountSuccessful - fileCountFailed;
      getFileSystem().delete(dirTodo, true);
      getFileSystem().delete(fileOutputs.get(fileOutputs.size() - 4), true);
      getFileSystem().delete(fileOutputs.get(fileOutputs.size() - 1), true);
      Assert.assertEquals(Driver.RETURN_SUCCESS,
          copyDriver.runner(new String[] { DIR_XML, getPathDfs(DIR_RAW_LANDED) }));
      Assert.assertEquals(1, getFileSystem().listStatus(new Path(getPathDfs(DIR_RAW_LANDED))).length);
      fileOutputs = DfsUtil.listFiles(getFileSystem(), new Path(getPathDfs(DIR_RAW_LANDED)), true);
      Assert.assertEquals(fileCountInput * 2 - fileCountFailed, fileOutputs.size());
      Assert.assertEquals(fileCountTodo,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_SKIPPED).longValue());
      Assert.assertEquals(fileCountFailed,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_FAILED).longValue());
      Assert.assertEquals(fileCountSuccessful,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL).longValue());
      Assert.assertEquals(fileCountInput, copyDriver.getCounter(CopyDriver.class.getCanonicalName(), Counter.FILES)
          .longValue());

    } finally {
      getConf().unset(CopyDriver.CONF_DIR_INCLUDE);
    }
  }

}
