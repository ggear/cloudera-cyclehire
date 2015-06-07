package com.cloudera.cyclehire.main.ingress;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
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
import com.cloudera.cyclehire.main.common.Driver;
import com.cloudera.cyclehire.main.common.hdfs.HDFSClientUtil;
import com.cloudera.cyclehire.main.ingress.copy.CopyDriver;
import com.cloudera.cyclehire.main.test.TestConstants;
import com.cloudera.framework.main.test.LocalClusterDfsMrBaseTest;

@RunWith(Parameterized.class)
public class CopyTest extends LocalClusterDfsMrBaseTest {

  public static String[] THREAD_NUMBER = new String[] { null, "1", "3" };
  public static String[] THREAD_QUEUE = new String[] { null,
      CopyDriver.CONF_THREAD_QUEUE_FILE, CopyDriver.CONF_THREAD_QUEUE_DIR };

  protected String threadNumber;
  protected String threadQueue;

  protected Driver copyDriver;

  public CopyTest(String theadNumber, String threadQueue) throws IOException {
    super();
    this.threadNumber = theadNumber;
    this.threadQueue = threadQueue;
  }

  @Parameters
  public static Collection<String[]> data() {
    List<String[]> records = new ArrayList<String[]>();
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
    Assert.assertEquals(Driver.RETURN_FAILURE_RUNTIME,
        copyDriver.runner(new String[0]));
    Assert
        .assertEquals(
            Driver.RETURN_FAILURE_RUNTIME,
            copyDriver
                .runner(new String[] { getPathLocal(TestConstants.PATH_LOCAL_DIR_NON_EXISTANT) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        copyDriver.runner(new String[] {
            getPathLocal(TestConstants.PATH_LOCAL_DIR_NON_EXISTANT),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        copyDriver.runner(new String[] {
            getPathLocal(TestConstants.PATH_LOCAL_DIR_NON_EXISTANT),
            getPathLocal(TestConstants.PATH_LOCAL_DIR_NON_EXISTANT),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        copyDriver.runner(new String[] {
            getPathLocal(TestConstants.PATH_LOCAL_DIR_NON_EXISTANT),
            getPathLocal(TestConstants.PATH_LOCAL_DIR_TAR),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
    Assert.assertEquals(
        Driver.RETURN_FAILURE_RUNTIME,
        copyDriver.runner(new String[] {
            getPathLocal(TestConstants.PATH_LOCAL_DIR_TAR),
            getPathLocal(TestConstants.PATH_LOCAL_DIR_TAR),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
  }

  @Test
  public void testExecuteDirExclude() throws Exception {

    List<Path> fileOutputs = null;
    int fileCountInput = new File(
        getPathLocal(TestConstants.PATH_LOCAL_DIR_TAR)).list().length;

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        copyDriver.runner(new String[] {
            getPathLocal(TestConstants.PATH_LOCAL_DIR_TAR),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
    fileOutputs = HDFSClientUtil.listFiles(getFileSystem(), new Path(
        getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED)), true);
    Assert.assertEquals(fileCountInput * 2, fileOutputs.size());
    Assert.assertEquals(
        0L,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
            Counter.FILES_FAILED).longValue());
    Assert.assertEquals(
        fileCountInput,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
            Counter.FILES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        fileCountInput,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
            Counter.FILES).longValue());

    copyDriver.reset();

    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        copyDriver.runner(new String[] {
            getPathLocal(TestConstants.PATH_LOCAL_DIR_TAR),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
    fileOutputs = HDFSClientUtil.listFiles(getFileSystem(), new Path(
        getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED)), true);
    Assert.assertEquals(fileCountInput * 2, fileOutputs.size());
    Assert.assertEquals(
        fileCountInput,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED).longValue());
    Assert.assertEquals(
        0L,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
            Counter.FILES_FAILED).longValue());
    Assert.assertEquals(
        0L,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
            Counter.FILES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        fileCountInput,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
            Counter.FILES).longValue());

    copyDriver.reset();

    Path dirTodo = fileOutputs.get(0).getParent();
    int fileCountSuccessful = HDFSClientUtil.listFiles(getFileSystem(),
        dirTodo, true).size() / 2;
    int fileCountFailed = 2;
    int fileCountTodo = fileOutputs.size() / 2 - fileCountSuccessful
        - fileCountFailed;
    getFileSystem().delete(dirTodo, true);
    getFileSystem().delete(fileOutputs.get(fileOutputs.size() - 4), true);
    getFileSystem().delete(fileOutputs.get(fileOutputs.size() - 1), true);
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        copyDriver.runner(new String[] {
            getPathLocal(TestConstants.PATH_LOCAL_DIR_TAR),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
    fileOutputs = HDFSClientUtil.listFiles(getFileSystem(), new Path(
        getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED)), true);
    Assert.assertEquals(fileCountInput * 2 - fileCountFailed,
        fileOutputs.size());
    Assert.assertEquals(
        fileCountTodo,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
            Counter.FILES_SKIPPED).longValue());
    Assert.assertEquals(
        fileCountFailed,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
            Counter.FILES_FAILED).longValue());
    Assert.assertEquals(
        fileCountSuccessful,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
            Counter.FILES_SUCCESSFUL).longValue());
    Assert.assertEquals(
        fileCountInput,
        copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
            Counter.FILES).longValue());

  }

  @Test
  public void testExecuteDirInclude() throws Exception {
    getConf().set(CopyDriver.CONF_DIR_INCLUDE, "true");
    try {

      List<Path> fileOutputs = null;
      int fileCountInput = new File(
          getPathLocal(TestConstants.PATH_LOCAL_DIRS_XML[0])).list().length;

      Assert.assertEquals(
          Driver.RETURN_SUCCESS,
          copyDriver.runner(new String[] {
              getPathLocal(TestConstants.PATH_LOCAL_DIRS_XML[0]),
              getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
      Assert
          .assertEquals(
              1,
              getFileSystem().listStatus(
                  new Path(getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED))).length);
      fileOutputs = HDFSClientUtil.listFiles(getFileSystem(), new Path(
          getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED)), true);
      Assert.assertEquals(fileCountInput * 2, fileOutputs.size());
      Assert.assertEquals(
          0L,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
              Counter.FILES_SKIPPED).longValue());
      Assert.assertEquals(
          0L,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
              Counter.FILES_FAILED).longValue());
      Assert.assertEquals(
          fileCountInput,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
              Counter.FILES_SUCCESSFUL).longValue());
      Assert.assertEquals(
          fileCountInput,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
              Counter.FILES).longValue());

      copyDriver.reset();

      Assert.assertEquals(
          Driver.RETURN_SUCCESS,
          copyDriver.runner(new String[] {
              getPathLocal(TestConstants.PATH_LOCAL_DIRS_XML[0]),
              getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
      Assert
          .assertEquals(
              1,
              getFileSystem().listStatus(
                  new Path(getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED))).length);
      fileOutputs = HDFSClientUtil.listFiles(getFileSystem(), new Path(
          getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED)), true);
      Assert.assertEquals(fileCountInput * 2, fileOutputs.size());
      Assert.assertEquals(
          fileCountInput,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
              Counter.FILES_SKIPPED).longValue());
      Assert.assertEquals(
          0L,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
              Counter.FILES_FAILED).longValue());
      Assert.assertEquals(
          0L,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
              Counter.FILES_SUCCESSFUL).longValue());
      Assert.assertEquals(
          fileCountInput,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
              Counter.FILES).longValue());

      copyDriver.reset();

      Path dirTodo = getFileSystem().listStatus(
          getFileSystem().listStatus(
              new Path(getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED)))[0]
              .getPath())[0].getPath();
      int fileCountSuccessful = HDFSClientUtil.listFiles(getFileSystem(),
          dirTodo, true).size() / 2;
      int fileCountFailed = 2;
      int fileCountTodo = fileOutputs.size() / 2 - fileCountSuccessful
          - fileCountFailed;
      getFileSystem().delete(dirTodo, true);
      getFileSystem().delete(fileOutputs.get(fileOutputs.size() - 4), true);
      getFileSystem().delete(fileOutputs.get(fileOutputs.size() - 1), true);
      Assert.assertEquals(
          Driver.RETURN_SUCCESS,
          copyDriver.runner(new String[] {
              getPathLocal(TestConstants.PATH_LOCAL_DIRS_XML[0]),
              getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
      Assert
          .assertEquals(
              1,
              getFileSystem().listStatus(
                  new Path(getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED))).length);
      fileOutputs = HDFSClientUtil.listFiles(getFileSystem(), new Path(
          getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED)), true);
      Assert.assertEquals(fileCountInput * 2 - fileCountFailed,
          fileOutputs.size());
      Assert.assertEquals(
          fileCountTodo,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
              Counter.FILES_SKIPPED).longValue());
      Assert.assertEquals(
          fileCountFailed,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
              Counter.FILES_FAILED).longValue());
      Assert.assertEquals(
          fileCountSuccessful,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
              Counter.FILES_SUCCESSFUL).longValue());
      Assert.assertEquals(
          fileCountInput,
          copyDriver.getCounter(CopyDriver.class.getCanonicalName(),
              Counter.FILES).longValue());

    } finally {
      getConf().unset(CopyDriver.CONF_DIR_INCLUDE);
    }
  }

}
