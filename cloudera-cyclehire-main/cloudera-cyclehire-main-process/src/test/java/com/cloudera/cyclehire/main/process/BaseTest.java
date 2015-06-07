package com.cloudera.cyclehire.main.process;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;

import com.cloudera.cyclehire.main.common.Driver;
import com.cloudera.cyclehire.main.ingress.copy.CopyDriver;
import com.cloudera.cyclehire.main.test.TestConstants;
import com.cloudera.framework.main.test.LocalClusterDfsMrBaseTest;

public abstract class BaseTest extends LocalClusterDfsMrBaseTest {

  @Before
  public void setUpIngress() throws Exception {
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        new CopyDriver(getConf()).runner(new String[] {
            getPathLocal(TestConstants.PATH_LOCAL_DIR_TAR),
            getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED) }));
    getFileSystem().copyFromLocalFile(
        new Path(getPathLocal(TestConstants.PATH_LOCAL_DIR_XML)),
        new Path(getPathDfs(TestConstants.PATH_HDFS_DIR_RAW_LANDED)));
  }

}
