package com.cloudera.cycelhire.main.process;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.junit.Assert;

import com.cloudera.cycelhire.main.common.Driver;
import com.cloudera.cycelhire.main.ingress.copy.CopyDriver;
import com.cloudera.cyclehire.main.test.BaseTestCase;
import com.cloudera.cyclehire.main.test.EmbeddedCoreTestCase;

public abstract class BaseTest extends EmbeddedCoreTestCase {

  public BaseTest() throws IOException {
    super();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        new CopyDriver(getFileSystem().getConf()).runner(new String[] { BaseTestCase.PATH_LOCAL_DIR_TAR,
            BaseTestCase.PATH_HDFS_DIR_RAW_LANDING }));
    getFileSystem().copyFromLocalFile(new Path(BaseTestCase.PATH_LOCAL_DIR_XML),
        new Path(BaseTestCase.PATH_HDFS_DIR_RAW_LANDING));
  }

}
