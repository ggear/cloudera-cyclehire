package com.cloudera.cyclehire.main.common;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.cloudera.cyclehire.main.common.Driver;
import com.cloudera.cyclehire.main.test.EmbeddedCoreTestCase;

public class DriverTest extends EmbeddedCoreTestCase {

  public DriverTest() throws IOException {
    super();
  }

  private Driver driver;

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    driver = new DriverNoOp(getFileSystem().getConf());
  }

  @Test
  public void testRunner() throws Exception {
    Assert.assertEquals(Driver.RETURN_SUCCESS, driver.runner(new String[0]));
  }

  private class DriverNoOp extends Driver {

    public DriverNoOp(Configuration confguration) {
      super(confguration);
    }

    @Override
    public int execute() {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
      }
      return RETURN_SUCCESS;
    }

  }
}
