package com.cloudera.cyclehire.main.test;

import java.io.IOException;

import org.apache.hadoop.mapred.HadoopTestCase;
import org.junit.After;
import org.junit.Before;

public abstract class EmbeddedCoreTestCase extends HadoopTestCase implements
    BaseTest {

  public EmbeddedCoreTestCase() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 2, 2);
    BaseTestCase.init();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    BaseTestCase.setUp(getFileSystem());
  }

  @After
  @Override
  public void tearDown() throws Exception {
    BaseTestCase.tearDown(getFileSystem());
    super.tearDown();
  }

}
