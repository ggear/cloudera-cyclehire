package com.cloudera.cyclehire.main.test;

import java.io.IOException;
import java.lang.reflect.Method;

import junit.extensions.TestSetup;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.mapred.HadoopTestCase;
import org.junit.After;
import org.junit.Before;

public abstract class EmbeddedCoreTestCase extends HadoopTestCase implements
    BaseTest {

  public EmbeddedCoreTestCase() throws IOException {
    super(HadoopTestCase.LOCAL_MR, HadoopTestCase.LOCAL_FS, 2, 2);
    BaseTestCase.init();
  }

  public void setUpClass() throws Exception {
  }

  public void tearDownClass() throws Exception {
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

  public junit.framework.Test getTestSuiteWithClassLifecycleMethods()
      throws Exception {
    TestSuite suite = new TestSuite();
    for (Method method : getClass().getMethods()) {
      if (method.getName().startsWith("test")) {
        TestCase testCase = getClass().getConstructor().newInstance();
        testCase.setName(method.getName());
        suite.addTest(testCase);
      }
    }
    TestSetup wrapper = new TestSetup(suite) {
      @Override
      protected void setUp() throws Exception {
        setUpClass();
      }

      @Override
      protected void tearDown() throws Exception {
        tearDownClass();
      }
    };
    return wrapper;
  }

}
