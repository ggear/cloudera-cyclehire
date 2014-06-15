package com.cloudera.cyclehire.main.test;

import java.io.File;

import junit.framework.Assert;

import org.junit.Test;

public class BaseTestCaseTest {

  @Test
  public void testPathHDFS() {
    Assert.assertEquals(BaseTest.PATH_HDFS, BaseTestCase.getPathHDFS(""));
    Assert.assertEquals(BaseTest.PATH_HDFS, BaseTestCase.getPathHDFS("/"));
    Assert.assertEquals(BaseTest.PATH_HDFS, BaseTestCase.getPathHDFS("//"));
    Assert.assertEquals(BaseTest.PATH_HDFS + "/tmp", BaseTestCase.getPathHDFS("tmp"));
    Assert.assertEquals(BaseTest.PATH_HDFS + "/tmp", BaseTestCase.getPathHDFS("/tmp"));
    Assert.assertEquals(BaseTest.PATH_HDFS + "/tmp", BaseTestCase.getPathHDFS("//tmp"));
    Assert.assertEquals(BaseTest.PATH_HDFS + "/tmp", BaseTestCase.getPathHDFS("///tmp"));
    Assert.assertEquals(BaseTest.PATH_HDFS + "/tmp/tmp", BaseTestCase.getPathHDFS("///tmp//tmp"));
  }

  @Test
  public void testPathLocal() {
    String localDir = new File(".").getAbsolutePath();
    localDir = localDir.substring(0, localDir.length() - 2);
    Assert.assertEquals(localDir, BaseTestCase.getPathLocal(""));
    Assert.assertEquals(localDir, BaseTestCase.getPathLocal("/"));
    Assert.assertEquals(localDir, BaseTestCase.getPathLocal("//"));
    Assert.assertEquals(localDir + "/tmp", BaseTestCase.getPathLocal("tmp"));
    Assert.assertEquals(localDir + "/tmp", BaseTestCase.getPathLocal("/tmp"));
    Assert.assertEquals(localDir + "/tmp", BaseTestCase.getPathLocal("//tmp"));
    Assert.assertEquals(localDir + "/tmp", BaseTestCase.getPathLocal("///tmp"));
    Assert.assertEquals(localDir + "/tmp/tmp", BaseTestCase.getPathLocal("///tmp//tmp"));
  }

}
