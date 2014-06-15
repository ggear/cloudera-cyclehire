package com.cloudera.cyclehire.main.test;

import java.io.File;

public interface BaseTest {

  public static String ENV_HADOOP_HOME = "HADOOP_HOME";

  public static String PATH_HADOOP_HOME = "target/test-runtime/hadoop";

  public static String PATH_HDFS = "target/test-hdfs";
  public static String PATH_LOCAL = "target/test-local";

  public static String PATH_LOCAL_WORKING_DIR = new File(".").getAbsolutePath();
  public static String PATH_LOCAL_WORKING_DIR_TARGET = PATH_LOCAL_WORKING_DIR + "/target";
  public static String PATH_LOCAL_WORKING_DIR_TARGET_HDFS = PATH_LOCAL_WORKING_DIR_TARGET + "/test-hdfs";

}
