package com.cloudera.cyclehire.main.test;

import com.cloudera.cyclehire.data.DataConstants;

public class TestConstants {

  public static final String PATH_LOCAL_DIR_NON_EXISTANT = "target/test-data/some/non/existant/dir";
  public static final String PATH_HDFS_DIR_NON_EXISTANT = "some/non/existant/dir";
  public static final String PATH_HDFS_DIR_RAW_LANDED = "tmp/cyclehire/raw/landed";
  public static final String PATH_HDFS_DIR_RAW_STAGED = "tmp/cyclehire/raw/staged";
  public static final String PATH_HDFS_DIR_RAW_PARTITIONED = "tmp/cyclehire/raw/partitioned";
  public static final String PATH_HDFS_DIR_PROCESSED = "tmp/cyclehire/processed";

  public static String PATH_LOCAL_DIR_TAR = DataConstants.PATH_LOCAL_TAR;
  public static String PATH_LOCAL_DIR_XML = DataConstants.PATH_LOCAL_XML;
  public static String[] PATH_LOCAL_DIRS_XML = new String[DataConstants.PATH_LOCAL_XMLS.length];
  static {
    for (int i = 0; i < DataConstants.PATH_LOCAL_XMLS.length; i++) {
      PATH_LOCAL_DIRS_XML[i] = DataConstants.PATH_LOCAL_XML + "/"
          + DataConstants.PATH_LOCAL_XMLS[i];
    }
  }

}
