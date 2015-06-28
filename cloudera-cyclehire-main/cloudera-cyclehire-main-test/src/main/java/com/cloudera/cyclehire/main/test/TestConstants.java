package com.cloudera.cyclehire.main.test;

import java.io.File;
import java.util.List;
import java.util.Map;

import com.cloudera.framework.main.test.BaseTest;

public interface TestConstants {

  // TODO - Cleanup
  public static final String DIR_DS = "cyclehire";
  public static final String DIR_DS_TAR_NOID = "tar-noid";
  public static final String DIR_DS_XML_NOID = "xml-noid";
  public static final String DIR_DS_XML_UUID = "xml-uuid";
  public static final String DIR_DSS_SINGLERECORDS = "single-records";
  public static final String DIR_DSS_MULTIRECORDS = "multi-records";
  public static final String DIR_DSS_MULTIPARTITIONS = "multi-partitions";
  public static final String DIR_DSS_EMPTYFILES = "empty-files";

  public static final Map<String, Map<String, List<File>>> FILES_DS = BaseTest.mapFilesLocal(BaseTest.REL_DIR_DATA)
      .get(DIR_DS);

  public static final String DIR_RAW_LANDED = "tmp/cyclehire/raw/landed";
  public static final String DIR_RAW_LANDED_TAR = DIR_RAW_LANDED + "/tar/gz";
  public static final String DIR_RAW_LANDED_XML = DIR_RAW_LANDED + "/xml/none";
  public static final String DIR_RAW_STAGED = "tmp/cyclehire/raw/staged";
  public static final String DIR_RAW_PARTITIONED = "tmp/cyclehire/raw/partitioned";
  public static final String DIR_PROCESSED = "tmp/cyclehire/processed";

  public static final String DIR_NON_EXISTANT = "some/non/existant/dir";
  public static final String REL_DIR_NON_EXISTANT = BaseTest.REL_DIR_DATA + "/" + DIR_NON_EXISTANT;

  public static final String ABS_DIR_DATASET = BaseTest.ABS_DIR_DATA + "/" + DIR_DS;

}
