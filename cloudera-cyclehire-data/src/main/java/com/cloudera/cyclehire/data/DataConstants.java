package com.cloudera.cyclehire.data;

import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;

public class DataConstants {

  public static final String PATH_LOCAL_TAR = "target/test-data/cyclehire/test/tar";
  public static final String[] PATH_LOCAL_TARS = new File(PATH_LOCAL_TAR)
      .list(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".tar.gz");
        }
      });
  static {
    Arrays.sort(PATH_LOCAL_TARS);
  }

  public static final String PATH_LOCAL_XML = "target/test-data/cyclehire/test/xml";
  public static final String[] PATH_LOCAL_XMLS = new File(PATH_LOCAL_XML)
      .list(new FilenameFilter() {
        @Override
        public boolean accept(File dir, String name) {
          return name.endsWith(".xml");
        }
      });
  static {
    Arrays.sort(PATH_LOCAL_XMLS);
  }
  public static final String[][] PATH_LOCAL_XMLSS = new String[PATH_LOCAL_XMLS.length][];
  static {
    for (int i = 0; i < PATH_LOCAL_XMLS.length; i++) {
      PATH_LOCAL_XMLSS[i] = new File(PATH_LOCAL_XML + "/" + PATH_LOCAL_XMLS[i])
          .list(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
              return name.endsWith(".xml");
            }
          });
    }
  }
  static {
    for (String[] pathLocalXMLs : PATH_LOCAL_XMLSS) {
      Arrays.sort(pathLocalXMLs);
    }
  }

}
