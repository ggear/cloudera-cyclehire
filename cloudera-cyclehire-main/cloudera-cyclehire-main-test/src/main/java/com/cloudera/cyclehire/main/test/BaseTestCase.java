package com.cloudera.cyclehire.main.test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;

import com.cloudera.cyclehire.data.DataConstants;

public abstract class BaseTestCase implements BaseTest {

  public static final String PATH_LOCAL_DIR_NON_EXISTANT = BaseTestCase
      .getPathLocal("target/test-data/some/non/existant/dir");
  public static final String PATH_HDFS_DIR_NON_EXISTANT = BaseTestCase
      .getPathHDFS("some/non/existant/dir");
  public static final String PATH_HDFS_DIR_RAW_LANDING = BaseTestCase
      .getPathHDFS("tmp/cyclehire/raw/landing");
  public static final String PATH_HDFS_DIR_RAW_STAGING = BaseTestCase
      .getPathHDFS("tmp/cyclehire/raw/staging");
  public static final String PATH_HDFS_DIR_RAW_PARTITIONING = BaseTestCase
      .getPathHDFS("tmp/cyclehire/raw/partition");

  public static String PATH_LOCAL_DIR_TAR = BaseTestCase
      .getPathLocal(DataConstants.PATH_LOCAL_TAR);
  public static String PATH_LOCAL_DIR_XML = BaseTestCase
      .getPathLocal(DataConstants.PATH_LOCAL_XML);
  public static String[] PATH_LOCAL_DIRS_XML = new String[DataConstants.PATH_LOCAL_XMLS.length];
  static {
    for (int i = 0; i < DataConstants.PATH_LOCAL_XMLS.length; i++) {
      PATH_LOCAL_DIRS_XML[i] = BaseTestCase
          .getPathLocal(DataConstants.PATH_LOCAL_XML + "/"
              + DataConstants.PATH_LOCAL_XMLS[i]);
    }
  }

  static {
    System.setProperty("java.security.krb5.realm", "CDHCLUSTER.com");
    System.setProperty("java.security.krb5.kdc", "kdc.cdhcluster.com");
    System.setProperty("java.security.krb5.conf", "/dev/null");

    System.setProperty("derby.stream.error.file",
        BaseTest.PATH_LOCAL_WORKING_DIR + "/target/derby.log");

    System.setProperty("dir.working", BaseTest.PATH_LOCAL_WORKING_DIR);
    System.setProperty("dir.working.target",
        BaseTest.PATH_LOCAL_WORKING_DIR_TARGET);
    System.setProperty("dir.working.target.hdfs",
        BaseTest.PATH_LOCAL_WORKING_DIR_TARGET_HDFS);
  }

  public static void initHadoopHome(String hadoopHome) {
    Map<String, String> env = new HashMap<String, String>();
    env.putAll(System.getenv());
    if (System.getenv(ENV_HADOOP_HOME) == null) {
      File target = new File(hadoopHome);
      if (target.exists()) {
        env.put(ENV_HADOOP_HOME, target.getAbsolutePath());
        BaseTestCase.setEnvionment(env);
      }
    }
  }

  public static void init() {
    try {
      Thread.sleep(1000);
    } catch (Exception exception) {
      throw new RuntimeException("Could not construct test", exception);
    }
    initHadoopHome(PATH_HADOOP_HOME);
  }

  public static void setUp(FileSystem fileSystem) throws IOException {
    if (fileSystem != null) {
      Path rootPath = new Path(BaseTestCase.getPathHDFS("/"));
      Path tmpPath = new Path(BaseTestCase.getPathHDFS("/tmp"));
      Path userPath = new Path(BaseTestCase.getPathHDFS("/user"));
      fileSystem.delete(rootPath, true);
      fileSystem.mkdirs(rootPath);
      fileSystem.mkdirs(tmpPath);
      fileSystem.setPermission(tmpPath, new FsPermission(FsAction.ALL,
          FsAction.ALL, FsAction.ALL));
      fileSystem.mkdirs(userPath);
      fileSystem.setPermission(userPath, new FsPermission(FsAction.ALL,
          FsAction.ALL, FsAction.ALL));
    }
  }

  public static void tearDown(FileSystem fileSystem) throws IOException {
    fileSystem.close();
  }

  public static String getPathLocal(String pathRelativeToModuleRoot) {
    String pathRelativeToModuleRootLessLeadingSlashes = stripLeadingSlashes(pathRelativeToModuleRoot);
    return pathRelativeToModuleRootLessLeadingSlashes.equals("") ? PATH_LOCAL_WORKING_DIR
        .length() < 2 ? "/" : PATH_LOCAL_WORKING_DIR.substring(0,
        PATH_LOCAL_WORKING_DIR.length() - 2) : new Path(PATH_LOCAL_WORKING_DIR,
        pathRelativeToModuleRootLessLeadingSlashes).toUri().toString();
  }

  public static String getPathHDFS(String pathRelativeToHDFSRoot) {
    String pathRelativeToHDFSRootLessLeadingSlashes = stripLeadingSlashes(pathRelativeToHDFSRoot);
    return pathRelativeToHDFSRootLessLeadingSlashes.equals("") ? PATH_HDFS
        : new Path(PATH_HDFS, pathRelativeToHDFSRootLessLeadingSlashes).toUri()
            .toString();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private static void setEnvionment(Map<String, String> newenv) {
    try {
      Class<?> processEnvironmentClass = Class
          .forName("java.lang.ProcessEnvironment");
      Field theEnvironmentField = processEnvironmentClass
          .getDeclaredField("theEnvironment");
      theEnvironmentField.setAccessible(true);
      Map<String, String> env = (Map<String, String>) theEnvironmentField
          .get(null);
      env.putAll(newenv);
      Field theCaseInsensitiveEnvironmentField = processEnvironmentClass
          .getDeclaredField("theCaseInsensitiveEnvironment");
      theCaseInsensitiveEnvironmentField.setAccessible(true);
      Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField
          .get(null);
      cienv.putAll(newenv);
    } catch (NoSuchFieldException e) {
      try {
        Class[] classes = Collections.class.getDeclaredClasses();
        Map<String, String> env = System.getenv();
        for (Class cl : classes) {
          if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
            Field field = cl.getDeclaredField("m");
            field.setAccessible(true);
            Object obj = field.get(env);
            Map<String, String> map = (Map<String, String>) obj;
            map.clear();
            map.putAll(newenv);
          }
        }
      } catch (Exception excpetion2) {
        // ignore
      }
    } catch (Exception expcetion1) {
      // ignore
    }
  }

  private static String stripLeadingSlashes(String string) {
    int indexAfterLeadingSlash = 0;
    while (indexAfterLeadingSlash < string.length()
        && string.charAt(indexAfterLeadingSlash) == '/')
      ++indexAfterLeadingSlash;
    return indexAfterLeadingSlash == 0 ? string : string.substring(
        indexAfterLeadingSlash, string.length());
  }

}
