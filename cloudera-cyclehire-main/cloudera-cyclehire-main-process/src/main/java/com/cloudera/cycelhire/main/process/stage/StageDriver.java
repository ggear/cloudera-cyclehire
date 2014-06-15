package com.cloudera.cycelhire.main.process.stage;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cycelhire.main.common.Counter;
import com.cloudera.cycelhire.main.common.Driver;
import com.cloudera.cycelhire.main.common.model.PartitionFlag;
import com.cloudera.cycelhire.main.common.model.PartitionKey;
import com.cloudera.cyclehire.main.common.hdfs.HDFSClientUtil;

public class StageDriver extends Driver {

  private static final Logger log = LoggerFactory.getLogger(StageDriver.class);

  private Path hdfsLandingPath;
  private Path hdfsStagingPath;

  public StageDriver() {
    super();
  }

  public StageDriver(Configuration confguration) {
    super(confguration);
  }

  @Override
  public String description() {
    return "Stage a set of files in batch corelated partitions";
  }

  @Override
  public String[] options() {
    return new String[] {};
  }

  @Override
  public String[] paramaters() {
    return new String[] { "hdfs-dir-landing", "hdfs-dir-staging" };
  }

  @Override
  public void reset() {
    super.reset();
    for (Counter counter : new Counter[] { Counter.FILES_SKIPPED, Counter.FILES_FAILED, Counter.FILES_SUCCESSFUL,
        Counter.FILES, Counter.PARTITIONS_SKIPPED, Counter.PARTITIONS_FAILED, Counter.PARTITIONS_SUCCESSFUL,
        Counter.PARTITIONS }) {
      incramentCounter(StageDriver.class.getCanonicalName(), counter, 0);
    }
  }

  @Override
  public int prepare(String... arguments) throws Exception {

    if (arguments == null || arguments.length != 2) {
      throw new Exception("Invalid number of arguments");
    }

    FileSystem hdfs = FileSystem.newInstance(getConf());

    hdfsLandingPath = new Path(arguments[0]);
    if (!hdfs.exists(hdfsLandingPath)
        || !HDFSClientUtil.canDoAction(hdfs, UserGroupInformation.getCurrentUser().getUserName(), UserGroupInformation
            .getCurrentUser().getGroupNames(), hdfsLandingPath, FsAction.READ)) {
      throw new Exception("HDFS landing directory [" + hdfsLandingPath + "] not available to user ["
          + UserGroupInformation.getCurrentUser().getUserName() + "]");
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS landing directory [" + hdfsLandingPath + "] validated");
    }

    hdfsStagingPath = new Path(arguments[1]);
    if (hdfs.exists(hdfsStagingPath)) {
      if (!hdfs.isDirectory(hdfsStagingPath)) {
        throw new Exception("HDFS staging directory [" + hdfsStagingPath + "] is not a directory");
      }
      if (!HDFSClientUtil.canDoAction(hdfs, UserGroupInformation.getCurrentUser().getUserName(), UserGroupInformation
          .getCurrentUser().getGroupNames(), hdfsStagingPath, FsAction.ALL)) {
        throw new Exception("HDFS staging directory [" + hdfsStagingPath
            + "] has too restrictive permissions to read/write as user ["
            + UserGroupInformation.getCurrentUser().getUserName() + "]");
      }
    } else {
      hdfs.mkdirs(hdfsStagingPath, new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS staging directory [" + hdfsStagingPath + "] validated");
    }

    return RETURN_SUCCESS;
  }

  @Override
  public int execute() throws InterruptedException, ExecutionException, IOException, ClassNotFoundException {

    FileSystem hdfs = FileSystem.newInstance(getConf());
    for (Path pathLanding : HDFSClientUtil.listFiles(hdfs, hdfsLandingPath, true)) {
      if (!PartitionFlag.isValue(pathLanding.getName())
          && PartitionFlag.isValue(hdfs, pathLanding, PartitionFlag._SUCCESS)) {
        PartitionFlag partitionFlag = PartitionFlag._PARTITION;
        PartitionKey partitionKey = new PartitionKey();
        try {
          partitionKey.batch(pathLanding.getParent().getName()).build();
        } catch (IllegalArgumentException exception) {
          partitionFlag = PartitionFlag._FAILED;
        }
        String pathLandingName = pathLanding.getName();
        String pathLandingString = pathLanding.toString();
        String pathStagingStringPrefix = pathLandingString.substring(0,
            pathLandingString.length() - pathLandingName.length()).replace(hdfsLandingPath.toString(),
            hdfsStagingPath.toString());
        List<String> pathPartitionStrings = partitionFlag.equals(PartitionFlag._FAILED) ? Arrays
            .asList(new String[] { PartitionKey.PARTITION_UNKNOWN }) : partitionKey.getPartitions();
        boolean pathLandingSkipped = true;
        for (String partition : pathPartitionStrings) {
          StringBuilder pathStagingString = new StringBuilder(512);
          pathStagingString.append(pathStagingStringPrefix);
          pathStagingString.append(partition);
          pathStagingString.append('/');
          pathStagingString.append(pathLandingName);
          Path pathStaging = new Path(pathStagingString.toString());
          if (PartitionFlag.valueOf(hdfs, pathStaging) == null) {
            pathLandingSkipped = false;
            HDFSClientUtil.createSymlink(hdfs, pathLanding, pathStaging, getConf());
            hdfs.createNewFile(new Path(pathStaging.getParent(), partitionFlag.toString()));
            if (partitionFlag.equals(PartitionFlag._PARTITION)) {
              incramentCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SUCCESSFUL, 1);
            } else {
              incramentCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_FAILED, 1);
            }
          } else {
            incramentCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS_SKIPPED, 1);
          }
          incramentCounter(StageDriver.class.getCanonicalName(), Counter.PARTITIONS, 1);
        }
        if (pathLandingSkipped) {
          incramentCounter(StageDriver.class.getCanonicalName(), Counter.FILES_SKIPPED, 1);
        } else if (partitionFlag.equals(PartitionFlag._FAILED)) {
          incramentCounter(StageDriver.class.getCanonicalName(), Counter.FILES_FAILED, 1);
        } else {
          incramentCounter(StageDriver.class.getCanonicalName(), Counter.FILES_SUCCESSFUL, 1);
        }
        incramentCounter(StageDriver.class.getCanonicalName(), Counter.FILES, 1);
      }
    }

    return RETURN_SUCCESS;

  }

  @Override
  public int cleanup() throws IOException {
    return RETURN_SUCCESS;
  }

  public static void main(String... arguments) throws Exception {
    System.exit(new StageDriver().runner(arguments));
  }

}
