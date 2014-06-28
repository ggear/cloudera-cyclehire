package com.cloudera.cycelhire.main.process.stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
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
    for (Counter counter : new Counter[] { Counter.FILES_SKIPPED,
        Counter.FILES_FAILED, Counter.FILES_SUCCESSFUL, Counter.FILES,
        Counter.BATCHES_SKIPPED, Counter.BATCHES_FAILED,
        Counter.BATCHES_SUCCESSFUL, Counter.BATCHES,
        Counter.PARTITIONS_SKIPPED, Counter.PARTITIONS_FAILED,
        Counter.PARTITIONS_SUCCESSFUL, Counter.PARTITIONS }) {
      incramentCounter(counter, 0);
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
        || !HDFSClientUtil.canDoAction(hdfs, UserGroupInformation
            .getCurrentUser().getUserName(), UserGroupInformation
            .getCurrentUser().getGroupNames(), hdfsLandingPath, FsAction.READ)) {
      throw new Exception("HDFS landing directory [" + hdfsLandingPath
          + "] not available to user ["
          + UserGroupInformation.getCurrentUser().getUserName() + "]");
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS landing directory [" + hdfsLandingPath + "] validated");
    }

    hdfsStagingPath = new Path(arguments[1]);
    if (hdfs.exists(hdfsStagingPath)) {
      if (!hdfs.isDirectory(hdfsStagingPath)) {
        throw new Exception("HDFS staging directory [" + hdfsStagingPath
            + "] is not a directory");
      }
      if (!HDFSClientUtil.canDoAction(hdfs, UserGroupInformation
          .getCurrentUser().getUserName(), UserGroupInformation
          .getCurrentUser().getGroupNames(), hdfsStagingPath, FsAction.ALL)) {
        throw new Exception("HDFS staging directory [" + hdfsStagingPath
            + "] has too restrictive permissions to read/write as user ["
            + UserGroupInformation.getCurrentUser().getUserName() + "]");
      }
    } else {
      hdfs.mkdirs(hdfsStagingPath, new FsPermission(FsAction.ALL,
          FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS staging directory [" + hdfsStagingPath + "] validated");
    }

    return RETURN_SUCCESS;
  }

  @Override
  public int execute() throws InterruptedException, ExecutionException,
      IOException, ClassNotFoundException {

    FileSystem hdfs = FileSystem.newInstance(getConf());

    Set<String> counterFiles = new HashSet<String>();
    Set<String> counterBatches = new HashSet<String>();
    Set<String> counterPartitions = new HashSet<String>();
    List<PartitionKey> counterPartitionKeys = new ArrayList<PartitionKey>();
    for (Path pathLanding : HDFSClientUtil.listFiles(hdfs, hdfsLandingPath,
        true)) {
      if (!PartitionFlag.isValue(pathLanding.getName())) {
        String pathLandingString = pathLanding.toString();
        if (PartitionFlag.list(hdfs, pathLanding, PartitionFlag._SUCCESS)) {
          for (PartitionKey partitionKey : PartitionKey.getKeys(pathLanding
              .getParent().getName(), pathLanding.getName())) {
            boolean pathValid = partitionKey.isValid()
                && hdfs.exists(new Path(pathLanding.getParent(), partitionKey
                    .getRecord()));
            Path pathStaging = new Path(new StringBuilder(
                PartitionKey.PATH_NOMINAL_LENGTH)
                .append(hdfsStagingPath)
                .append('/')
                .append(
                    pathValid ? Counter.BATCHES_SUCCESSFUL.getPath()
                        : Counter.BATCHES_FAILED.getPath()).append('/')
                .append(partitionKey.getPath()).toString());
            if (PartitionFlag.list(hdfs, pathStaging).isEmpty()) {
              if (pathValid) {
                HDFSClientUtil.createSymlinkOrCopy(hdfs, pathLanding,
                    pathStaging);
              } else {
                hdfs.createNewFile(pathStaging);
              }
              PartitionFlag.update(hdfs, pathStaging.getParent(),
                  pathValid ? PartitionFlag._PARTITION : PartitionFlag._FAILED);
              incramentCounter(pathValid ? Counter.FILES_SUCCESSFUL
                  : Counter.FILES_FAILED, 1, pathLandingString, counterFiles);
              incramentCounter(pathValid ? Counter.BATCHES_SUCCESSFUL
                  : Counter.BATCHES_FAILED, 1, partitionKey.getPartition()
                  + '/' + partitionKey.getBatch(), counterBatches);
              incramentCounter(pathValid ? Counter.PARTITIONS_SUCCESSFUL
                  : Counter.PARTITIONS_FAILED, 1, partitionKey.getPartition(),
                  counterPartitions);
            } else {
              counterPartitionKeys.add(partitionKey);
              incramentCounter(Counter.FILES_SKIPPED, 1, pathLandingString,
                  counterFiles);
            }
          }
        } else {
          incramentCounter(Counter.FILES_SKIPPED, 1, pathLandingString,
              counterFiles);
        }
      }
    }
    for (PartitionKey partitionKey : counterPartitionKeys) {
      if (!counterBatches.contains(partitionKey.getPartition() + '/'
          + partitionKey.getBatch())) {
        incramentCounter(Counter.BATCHES_SKIPPED, 1,
            partitionKey.getPartition() + '/' + partitionKey.getBatch(),
            counterBatches);
      }
      if (!counterPartitions.contains(partitionKey.getPartition())) {
        incramentCounter(Counter.PARTITIONS_SKIPPED, 1,
            partitionKey.getPartition(), counterPartitions);
      }
    }
    incramentCounter(Counter.FILES, counterFiles.size());
    incramentCounter(Counter.BATCHES, counterBatches.size());
    incramentCounter(Counter.PARTITIONS, counterPartitions.size());

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
