package com.cloudera.cyclehire.main.process.stage;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
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

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.DfsUtil;
import com.cloudera.cyclehire.main.common.model.PartitionFlag;
import com.cloudera.cyclehire.main.common.model.PartitionKey;
import com.cloudera.framework.main.common.Driver;

public class StageDriver extends Driver {

  public static final Counter[] COUNTERS = new Counter[] {
      Counter.FILES_SKIPPED, Counter.FILES_FAILED, Counter.FILES_SUCCESSFUL,
      Counter.FILES, Counter.BATCHES_SKIPPED, Counter.BATCHES_FAILED,
      Counter.BATCHES_SUCCESSFUL, Counter.BATCHES, Counter.PARTITIONS_SKIPPED,
      Counter.PARTITIONS_FAILED, Counter.PARTITIONS_SUCCESSFUL,
      Counter.PARTITIONS };

  private static final Logger LOG = LoggerFactory.getLogger(StageDriver.class);

  private Path inputLandedPath;
  private Path inputStagedPath;

  public StageDriver() {
    super();
  }

  public StageDriver(Configuration confguration) {
    super(confguration);
  }

  @Override
  public String description() {
    return "Stage a set of files";
  }

  @Override
  public String[] options() {
    return new String[] {};
  }

  @Override
  public String[] parameters() {
    return new String[] { "hdfs-dir-landed", "hdfs-dir-staged" };
  }

  @Override
  public void reset() {
    super.reset();
    for (Counter counter : COUNTERS) {
      incrementCounter(counter, 0);
    }
  }

  @Override
  public int prepare(String... arguments) throws Exception {

    if (arguments == null || arguments.length != 2) {
      throw new Exception("Invalid number of arguments");
    }

    FileSystem hdfs = FileSystem.newInstance(getConf());

    inputLandedPath = new Path(arguments[0]);
    if (!hdfs.exists(inputLandedPath)
        || !DfsUtil.canDoAction(hdfs, UserGroupInformation.getCurrentUser()
            .getUserName(), UserGroupInformation.getCurrentUser()
            .getGroupNames(), inputLandedPath, FsAction.READ)) {
      throw new Exception("HDFS landed directory [" + inputLandedPath
          + "] not available to user ["
          + UserGroupInformation.getCurrentUser().getUserName() + "]");
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("HDFS landed directory [" + inputLandedPath + "] validated");
    }

    inputStagedPath = new Path(arguments[1]);
    if (hdfs.exists(inputStagedPath)) {
      if (!hdfs.isDirectory(inputStagedPath)) {
        throw new Exception("HDFS staged directory [" + inputStagedPath
            + "] is not a directory");
      }
      if (!DfsUtil.canDoAction(hdfs, UserGroupInformation.getCurrentUser()
          .getUserName(),
          UserGroupInformation.getCurrentUser().getGroupNames(),
          inputStagedPath, FsAction.ALL)) {
        throw new Exception("HDFS staged directory [" + inputStagedPath
            + "] has too restrictive permissions to read/write as user ["
            + UserGroupInformation.getCurrentUser().getUserName() + "]");
      }
    } else {
      hdfs.mkdirs(inputStagedPath, new FsPermission(FsAction.ALL,
          FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
    }
    if (LOG.isInfoEnabled()) {
      LOG.info("HDFS staged directory [" + inputStagedPath + "] validated");
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
    Map<Path, PartitionKey> stagedFailed = new HashMap<>();
    Map<Path, PartitionKey> stagedTodo = new HashMap<>();
    for (Path landedPath : DfsUtil.listFiles(hdfs, inputLandedPath, true)) {
      if (!PartitionFlag.isValue(landedPath.getName())) {
        for (PartitionKey partitionKey : PartitionKey.getKeys(landedPath
            .getParent().getName(), landedPath.getName())) {
          boolean landedPathExists = hdfs.exists(new Path(landedPath
              .getParent(), partitionKey.getRecord()));
          Path stagedPath = new Path(
              new StringBuilder(PartitionKey.PATH_NOMINAL_LENGTH)
                  .append(inputStagedPath)
                  .append('/')
                  .append(
                      partitionKey.isValid() && landedPathExists ? Counter.BATCHES_SUCCESSFUL
                          .getPath() : Counter.BATCHES_FAILED.getPath())
                  .append('/').append(partitionKey.getPath()).toString());
          if (PartitionFlag.list(hdfs, landedPath, PartitionFlag._SUCCESS)
              && PartitionFlag.list(hdfs, stagedPath).isEmpty()) {
            if (partitionKey.isValid() && landedPathExists) {
              DfsUtil.createSymlinkOrCopy(hdfs, landedPath, stagedPath);
              PartitionFlag.update(hdfs, stagedPath.getParent(),
                  PartitionFlag._PARTITION);
              incrementCounter(Counter.FILES_SUCCESSFUL, 1,
                  partitionKey.getBatch() + '/' + partitionKey.getRecord(),
                  counterFiles);
              incrementCounter(Counter.BATCHES_SUCCESSFUL, 1,
                  partitionKey.getPartition() + '/' + partitionKey.getBatch(),
                  counterBatches);
              incrementCounter(Counter.PARTITIONS_SUCCESSFUL, 1,
                  partitionKey.getPartition(), counterPartitions);
            } else {
              stagedFailed.put(stagedPath, partitionKey);
              if (landedPathExists) {
                incrementCounter(Counter.FILES_FAILED, 1,
                    partitionKey.getBatch() + '/' + partitionKey.getRecord(),
                    counterFiles);
              }
            }
          } else {
            stagedTodo.put(stagedPath, partitionKey);
            if (landedPathExists) {
              incrementCounter(Counter.FILES_SKIPPED, 1,
                  partitionKey.getBatch() + '/' + partitionKey.getRecord(),
                  counterFiles);
            }
          }
        }
      }
    }
    for (Path stagedPath : stagedFailed.keySet()) {
      PartitionKey partitionKey = stagedFailed.get(stagedPath);
      hdfs.createNewFile(stagedPath);
      PartitionFlag.update(hdfs, stagedPath.getParent(), PartitionFlag._FAILED);
      incrementCounter(Counter.BATCHES_FAILED, 1, partitionKey.getPartition()
          + '/' + partitionKey.getBatch(), counterBatches);
      incrementCounter(Counter.PARTITIONS_FAILED, 1,
          partitionKey.getPartition(), counterPartitions);
    }
    for (Path stagedPath : stagedTodo.keySet()) {
      PartitionKey partitionKey = stagedTodo.get(stagedPath);
      incrementCounter(Counter.BATCHES_SKIPPED, 1, partitionKey.getPartition()
          + '/' + partitionKey.getBatch(), counterBatches);
      incrementCounter(Counter.PARTITIONS_SKIPPED, 1,
          partitionKey.getPartition(), counterPartitions);
    }
    incrementCounter(Counter.FILES, counterFiles.size());
    incrementCounter(Counter.BATCHES, counterBatches.size());
    incrementCounter(Counter.PARTITIONS, counterPartitions.size());

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
