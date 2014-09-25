package com.cloudera.cyclehire.main.process.partition;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormatNoCheck;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.Driver;
import com.cloudera.cyclehire.main.common.hdfs.HDFSClientUtil;
import com.cloudera.cyclehire.main.common.mapreduce.MapReduceUtil;
import com.cloudera.cyclehire.main.common.model.PartitionFlag;
import com.cloudera.cyclehire.main.common.model.PartitionKey;

public class PartitionDriver extends Driver {

  public static final Counter[] COUNTERS = new Counter[] {
      Counter.BATCHES_SKIPPED, Counter.BATCHES_FAILED,
      Counter.BATCHES_SUCCESSFUL, Counter.BATCHES, Counter.PARTITIONS_SKIPPED,
      Counter.PARTITIONS_FAILED, Counter.PARTITIONS_SUCCESSFUL,
      Counter.PARTITIONS, Counter.RECORDS };

  public static final String OUTPUT_FORMAT = "sequence";

  private static final Logger log = LoggerFactory
      .getLogger(PartitionDriver.class);

  private Path inputStagedPath;
  private Path inputPartitionedPath;

  public PartitionDriver() {
    super();
  }

  public PartitionDriver(Configuration confguration) {
    super(confguration);
  }

  @Override
  public String description() {
    return "Partition a set of files";
  }

  @Override
  public String[] options() {
    return new String[] {};
  }

  @Override
  public String[] paramaters() {
    return new String[] { "hdfs-dir-staged", "hdfs-dir-partitioned" };
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

    inputStagedPath = new Path(arguments[0]);
    if (!hdfs.exists(inputStagedPath)
        || !HDFSClientUtil.canDoAction(hdfs, UserGroupInformation
            .getCurrentUser().getUserName(), UserGroupInformation
            .getCurrentUser().getGroupNames(), inputStagedPath, FsAction.READ)) {
      throw new Exception("HDFS staged directory [" + inputStagedPath
          + "] not available to user ["
          + UserGroupInformation.getCurrentUser().getUserName() + "]");
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS staged directory [" + inputStagedPath + "] validated");
    }

    inputPartitionedPath = new Path(arguments[1]);
    if (hdfs.exists(inputPartitionedPath)) {
      if (!hdfs.isDirectory(inputPartitionedPath)) {
        throw new Exception("HDFS partitioned directory ["
            + inputPartitionedPath + "] is not a directory");
      }
      if (!HDFSClientUtil
          .canDoAction(hdfs, UserGroupInformation.getCurrentUser()
              .getUserName(), UserGroupInformation.getCurrentUser()
              .getGroupNames(), inputPartitionedPath, FsAction.ALL)) {
        throw new Exception("HDFS partitioned directory ["
            + inputPartitionedPath
            + "] has too restrictive permissions to read/write as user ["
            + UserGroupInformation.getCurrentUser().getUserName() + "]");
      }
    } else {
      hdfs.mkdirs(inputPartitionedPath, new FsPermission(FsAction.ALL,
          FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS partitioned directory [" + inputPartitionedPath
          + "] validated");
    }

    return RETURN_SUCCESS;
  }

  @Override
  public int execute() throws InterruptedException, ExecutionException,
      IOException, ClassNotFoundException {

    FileSystem hdfs = FileSystem.newInstance(getConf());

    Set<String> counterBatches = new HashSet<String>();
    Set<String> counterPartitions = new HashSet<String>();
    List<Path> stagedPaths = new ArrayList<Path>();
    Map<String, PartitionKey> partitionKeys = new HashMap<String, PartitionKey>();
    for (Path stagedPath : HDFSClientUtil
        .listFiles(hdfs, inputStagedPath, true)) {
      if (!PartitionFlag.isValue(stagedPath.getName())) {
        PartitionKey partitionKey = new PartitionKey().path(stagedPath
            .toString());
        if (PartitionFlag.list(hdfs, stagedPath, PartitionFlag._PARTITION)) {
          stagedPaths.add(stagedPath);
          if (!partitionKeys.containsKey(partitionKey.getBatch())) {
            partitionKeys.put(partitionKey.getBatch(), partitionKey);
          }
        } else {
          incrementCounter(Counter.BATCHES_SKIPPED, 1,
              partitionKey.getPartition() + '/' + partitionKey.getBatch(),
              counterBatches);
          incrementCounter(Counter.PARTITIONS_SKIPPED, 1,
              partitionKey.getPartition(), counterPartitions);
        }
      }
    }

    Job job = null;
    boolean jobSuccess = partitionKeys.isEmpty();
    if (!partitionKeys.isEmpty()) {
      job = Job.getInstance(getConf());
      job.setJobName(getClass().getSimpleName());
      job.getConfiguration().set(
          FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
          Boolean.FALSE.toString());
      for (PartitionKey partitionKey : partitionKeys.values()) {
        if (PartitionInputFormat.supports(partitionKey.getType(),
            partitionKey.getCodec())) {
          MultipleInputs.addInputPath(
              job,
              new Path(new StringBuilder(PartitionKey.PATH_NOMINAL_LENGTH)
                  .append(inputStagedPath).append('/')
                  .append(Counter.BATCHES_SUCCESSFUL.getPath())
                  .append(partitionKey.getPathBatch()).toString()),
              PartitionInputFormat.get(partitionKey.getType(),
                  partitionKey.getCodec()));
        }
      }
      job.setMapOutputKeyClass(PartitionKey.class);
      job.setMapOutputValueClass(Text.class);
      job.setMapperClass(PartitionMapper.class);
      job.setNumReduceTasks(0);
      LazyOutputFormatNoCheck.setOutputFormatClass(job, TextOutputFormat.class);
      FileOutputFormat.setOutputPath(job, new Path(inputPartitionedPath,
          Counter.BATCHES_SUCCESSFUL.getPath()));
      SequenceFileOutputFormat.setOutputCompressionType(job,
          CompressionType.NONE);
      FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
      MultipleOutputs.addNamedOutput(job, OUTPUT_FORMAT,
          SequenceFileOutputFormat.class, PartitionKey.class, Text.class);
      job.setJarByClass(PartitionDriver.class);
      jobSuccess = job.waitForCompletion(log.isInfoEnabled());
      if (job != null) {
        importCounters(job, new Counter[] { Counter.RECORDS });
      }
    }

    for (Path stagedPath : stagedPaths) {
      PartitionKey partitionKey = new PartitionKey()
          .path(stagedPath.toString()).type(OUTPUT_FORMAT)
          .codec(MapReduceUtil.getCodecString(getConf()));
      Path partitionedPath = new Path(new StringBuffer(
          PartitionKey.PATH_NOMINAL_LENGTH)
          .append(inputPartitionedPath)
          .append('/')
          .append(Counter.BATCHES_SUCCESSFUL.getPath())
          .append(
              partitionKey.type(OUTPUT_FORMAT)
                  .codec(MapReduceUtil.getCodecString(getConf()))
                  .getPathPartition()).toString());
      boolean partitioned = HDFSClientUtil.listFiles(hdfs, partitionedPath,
          false).size() > 0;
      if (partitioned) {
        PartitionFlag.update(hdfs, partitionedPath, PartitionFlag._SUCCESS);
      }
      PartitionFlag.update(hdfs, stagedPath,
          partitioned ? PartitionFlag._CLEANSE : PartitionFlag._FAILED);
      incrementCounter(partitioned ? Counter.BATCHES_SUCCESSFUL
          : Counter.BATCHES_FAILED, 1, partitionKey.getPartition() + '/'
          + partitionKey.getBatch(), counterBatches);
      incrementCounter(partitioned ? Counter.PARTITIONS_SUCCESSFUL
          : Counter.PARTITIONS_FAILED, 1, partitionKey.getPartition(),
          counterPartitions);
    }
    incrementCounter(Counter.BATCHES, counterBatches.size());
    incrementCounter(Counter.PARTITIONS, counterPartitions.size());

    return jobSuccess ? RETURN_SUCCESS : RETURN_FAILURE_RUNTIME;

  }

  @Override
  public int cleanup() throws IOException {
    return RETURN_SUCCESS;
  }

  public static void main(String... arguments) throws Exception {
    System.exit(new PartitionDriver().runner(arguments));
  }

}
