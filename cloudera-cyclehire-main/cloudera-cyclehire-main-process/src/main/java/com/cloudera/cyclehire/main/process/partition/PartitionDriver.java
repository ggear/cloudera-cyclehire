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

  public static final String NAMED_OUTPUT_SEQUENCE = "sequence";

  private static final Logger log = LoggerFactory
      .getLogger(PartitionDriver.class);

  private Path hdfsStagedPath;
  private Path hdfsPartitionedPath;

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

    hdfsStagedPath = new Path(arguments[0]);
    if (!hdfs.exists(hdfsStagedPath)
        || !HDFSClientUtil.canDoAction(hdfs, UserGroupInformation
            .getCurrentUser().getUserName(), UserGroupInformation
            .getCurrentUser().getGroupNames(), hdfsStagedPath, FsAction.READ)) {
      throw new Exception("HDFS staged directory [" + hdfsStagedPath
          + "] not available to user ["
          + UserGroupInformation.getCurrentUser().getUserName() + "]");
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS staged directory [" + hdfsStagedPath + "] validated");
    }

    hdfsPartitionedPath = new Path(arguments[1]);
    if (hdfs.exists(hdfsPartitionedPath)) {
      if (!hdfs.isDirectory(hdfsPartitionedPath)) {
        throw new Exception("HDFS partitioned directory ["
            + hdfsPartitionedPath + "] is not a directory");
      }
      if (!HDFSClientUtil.canDoAction(hdfs, UserGroupInformation
          .getCurrentUser().getUserName(), UserGroupInformation
          .getCurrentUser().getGroupNames(), hdfsPartitionedPath, FsAction.ALL)) {
        throw new Exception("HDFS partitioned directory ["
            + hdfsPartitionedPath
            + "] has too restrictive permissions to read/write as user ["
            + UserGroupInformation.getCurrentUser().getUserName() + "]");
      }
    } else {
      hdfs.mkdirs(hdfsPartitionedPath, new FsPermission(FsAction.ALL,
          FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS partitioned directory [" + hdfsPartitionedPath
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
    Map<String, PartitionKey> partitionKeys = new HashMap<String, PartitionKey>();
    List<Path> partitionPaths = new ArrayList<Path>();
    for (Path pathStaged : HDFSClientUtil.listFiles(hdfs, hdfsStagedPath, true)) {
      if (!PartitionFlag.isValue(pathStaged.getName())) {
        PartitionKey partitionKey = new PartitionKey().path(pathStaged
            .toString());
        if (PartitionFlag.list(hdfs, pathStaged, PartitionFlag._PARTITION)) {
          partitionPaths.add(pathStaged);
          if (!partitionKeys.containsKey(partitionKey.getRecord())) {
            partitionKeys.put(partitionKey.getRecord(), partitionKey);
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
                  .append(hdfsStagedPath).append('/')
                  .append(Counter.BATCHES_SUCCESSFUL.getPath()).append('/')
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
      FileOutputFormat.setOutputPath(job, hdfsPartitionedPath);
      SequenceFileOutputFormat.setOutputCompressionType(job,
          CompressionType.NONE);
      FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
      MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT_SEQUENCE,
          SequenceFileOutputFormat.class, PartitionKey.class, Text.class);
      job.setJarByClass(PartitionDriver.class);
      jobSuccess = job.waitForCompletion(log.isInfoEnabled());
      if (job != null) {
        importCounters(job, new Counter[] { Counter.RECORDS });
      }
    }

    for (Path pathStaged : partitionPaths) {
      if (PartitionFlag.list(hdfs, pathStaged, PartitionFlag._PARTITION)) {
        PartitionKey partitionKey = new PartitionKey()
            .path(pathStaged.toString()).type(NAMED_OUTPUT_SEQUENCE)
            .codec(MapReduceUtil.getCodecString(getConf()));
        Path pathPartitioned = new Path(new StringBuffer(
            PartitionKey.PATH_NOMINAL_LENGTH)
            .append(hdfsPartitionedPath)
            .append(
                partitionKey.type(NAMED_OUTPUT_SEQUENCE)
                    .codec(MapReduceUtil.getCodecString(getConf()))
                    .getPathPartition()).toString());
        boolean partitioned = HDFSClientUtil.listFiles(hdfs, pathPartitioned,
            false).size() > 0;
        PartitionFlag.update(hdfs, pathStaged,
            partitioned ? PartitionFlag._CLENSE : PartitionFlag._FAILED);
        incrementCounter(partitioned ? Counter.BATCHES_SUCCESSFUL
            : Counter.BATCHES_FAILED, 1, partitionKey.getPartition() + '/'
            + partitionKey.getBatch(), counterBatches);
        incrementCounter(partitioned ? Counter.PARTITIONS_SUCCESSFUL
            : Counter.PARTITIONS_FAILED, 1, partitionKey.getPartition(),
            counterPartitions);
      }
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
