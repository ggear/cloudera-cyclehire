package com.cloudera.cyclehire.main.process.cleanse;

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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
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
import com.cloudera.cyclehire.main.process.table.Table;

public class CleanseDriver extends Driver {

  public static final Counter[] COUNTERS = new Counter[] {
      Counter.BATCHES_SKIPPED, Counter.BATCHES_FAILED,
      Counter.BATCHES_SUCCESSFUL, Counter.BATCHES, Counter.PARTITIONS_SKIPPED,
      Counter.PARTITIONS_FAILED, Counter.PARTITIONS_SUCCESSFUL,
      Counter.PARTITIONS, Counter.RECORDS_MALFORMED, Counter.RECORDS_DUPLICATE,
      Counter.RECORDS_CLEANSED, Counter.RECORDS };

  public static final String NAMED_OUTPUT_SEQUENCE = "sequence";

  private static final Logger log = LoggerFactory
      .getLogger(CleanseDriver.class);

  private Path inputStagedPath;
  private Path inputPartitionedPath;
  private Path inputProcessedPath;

  public CleanseDriver() {
    super();
  }

  public CleanseDriver(Configuration confguration) {
    super(confguration);
  }

  @Override
  public String description() {
    return "Clense a set of files";
  }

  @Override
  public String[] options() {
    return new String[] {};
  }

  @Override
  public String[] paramaters() {
    return new String[] { "hdfs-dir-staged", "hdfs-dir-partitioned",
        "hdfs-dir-processed" };
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

    if (arguments == null || arguments.length != 3) {
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
    if (!hdfs.exists(inputPartitionedPath)
        || !HDFSClientUtil.canDoAction(hdfs, UserGroupInformation
            .getCurrentUser().getUserName(), UserGroupInformation
            .getCurrentUser().getGroupNames(), inputPartitionedPath,
            FsAction.READ)) {
      throw new Exception("HDFS partitioned directory [" + inputPartitionedPath
          + "] not available to user ["
          + UserGroupInformation.getCurrentUser().getUserName() + "]");
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS partitioned directory [" + inputPartitionedPath
          + "] validated");
    }

    inputProcessedPath = new Path(arguments[2]);
    if (hdfs.exists(inputProcessedPath)) {
      if (!hdfs.isDirectory(inputProcessedPath)) {
        throw new Exception("HDFS processed directory [" + inputProcessedPath
            + "] is not a directory");
      }
      if (!HDFSClientUtil.canDoAction(hdfs, UserGroupInformation
          .getCurrentUser().getUserName(), UserGroupInformation
          .getCurrentUser().getGroupNames(), inputProcessedPath, FsAction.ALL)) {
        throw new Exception("HDFS processed directory [" + inputProcessedPath
            + "] has too restrictive permissions to read/write as user ["
            + UserGroupInformation.getCurrentUser().getUserName() + "]");
      }
    } else {
      hdfs.mkdirs(inputProcessedPath, new FsPermission(FsAction.ALL,
          FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS processed directory [" + inputProcessedPath
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
        if (PartitionFlag.list(hdfs, stagedPath, PartitionFlag._CLEANSE)) {
          stagedPaths.add(stagedPath);
          if (!partitionKeys.containsKey(partitionKey.getPartition())) {
            partitionKeys.put(partitionKey.getPartition(), partitionKey);
            for (Counter counter : new Counter[] { Counter.RECORDS_MALFORMED,
                Counter.RECORDS_DUPLICATE, Counter.RECORDS_CLEANSED }) {
              Path cleansedPath = new Path(new StringBuilder(
                  PartitionKey.PATH_NOMINAL_LENGTH)
                  .append(inputProcessedPath)
                  .append('/')
                  .append(counter.getPath())
                  .append(
                      new PartitionKey().path(stagedPath.toString())
                          .type(NAMED_OUTPUT_SEQUENCE)
                          .codec(MapReduceUtil.getCodecString(getConf()))
                          .getPathPartition()).toString());
              hdfs.delete(cleansedPath, true);
            }
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
      job.setInputFormatClass(SequenceFileInputFormat.class);
      for (PartitionKey partitionKey : partitionKeys.values()) {
        FileInputFormat.addInputPath(
            job,
            new Path(new StringBuilder(PartitionKey.PATH_NOMINAL_LENGTH)
                .append(inputPartitionedPath)
                .append('/')
                .append(Counter.BATCHES_SUCCESSFUL.getPath())
                .append(
                    partitionKey.type(NAMED_OUTPUT_SEQUENCE)
                        .codec(MapReduceUtil.getCodecString(getConf()))
                        .getPathPartition()).toString()));
      }
      job.setMapOutputKeyClass(PartitionKey.class);
      job.setMapOutputValueClass(Text.class);
      job.setMapperClass(ClenseMapper.class);
      job.setPartitionerClass(ClenseReducerPartitioner.class);
      job.setGroupingComparatorClass(ClenseReducerGrouper.class);
      job.setSortComparatorClass(ClenseReducerSorter.class);
      job.setReducerClass(ClenseReducer.class);
      LazyOutputFormatNoCheck.setOutputFormatClass(job, TextOutputFormat.class);
      FileOutputFormat.setOutputPath(job, inputProcessedPath);
      SequenceFileOutputFormat.setOutputCompressionType(job,
          CompressionType.NONE);
      FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
      MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT_SEQUENCE,
          SequenceFileOutputFormat.class, NullWritable.class, Text.class);
      ClenseReducerPartitioner.setPartitions(job.getConfiguration(),
          partitionKeys.keySet());
      job.setNumReduceTasks(partitionKeys.size());
      job.setJarByClass(CleanseDriver.class);
      jobSuccess = job.waitForCompletion(log.isInfoEnabled());
      if (job != null) {
        importCounters(job, new Counter[] { Counter.RECORDS_MALFORMED,
            Counter.RECORDS_DUPLICATE, Counter.RECORDS_CLEANSED,
            Counter.RECORDS });
      }
    }

    for (Path stagedPath : stagedPaths) {
      PartitionKey partitionKey = new PartitionKey()
          .path(stagedPath.toString()).type(NAMED_OUTPUT_SEQUENCE)
          .codec(MapReduceUtil.getCodecString(getConf()));
      boolean cleansed = false;
      for (Counter counter : new Counter[] { Counter.RECORDS_MALFORMED,
          Counter.RECORDS_DUPLICATE, Counter.RECORDS_CLEANSED }) {
        Path cleansedPath = new Path(new StringBuilder(
            PartitionKey.PATH_NOMINAL_LENGTH).append(inputProcessedPath)
            .append('/').append(counter.getPath())
            .append(partitionKey.getPathPartition()).toString());
        if (HDFSClientUtil.listFiles(hdfs, cleansedPath, false).size() > 0) {
          cleansed = true;
          PartitionFlag.update(hdfs, cleansedPath, PartitionFlag._SUCCESS);
          if (counter.equals(Counter.RECORDS_CLEANSED)) {
            for (String rewriteFormat : Table.DDL_LOCATION_PROCESSED_REWRITE_FORMATS) {
              PartitionFlag.update(
                  hdfs,
                  new Path(cleansedPath
                      .toString()
                      .replace(Counter.RECORDS_CLEANSED.getPath(),
                          Counter.RECORDS_REWRITE.getPath())
                      .replace(
                          partitionKey.getType() + '/'
                              + partitionKey.getCodec(), rewriteFormat)),
                  PartitionFlag._REWRITE);
            }
          }
        }
      }
      PartitionFlag.update(hdfs, stagedPath, cleansed ? PartitionFlag._SUCCESS
          : PartitionFlag._FAILED);
      incrementCounter(cleansed ? Counter.BATCHES_SUCCESSFUL
          : Counter.BATCHES_FAILED, 1, partitionKey.getPartition() + '/'
          + partitionKey.getBatch(), counterBatches);
      incrementCounter(cleansed ? Counter.PARTITIONS_SUCCESSFUL
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
    System.exit(new CleanseDriver().runner(arguments));
  }

}
