package com.cloudera.cycelhire.main.process.clense;

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

import com.cloudera.cycelhire.main.common.Counter;
import com.cloudera.cycelhire.main.common.Driver;
import com.cloudera.cycelhire.main.common.model.PartitionFlag;
import com.cloudera.cycelhire.main.common.model.PartitionKey;
import com.cloudera.cyclehire.main.common.hdfs.HDFSClientUtil;
import com.cloudera.cyclehire.main.common.mapreduce.MapReduceUtil;

public class ClenseDriver extends Driver {

  public static final String NAMED_OUTPUT_SEQUENCE = "sequence";

  private static final Logger log = LoggerFactory.getLogger(ClenseDriver.class);

  private Path hdfsStagedPath;
  private Path hdfsPartitionedPath;
  private Path hdfsCleansedPath;

  public ClenseDriver() {
    super();
  }

  public ClenseDriver(Configuration confguration) {
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
        "hdfs-dir-cleansed" };
  }

  @Override
  public void reset() {
    super.reset();
    for (Counter counter : new Counter[] { Counter.BATCHES_SKIPPED,
        Counter.BATCHES_FAILED, Counter.BATCHES_SUCCESSFUL, Counter.BATCHES,
        Counter.PARTITIONS_SKIPPED, Counter.PARTITIONS_FAILED,
        Counter.PARTITIONS_SUCCESSFUL, Counter.PARTITIONS,
        Counter.RECORDS_MALFORMED, Counter.RECORDS_DUPLICATE,
        Counter.RECORDS_CLEANSED, Counter.RECORDS }) {
      incrementCounter(counter, 0);
    }
  }

  @Override
  public int prepare(String... arguments) throws Exception {

    if (arguments == null || arguments.length != 3) {
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
    if (!hdfs.exists(hdfsPartitionedPath)
        || !HDFSClientUtil.canDoAction(hdfs, UserGroupInformation
            .getCurrentUser().getUserName(), UserGroupInformation
            .getCurrentUser().getGroupNames(), hdfsPartitionedPath,
            FsAction.READ)) {
      throw new Exception("HDFS partitioned directory [" + hdfsPartitionedPath
          + "] not available to user ["
          + UserGroupInformation.getCurrentUser().getUserName() + "]");
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS partitioned directory [" + hdfsPartitionedPath
          + "] validated");
    }

    hdfsCleansedPath = new Path(arguments[2]);
    if (hdfs.exists(hdfsCleansedPath)) {
      if (!hdfs.isDirectory(hdfsCleansedPath)) {
        throw new Exception("HDFS cleansed directory [" + hdfsCleansedPath
            + "] is not a directory");
      }
      if (!HDFSClientUtil.canDoAction(hdfs, UserGroupInformation
          .getCurrentUser().getUserName(), UserGroupInformation
          .getCurrentUser().getGroupNames(), hdfsCleansedPath, FsAction.ALL)) {
        throw new Exception("HDFS cleansed directory [" + hdfsCleansedPath
            + "] has too restrictive permissions to read/write as user ["
            + UserGroupInformation.getCurrentUser().getUserName() + "]");
      }
    } else {
      hdfs.mkdirs(hdfsCleansedPath, new FsPermission(FsAction.ALL,
          FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS cleansed directory [" + hdfsCleansedPath + "] validated");
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
    for (Path pathStaged : HDFSClientUtil.listFiles(hdfs, hdfsStagedPath, true)) {
      if (!PartitionFlag.isValue(pathStaged.getName())) {
        PartitionKey partitionKey = new PartitionKey().path(pathStaged
            .toString());
        if (PartitionFlag.list(hdfs, pathStaged, PartitionFlag._CLENSE)) {
          if (!partitionKeys.containsKey(partitionKey.getBatch())) {
            partitionKeys.put(partitionKey.getPartition(), partitionKey);
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
                .append(hdfsPartitionedPath)
                .append('/')
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
      FileOutputFormat.setOutputPath(job, hdfsCleansedPath);
      SequenceFileOutputFormat.setOutputCompressionType(job,
          CompressionType.NONE);
      FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
      MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT_SEQUENCE,
          SequenceFileOutputFormat.class, PartitionKey.class, Text.class);
      job.setNumReduceTasks(partitionKeys.size());
      job.setJarByClass(ClenseDriver.class);
      jobSuccess = job.waitForCompletion(log.isInfoEnabled());
      if (job != null) {
        importCounters(ClenseDriver.class.getCanonicalName(), job,
            new Counter[] { Counter.RECORDS });
      }
    }

    // for (String partitionKeyBatch : partitionKeys.keySet()) {
    // for (PartitionKey partitionKey : PartitionKey.getKeys(partitionKeyBatch))
    // {
    // Path pathStaged = new Path(new StringBuffer(
    // PartitionKey.PATH_NOMINAL_LENGTH).append(hdfsStagedPath)
    // .append('/').append(Counter.BATCHES_SUCCESSFUL.getPath())
    // .append(partitionKey.getPathBatch()).toString());
    // Path pathPartitioned = new Path(new StringBuffer(
    // PartitionKey.PATH_NOMINAL_LENGTH)
    // .append(hdfsPartitionedPath)
    // .append(
    // partitionKey.type(NAMED_OUTPUT_SEQUENCE)
    // .codec(MapReduceUtil.getCodecString(getConf()))
    // .getPathPartition()).toString());
    // boolean partitioned = HDFSClientUtil.listFiles(hdfs, pathPartitioned,
    // false).size() > 0;
    // PartitionFlag.update(hdfs, pathStaged,
    // partitioned ? PartitionFlag._CLENSE : PartitionFlag._FAILED);
    // incramentCounter(partitioned ? Counter.BATCHES_SUCCESSFUL
    // : Counter.BATCHES_FAILED, 1, partitionKey.getPartition() + '/'
    // + partitionKey.getBatch(), counterBatches);
    // incramentCounter(partitioned ? Counter.PARTITIONS_SUCCESSFUL
    // : Counter.PARTITIONS_FAILED, 1, partitionKey.getPartition(),
    // counterPartitions);
    // }
    // }
    // incramentCounter(Counter.BATCHES, counterBatches.size());
    // incramentCounter(Counter.PARTITIONS, counterPartitions.size());

    return jobSuccess ? RETURN_SUCCESS : RETURN_FAILURE_RUNTIME;

  }

  @Override
  public int cleanup() throws IOException {
    return RETURN_SUCCESS;
  }

  public static void main(String... arguments) throws Exception {
    System.exit(new ClenseDriver().runner(arguments));
  }

}
