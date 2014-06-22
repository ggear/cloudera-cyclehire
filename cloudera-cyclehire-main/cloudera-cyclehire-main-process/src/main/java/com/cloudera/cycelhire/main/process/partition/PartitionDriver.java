package com.cloudera.cycelhire.main.process.partition;

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

import com.cloudera.cycelhire.main.common.Counter;
import com.cloudera.cycelhire.main.common.Driver;
import com.cloudera.cycelhire.main.common.model.PartitionFlag;
import com.cloudera.cycelhire.main.common.model.PartitionKey;
import com.cloudera.cyclehire.main.common.hdfs.HDFSClientUtil;
import com.cloudera.cyclehire.main.common.mapreduce.MapReduceUtil;

public class PartitionDriver extends Driver {

  public static final String NAMED_OUTPUT = "sequence";

  private static final Logger log = LoggerFactory
      .getLogger(PartitionDriver.class);

  private Path hdfsStagingPath;
  private Path hdfsPartitioningPath;

  public PartitionDriver() {
    super();
  }

  public PartitionDriver(Configuration confguration) {
    super(confguration);
  }

  @Override
  public String description() {
    return "Partition the records for a set of files";
  }

  @Override
  public String[] options() {
    return new String[] {};
  }

  @Override
  public String[] paramaters() {
    return new String[] { "hdfs-dir-staging", "hdfs-dir-partitioning" };
  }

  @Override
  public void reset() {
    super.reset();
    for (Counter counter : new Counter[] { Counter.BATCHES_SKIPPED,
        Counter.BATCHES_FAILED, Counter.BATCHES_SUCCESSFUL, Counter.BATCHES,
        Counter.PARTITIONS_SKIPPED, Counter.PARTITIONS_FAILED,
        Counter.PARTITIONS_SUCCESSFUL, Counter.PARTITIONS, Counter.RECORDS }) {
      incramentCounter(PartitionDriver.class.getCanonicalName(), counter, 0);
    }
  }

  @Override
  public int prepare(String... arguments) throws Exception {

    if (arguments == null || arguments.length != 2) {
      throw new Exception("Invalid number of arguments");
    }

    FileSystem hdfs = FileSystem.newInstance(getConf());

    hdfsStagingPath = new Path(arguments[0]);
    if (!hdfs.exists(hdfsStagingPath)
        || !HDFSClientUtil.canDoAction(hdfs, UserGroupInformation
            .getCurrentUser().getUserName(), UserGroupInformation
            .getCurrentUser().getGroupNames(), hdfsStagingPath, FsAction.READ)) {
      throw new Exception("HDFS staging directory [" + hdfsStagingPath
          + "] not available to user ["
          + UserGroupInformation.getCurrentUser().getUserName() + "]");
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS staging directory [" + hdfsStagingPath + "] validated");
    }

    hdfsPartitioningPath = new Path(arguments[1]);
    if (hdfs.exists(hdfsPartitioningPath)) {
      if (!hdfs.isDirectory(hdfsPartitioningPath)) {
        throw new Exception("HDFS partition directory [" + hdfsPartitioningPath
            + "] is not a directory");
      }
      if (!HDFSClientUtil
          .canDoAction(hdfs, UserGroupInformation.getCurrentUser()
              .getUserName(), UserGroupInformation.getCurrentUser()
              .getGroupNames(), hdfsPartitioningPath, FsAction.ALL)) {
        throw new Exception("HDFS partition directory [" + hdfsPartitioningPath
            + "] has too restrictive permissions to read/write as user ["
            + UserGroupInformation.getCurrentUser().getUserName() + "]");
      }
    } else {
      hdfs.mkdirs(hdfsPartitioningPath, new FsPermission(FsAction.ALL,
          FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS partitioning directory [" + hdfsPartitioningPath
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
    Map<Path, PartitionKey> pathStagingsPartitioning = new HashMap<Path, PartitionKey>();
    for (Path pathStaging : HDFSClientUtil.listFiles(hdfs, hdfsStagingPath,
        true)) {
      if (!PartitionFlag.isValue(pathStaging.getName())) {
        PartitionKey partitionKey = new PartitionKey().path(pathStaging
            .toString());
        if (PartitionFlag.list(hdfs, pathStaging, PartitionFlag._PARTITION)) {
          if (!pathStagingsPartitioning.containsKey(pathStaging.getParent())) {
            if (partitionKey.isValid()) {
              pathStagingsPartitioning.put(pathStaging.getParent(),
                  partitionKey);
            }
          }
          incramentCounter(PartitionDriver.class.getCanonicalName(),
              Counter.BATCHES, 0, pathStaging.getParent().toString(),
              counterBatches);
          incramentCounter(PartitionDriver.class.getCanonicalName(),
              Counter.PARTITIONS, 0, partitionKey.getPartition(),
              counterPartitions);
        } else {
          incramentCounter(PartitionDriver.class.getCanonicalName(),
              Counter.BATCHES_SKIPPED, 1, pathStaging.getParent().toString(),
              counterBatches);
          incramentCounter(PartitionDriver.class.getCanonicalName(),
              Counter.PARTITIONS_SKIPPED, 1, partitionKey.getPartition(),
              counterPartitions);
        }
      }
    }
    incramentCounter(PartitionDriver.class.getCanonicalName(), Counter.BATCHES,
        counterBatches.size());
    incramentCounter(PartitionDriver.class.getCanonicalName(),
        Counter.PARTITIONS, counterPartitions.size());

    Job job = null;
    boolean jobSuccess = false;
    if (!pathStagingsPartitioning.isEmpty()) {
      job = Job.getInstance(getConf());
      job.setJobName(getClass().getSimpleName());
      job.getConfiguration().set(
          FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER,
          Boolean.FALSE.toString());
      for (Path pathStaging : pathStagingsPartitioning.keySet()) {
        if (PartitionInputFormat.supports(
            pathStagingsPartitioning.get(pathStaging).getType(),
            pathStagingsPartitioning.get(pathStaging).getCodec())) {
          MultipleInputs.addInputPath(job, pathStaging, PartitionInputFormat
              .get(pathStagingsPartitioning.get(pathStaging).getType(),
                  pathStagingsPartitioning.get(pathStaging).getCodec()));
        }
      }
      job.setMapOutputKeyClass(PartitionKey.class);
      job.setMapOutputValueClass(Text.class);
      job.setMapperClass(PartitionMapper.class);
      job.setNumReduceTasks(0);
      LazyOutputFormatNoCheck.setOutputFormatClass(job, TextOutputFormat.class);
      FileOutputFormat.setOutputPath(job, hdfsPartitioningPath);
      SequenceFileOutputFormat.setOutputCompressionType(job,
          CompressionType.NONE);
      FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
      MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT,
          SequenceFileOutputFormat.class, PartitionKey.class, Text.class);
      job.setJarByClass(PartitionDriver.class);
      jobSuccess = job.waitForCompletion(log.isInfoEnabled());
      if (job != null) {
        importCounters(PartitionDriver.class.getCanonicalName(), job,
            new Counter[] { Counter.RECORDS });
      }
    }

    counterBatches.clear();
    counterPartitions.clear();
    for (Path pathStaging : pathStagingsPartitioning.keySet()) {
      PartitionKey partitionKey = pathStagingsPartitioning.get(pathStaging);
      String pathPartionedPrefix = new StringBuffer(512)
          .append(hdfsPartitioningPath).append('/').append(NAMED_OUTPUT)
          .append('/').append(MapReduceUtil.getCodecString(getConf()))
          .append('/').toString();
      for (String partition : partitionKey.getPartitions()) {
        Path pathPartitioned = new Path(new StringBuffer(512)
            .append(pathPartionedPrefix).append(partition).append('/')
            .append(partitionKey.getBatch()).toString());
        boolean partitioned = HDFSClientUtil.listFiles(hdfs, pathPartitioned,
            false).size() > 0;
        PartitionFlag.update(hdfs, new Path(pathStaging.getParent(),
            partitionKey.getBatch()), partitioned ? PartitionFlag._PROCESS
            : PartitionFlag._FAILED);
        incramentCounter(PartitionDriver.class.getCanonicalName(),
            partitioned ? Counter.BATCHES_SUCCESSFUL : Counter.BATCHES_FAILED,
            1, pathPartitioned.toString(), counterBatches);
        incramentCounter(PartitionDriver.class.getCanonicalName(),
            partitioned ? Counter.PARTITIONS_SUCCESSFUL
                : Counter.PARTITIONS_FAILED, 1, partitionKey.getPartition(),
            counterPartitions);
      }
    }

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
