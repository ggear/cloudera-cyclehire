package com.cloudera.cycelhire.main.process.partition;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.cycelhire.main.common.Counter;
import com.cloudera.cycelhire.main.common.Driver;
import com.cloudera.cycelhire.main.common.model.PartitionFlag;
import com.cloudera.cyclehire.main.common.hdfs.HDFSClientUtil;

public class PartitionDriver extends Driver {

  public static final String NAMED_OUTPUT = "sequence";

  private static final Logger log = LoggerFactory.getLogger(PartitionDriver.class);

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
    for (Counter counter : new Counter[] { Counter.PARTITIONS, Counter.RECORDS }) {
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

    hdfsPartitioningPath = new Path(arguments[1]);
    if (hdfs.exists(hdfsPartitioningPath)) {
      if (!hdfs.isDirectory(hdfsPartitioningPath)) {
        throw new Exception("HDFS partition directory [" + hdfsPartitioningPath + "] is not a directory");
      }
      if (!HDFSClientUtil.canDoAction(hdfs, UserGroupInformation.getCurrentUser().getUserName(), UserGroupInformation
          .getCurrentUser().getGroupNames(), hdfsPartitioningPath, FsAction.ALL)) {
        throw new Exception("HDFS partition directory [" + hdfsPartitioningPath
            + "] has too restrictive permissions to read/write as user ["
            + UserGroupInformation.getCurrentUser().getUserName() + "]");
      }
    } else {
      hdfs.mkdirs(hdfsPartitioningPath, new FsPermission(FsAction.ALL, FsAction.READ_EXECUTE, FsAction.READ_EXECUTE));
    }
    if (log.isInfoEnabled()) {
      log.info("HDFS partitioning directory [" + hdfsPartitioningPath + "] validated");
    }

    return RETURN_SUCCESS;
  }

  @Override
  public int execute() throws InterruptedException, ExecutionException, IOException, ClassNotFoundException {

    FileSystem hdfs = FileSystem.newInstance(getConf());
    for (Path pathPartitioning : HDFSClientUtil.listFiles(hdfs, hdfsStagingPath, true)) {
      if (!PartitionFlag.isValue(pathPartitioning.getName())
          && PartitionFlag.isValue(hdfs, pathPartitioning, PartitionFlag._PARTITION)) {
      }
    }

    Job job = null;
    boolean jobSuccess = false;

    // TODO
    // if (!hdfsLandingStagingPaths.isEmpty()) {
    // job = Job.getInstance(getConf());
    // job.setJobName(getClass().getSimpleName());
    // job.getConfiguration().set(FileOutputCommitter.SUCCESSFUL_JOB_OUTPUT_DIR_MARKER, Boolean.FALSE.toString());
    //
    // job.setInputFormatClass(PartitionInputFormatTarGzip.class);
    // FileInputFormat.setInputPaths(job,
    // hdfsLandingStagingPaths.keySet().toArray(new Path[hdfsLandingStagingPaths.size()]));
    //
    // MultipleInputs.addInputPath(job, null, PartitionInputFormatTarGzip.class);
    // MultipleInputs.addInputPath(job, null, PartitionInputFormatXML.class);
    //
    // job.setMapOutputKeyClass(PartitionKey.class);
    // job.setMapOutputValueClass(Text.class);
    // job.setMapperClass(PartitionMapper.class);
    // job.setNumReduceTasks(0);
    // LazyOutputFormatNoCheck.setOutputFormatClass(job, TextOutputFormat.class);
    // FileOutputFormat.setOutputPath(job, hdfsPartitioningPath);
    // SequenceFileOutputFormat.setOutputCompressionType(job, CompressionType.NONE);
    // FileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
    // MultipleOutputs.addNamedOutput(job, NAMED_OUTPUT, SequenceFileOutputFormat.class, PartitionKey.class,
    // Text.class);
    // job.setJarByClass(PartitionDriver.class);
    // jobSuccess = job.waitForCompletion(log.isInfoEnabled());
    // if (job != null) {
    // importCounters(PartitionDriver.class.getCanonicalName(), job, new Counter[] { Counter.RECORDS });
    // }
    // }

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
