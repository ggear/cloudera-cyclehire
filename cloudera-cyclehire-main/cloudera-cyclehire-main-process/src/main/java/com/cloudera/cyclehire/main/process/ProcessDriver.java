package com.cloudera.cyclehire.main.process;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.Driver;
import com.cloudera.cyclehire.main.process.cleanse.CleanseDriver;
import com.cloudera.cyclehire.main.process.partition.PartitionDriver;
import com.cloudera.cyclehire.main.process.stage.StageDriver;

public class ProcessDriver extends Driver {

  private String hdfsLandedPath;
  private String hdfsStagedPath;
  private String hdfsPartitionedPath;
  private String hdfsCleansedPath;

  public ProcessDriver() {
    super();
  }

  public ProcessDriver(Configuration confguration) {
    super(confguration);
  }

  @Override
  public String description() {
    return "Process a set of files";
  }

  @Override
  public String[] options() {
    return new String[] {};
  }

  @Override
  public String[] paramaters() {
    return new String[] { "hdfs-dir-landed", "hdfs-dir-staged",
        "hdfs-dir-partitioned", "hdfs-dir-cleansed" };
  }

  @Override
  public void reset() {
    super.reset();
    for (Counter counter : StageDriver.COUNTERS) {
      incrementCounter(StageDriver.class.getCanonicalName(), counter, 0);
    }
    for (Counter counter : PartitionDriver.COUNTERS) {
      incrementCounter(PartitionDriver.class.getCanonicalName(), counter, 0);
    }
    for (Counter counter : CleanseDriver.COUNTERS) {
      incrementCounter(CleanseDriver.class.getCanonicalName(), counter, 0);
    }
  }

  @Override
  public int prepare(String... arguments) throws Exception {

    if (arguments == null || arguments.length != 4) {
      throw new Exception("Invalid number of arguments");
    }

    hdfsLandedPath = arguments[0];
    hdfsStagedPath = arguments[1];
    hdfsPartitionedPath = arguments[2];
    hdfsCleansedPath = arguments[3];

    return RETURN_SUCCESS;
  }

  @Override
  public int execute() throws InterruptedException, ExecutionException,
      IOException, ClassNotFoundException {

    int returnValue = RETURN_FAILURE_RUNTIME;
    Driver stageDriver = new StageDriver(getConf());
    Driver partitionDriver = new PartitionDriver(getConf());
    Driver cleanseDriver = new CleanseDriver(getConf());
    if ((returnValue = stageDriver.run(new String[] { hdfsLandedPath,
        hdfsStagedPath })) == RETURN_SUCCESS) {
      if ((returnValue = partitionDriver.run(new String[] { hdfsStagedPath,
          hdfsPartitionedPath })) == RETURN_SUCCESS) {
        returnValue = cleanseDriver.run(new String[] { hdfsStagedPath,
            hdfsPartitionedPath, hdfsCleansedPath });
      }
    }

    importCountersAll(stageDriver.getCounters());
    importCountersAll(partitionDriver.getCounters());
    importCountersAll(cleanseDriver.getCounters());

    return returnValue;

  }

  @Override
  public int cleanup() throws IOException {
    return RETURN_SUCCESS;
  }

  public static void main(String... arguments) throws Exception {
    System.exit(new ProcessDriver().runner(arguments));
  }

}
