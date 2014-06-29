package com.cloudera.cycelhire.main.process.clense;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.cloudera.cycelhire.main.common.model.PartitionKey;

public class ClenseReducerPartitioner extends Partitioner<PartitionKey, Text> {

  @Override
  public int getPartition(PartitionKey key, Text value, int numPartitions) {
    return (int) ((key.getEpochUpdate().hashCode() & 0x00000000ffffffffL) % numPartitions);
  }

}
