package com.cloudera.cyclehire.main.process.cleanse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import com.cloudera.cyclehire.main.common.model.PartitionKey;

public class ClenseReducerPartitioner extends Partitioner<PartitionKey, Text> implements Configurable {

  public static final String PARTITIONS = "mapreduce.clense.reducer.partitioner.partitions";

  private Configuration configuration;
  private Map<String, Integer> partitionsIndex;

  @Override
  public void setConf(Configuration configuration) {
    this.configuration = configuration;
    this.partitionsIndex = new HashMap<String, Integer>();
    int index = 1;
    for (String partition : getPartitions(configuration)) {
      partitionsIndex.put(partition, index++);
    }
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  public static void setPartitions(Configuration configuration, Set<String> partitions) {
    configuration.set(PARTITIONS, StringUtils.join(partitions, ','));
  }

  public static Set<String> getPartitions(Configuration configuration) {
    return new HashSet<String>(Arrays.asList(configuration.getStrings(PARTITIONS, new String[0])));
  }

  @Override
  public int getPartition(PartitionKey key, Text value, int numPartitions) {
    return (partitionsIndex.containsKey(key.getPartition()) ? partitionsIndex.get(key.getPartition()) : key
        .getPartition().hashCode() & Integer.MAX_VALUE)
        % numPartitions;
  }
}
