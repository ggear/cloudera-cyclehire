package com.cloudera.cyclehire.main.process.cleanse;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.cloudera.cyclehire.main.common.model.PartitionKey;

public class ClenseReducerGrouper extends WritableComparator {

  protected ClenseReducerGrouper() {
    super(PartitionKey.class, true);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public int compare(WritableComparable one, WritableComparable two) {
    return ((PartitionKey) one).getEpochUpdate().compareTo(
        ((PartitionKey) two).getEpochUpdate());
  }

}
