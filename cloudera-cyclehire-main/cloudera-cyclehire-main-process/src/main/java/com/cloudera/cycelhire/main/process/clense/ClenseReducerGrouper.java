package com.cloudera.cycelhire.main.process.clense;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import com.cloudera.cycelhire.main.common.model.PartitionKey;

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
