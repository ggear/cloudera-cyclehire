package com.cloudera.cycelhire.main.process.clense;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import com.cloudera.cycelhire.main.common.Counter;
import com.cloudera.cycelhire.main.common.model.PartitionKey;

public class ClenseReducer extends
    Reducer<PartitionKey, Text, PartitionKey, Text> {

  @Override
  protected void reduce(PartitionKey key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    for (Text value : values) {
      context.getCounter(Counter.RECORDS).increment(1);
    }
  }

}
