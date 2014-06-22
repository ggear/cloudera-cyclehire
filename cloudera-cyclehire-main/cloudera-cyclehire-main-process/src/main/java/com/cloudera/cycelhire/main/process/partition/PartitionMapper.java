package com.cloudera.cycelhire.main.process.partition;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.cloudera.cycelhire.main.common.Counter;
import com.cloudera.cycelhire.main.common.model.PartitionKey;
import com.cloudera.cyclehire.main.common.mapreduce.MapReduceUtil;

public class PartitionMapper extends
    Mapper<PartitionKey, Text, PartitionKey, Text> {

  private MultipleOutputs<PartitionKey, Text> multipleOutputs;

  @Override
  public void setup(Context context) {
    multipleOutputs = new MultipleOutputs<PartitionKey, Text>(context);
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    multipleOutputs.close();
  }

  @Override
  protected void map(PartitionKey key, Text value, Context context)
      throws IOException, InterruptedException {
    try {
      multipleOutputs.write(
          PartitionDriver.NAMED_OUTPUT,
          key,
          value,
          PartitionDriver.NAMED_OUTPUT + "/"
              + MapReduceUtil.getCodecString(context.getConfiguration()) + "/"
              + key.getPartition() + "/" + key.getBatch() + "/"
              + key.getBatch());
    } catch (IllegalArgumentException exception) {
      // necessary for MRUnit to work with MultipleOutputs
      context.write(key, value);
    }
    context.getCounter(Counter.RECORDS).increment(1);
  }
}
