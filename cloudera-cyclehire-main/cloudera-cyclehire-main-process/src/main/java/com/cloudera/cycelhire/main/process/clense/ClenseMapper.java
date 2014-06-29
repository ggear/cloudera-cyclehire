package com.cloudera.cycelhire.main.process.clense;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudera.cycelhire.main.common.model.PartitionKey;
import com.cloudera.cycelhire.main.common.model.PartitionValue;

public class ClenseMapper extends
    Mapper<PartitionKey, Text, PartitionKey, Text> {

  @Override
  protected void map(PartitionKey key, Text value, Context context)
      throws IOException, InterruptedException {
    context.write(new PartitionValue().key(key).epochUpdate(value.toString())
        .getKey(), value);
  }

}
