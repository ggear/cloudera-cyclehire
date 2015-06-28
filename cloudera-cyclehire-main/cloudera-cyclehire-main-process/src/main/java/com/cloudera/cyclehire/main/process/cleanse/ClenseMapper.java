package com.cloudera.cyclehire.main.process.cleanse;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.cloudera.cyclehire.main.common.model.PartitionKey;
import com.cloudera.cyclehire.main.common.model.PartitionRecord;

public class ClenseMapper extends Mapper<PartitionKey, Text, PartitionKey, Text> {

  @Override
  protected void map(PartitionKey key, Text value, Context context) throws IOException, InterruptedException {
    context.write(new PartitionRecord().key(key).epochUpdate(value.toString()).getKey(), value);
  }

}
