package com.cloudera.cyclehire.main.process.cleanse;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.mapreduce.MapReduceUtil;
import com.cloudera.cyclehire.main.common.model.PartitionKey;
import com.cloudera.cyclehire.main.common.model.PartitionRecord;
import com.cloudera.cyclehire.main.process.partition.PartitionDriver;

public class ClenseReducer extends
    Reducer<PartitionKey, Text, PartitionKey, Text> {

  public static final int RECORD_BUFFER_SIZE_DATA = 500000;
  public static final int RECORD_BUFFER_SIZE_METADATA = 256;

  private static final String PATH_PREFIX = "./";

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
  protected void reduce(PartitionKey key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    PartitionRecord recordValue = null;
    for (Text value : values) {
      Counter counter = null;
      String valueString = value.toString();
      PartitionRecord record = new PartitionRecord().key(key).xml(
          valueString.substring(0,
              valueString.indexOf(MapReduceUtil.RECORD_COLUMN_DELIM)));
      if (recordValue == null && record.isValid()) {
        StringBuilder valueStringBuilder = new StringBuilder(
            RECORD_BUFFER_SIZE_DATA);
        for (List<String> valueStringLists : (recordValue = record).getTable()) {
          valueStringBuilder.append(key.getEpochGet())
              .append(MapReduceUtil.RECORD_COLUMN_DELIM)
              .append(key.getEpochUpdate());
          for (String valueStringList : valueStringLists) {
            valueStringBuilder.append(MapReduceUtil.RECORD_COLUMN_DELIM)
                .append(valueStringList);
          }
        }
        value = new Text(valueStringBuilder.toString());
        context.getCounter(counter = Counter.RECORDS_CLEANSED).increment(1);
      } else {
        context.getCounter(
            counter = record.isValid() ? Counter.RECORDS_DUPLICATE
                : Counter.RECORDS_MALFORMED).increment(1);
      }
      multipleOutputs.write(
          CleanseDriver.NAMED_OUTPUT_SEQUENCE,
          key,
          value,
          new StringBuilder(RECORD_BUFFER_SIZE_METADATA)
              .append(PATH_PREFIX)
              .append(counter.getPath())
              .append(
                  key.type(PartitionDriver.OUTPUT_FORMAT)
                      .codec(
                          MapReduceUtil.getCodecString(context
                              .getConfiguration())).getPathPartition())
              .append('/').append(PartitionKey.TOKEN_NAME).toString());
      context.getCounter(Counter.RECORDS).increment(1);
    }
  }

}
