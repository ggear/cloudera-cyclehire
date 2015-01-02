package com.cloudera.cyclehire.main.process.cleanse;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.mapreduce.MapReduceUtil;
import com.cloudera.cyclehire.main.common.model.PartitionKey;
import com.cloudera.cyclehire.main.common.model.PartitionRecord;
import com.cloudera.cyclehire.main.process.partition.PartitionDriver;

public class ClenseReducer extends
    Reducer<PartitionKey, Text, NullWritable, Text> {

  public static final int RECORD_BUFFER_SIZE_DATA = 1024;
  public static final int RECORD_BUFFER_SIZE_METADATA = 256;

  private static final String PATH_PREFIX = "./";

  private MultipleOutputs<NullWritable, Text> multipleOutputs;

  @Override
  public void setup(Context context) {
    multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    multipleOutputs.close();
  }

  @Override
  protected void reduce(PartitionKey key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    boolean isDuplicate = false;
    String valueTimestamps = new StringBuilder().append(key.getEpochGet())
        .append(MapReduceUtil.RECORD_COLUMN_DELIM).append(key.getEpochUpdate())
        .toString();
    for (Text value : values) {
      Counter counter = null;
      String valueString = value.toString();
      PartitionRecord record = new PartitionRecord().key(key).xml(
          valueString.substring(0,
              valueString.indexOf(MapReduceUtil.RECORD_COLUMN_DELIM)));
      for (List<String> valueStringLists : record.getTable()) {
        StringBuilder valueStringBuilder = new StringBuilder(
            RECORD_BUFFER_SIZE_DATA).append(valueTimestamps);
        for (String valueStringList : valueStringLists) {
          valueStringBuilder.append(MapReduceUtil.RECORD_COLUMN_DELIM).append(
              valueStringList);
        }
        valueStringBuilder.append(MapReduceUtil.RECORD_COLUMN_DELIM);
        if (record.isValid()) {
          valueStringBuilder.append("");
          if (!isDuplicate) {
            counter = Counter.RECORDS_CLEANSED;
          } else {
            counter = Counter.RECORDS_DUPLICATE;
          }
        } else {
          valueStringBuilder.append(record.getXml());
          counter = Counter.RECORDS_MALFORMED;
        }
        value = new Text(valueStringBuilder.toString());
        multipleOutputs.write(
            CleanseDriver.NAMED_OUTPUT_SEQUENCE,
            NullWritable.get(),
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
      }
      isDuplicate = isDuplicate || record.isValid();
      context.getCounter(counter).increment(1);
      context.getCounter(Counter.RECORDS).increment(1);
    }
  }

}
