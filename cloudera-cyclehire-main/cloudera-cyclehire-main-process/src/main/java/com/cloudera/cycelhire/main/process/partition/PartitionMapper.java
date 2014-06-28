package com.cloudera.cycelhire.main.process.partition;

import java.io.IOException;
import java.util.UUID;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.cloudera.cycelhire.main.common.Counter;
import com.cloudera.cycelhire.main.common.model.PartitionKey;
import com.cloudera.cyclehire.main.common.mapreduce.MapReduceUtil;

public class PartitionMapper extends
    Mapper<PartitionKey, Text, PartitionKey, Text> {

  public static final String RECORD_COLUMN_DELIM = "\u0001";
  public static final String RECORD_COLUMN_DELIM_ESCAPED = "\\u0001";

  public static final int RECORD_BUFFER_SIZE_DATA = 500000;
  public static final int RECORD_BUFFER_SIZE_METADATA = 256;

  private final String pathPrefix = ".";
  private final String pathSuffix = new StringBuilder().append('-')
      .append(UUID.randomUUID()).toString();

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
      // Control characters are not allowed "in spirit" in XML, so default java
      // implementations and a multiple scan/copy OK given a rare positive match
      // Also permit this slight modification (escaping) but leave source data
      // otherwise intact for full history
      if (value.find(RECORD_COLUMN_DELIM) != -1) {
        value.set(value.toString().replace(RECORD_COLUMN_DELIM,
            RECORD_COLUMN_DELIM_ESCAPED));
      }
      // 1-copy of O(RECORD_BUFFER_SIZE_DATA) sized value data, naive
      // implementations can easily result in 3-copy
      byte[] valueMetaData = new StringBuilder(RECORD_BUFFER_SIZE_METADATA)
          .append(RECORD_COLUMN_DELIM).append(key.getBatch())
          .append(RECORD_COLUMN_DELIM).append(key.getRecord()).toString()
          .getBytes("UTF-8");
      value.append(valueMetaData, 0, valueMetaData.length);
      multipleOutputs
          .write(
              PartitionDriver.NAMED_OUTPUT_SEQUENCE,
              key,
              value,
              new StringBuilder(RECORD_BUFFER_SIZE_METADATA)
                  .append(pathPrefix)
                  .append(
                      key.type(PartitionDriver.NAMED_OUTPUT_SEQUENCE)
                          .codec(
                              MapReduceUtil.getCodecString(context
                                  .getConfiguration())).getPathPartition())
                  .append('/').append(key.getBatch()).append(pathSuffix)
                  .toString());

    } catch (IllegalArgumentException exception) {
      // necessary for MRUnit to work with MultipleOutputs
      context.write(key, value);
    }
    context.getCounter(Counter.RECORDS).increment(1);
  }
}
