package com.cloudera.cyclehire.main.common.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapReduceUtil {

  public static String getCodecString(Configuration configuration) {
    String codecType = configuration.get(FileOutputFormat.COMPRESS_TYPE, null);
    Class<?> codecClass = configuration
        .getClass(FileOutputFormat.COMPRESS_CODEC, codecType == null
            || !codecType.equals(CompressionType.NONE.toString()) ? null
            : DefaultCodec.class);
    if (codecClass == null) {
      return "none";
    } else {
      try {
        return ((CompressionCodec) codecClass.newInstance())
            .getDefaultExtension().replace(".", "");
      } catch (Exception exception) {
        throw new RuntimeException("Could not determine codec", exception);
      }
    }
  }

}
