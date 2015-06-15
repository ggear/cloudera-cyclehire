package com.cloudera.cyclehire.main.common;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MrUtil {

  public static final String RECORD_COLUMN_DELIM = "\u0001";
  public static final String RECORD_COLUMN_DELIM_ESCAPED = "\\u0001";

  public static final String CODEC_NONE = "none";

  public static String getCodecString(Configuration configuration) {
    boolean compress = configuration.getBoolean(FileOutputFormat.COMPRESS_TYPE,
        false);
    if (compress) {
      String codecType = configuration
          .get(FileOutputFormat.COMPRESS_TYPE, null);
      Class<?> codecClass = configuration.getClass(
          FileOutputFormat.COMPRESS_CODEC,
          codecType == null
              || !codecType.equals(CompressionType.NONE.toString()) ? null
              : DefaultCodec.class);
      if (codecClass == null) {
        return CODEC_NONE;
      } else {
        try {
          return ((CompressionCodec) codecClass.newInstance())
              .getDefaultExtension().replace(".", "");
        } catch (Exception exception) {
          throw new RuntimeException("Could not determine codec", exception);
        }
      }
    }
    return CODEC_NONE;
  }

}
