package com.cloudera.cycelhire.main.process.partition;

import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import com.cloudera.cycelhire.main.common.model.PartitionKey;
import com.google.common.collect.ImmutableMap;

public class PartitionInputFormat {

  private static Map<String, ImmutableMap<String, ? extends Class<? extends FileInputFormat<PartitionKey, Text>>>> INPUT_FORMATS = ImmutableMap
      .of("tar", ImmutableMap.of("gz", PartitionInputFormatTarGzip.class),
          "xml", ImmutableMap.of("none", PartitionInputFormatXML.class));

  public static boolean supports(String type, String codec) {
    return INPUT_FORMATS.containsKey(type)
        && INPUT_FORMATS.get(type).containsKey(codec);
  }

  public static Class<? extends FileInputFormat<PartitionKey, Text>> get(
      String type, String codec) {
    if (!supports(type, codec)) {
      throw new IllegalArgumentException("Do not know how to process [" + type
          + "] with codec [" + codec + "]");
    }
    return INPUT_FORMATS.get(type).get(codec);
  }

}
