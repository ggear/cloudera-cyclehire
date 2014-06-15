package com.cloudera.cycelhire.main.common.model;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public enum PartitionFlag {

  _SUCCESS, _PARTITION, _PROCESS, _FAILED, _LOCKED;

  public static boolean isValue(String value) {
    if (value != null) {
      for (PartitionFlag partitionFlag : PartitionFlag.values()) {
        if (partitionFlag.toString().equals(value)) {
          return true;
        }
      }
    }
    return false;
  }

  public static boolean isValue(FileSystem hdfs, Path path, PartitionFlag partitionFlag) throws IOException {
    FileStatus pathStatus = hdfs.getFileStatus(path);
    if (pathStatus != null && !isValue(path.getName())) {
      if (!pathStatus.isDirectory()) {
        pathStatus = hdfs.getFileStatus(path.getParent());
      }
      if (hdfs.exists(new Path(pathStatus.getPath(), partitionFlag.toString()))) {
        return true;
      }
    }
    return false;
  }

  public static PartitionFlag valueOf(FileSystem hdfs, Path path) throws IOException {
    try {
      FileStatus pathStatus = hdfs.getFileStatus(path);
      if (pathStatus != null && !isValue(path.getName())) {
        if (!pathStatus.isDirectory()) {
          pathStatus = hdfs.getFileStatus(path.getParent());
        }
        for (PartitionFlag partitionFlag : PartitionFlag.values()) {
          if (hdfs.exists(new Path(pathStatus.getPath(), partitionFlag.toString()))) {
            return partitionFlag;
          }
        }
      }
    } catch (FileNotFoundException exception) {
      // ignore exception, bad to use exceptions for flow control but this is what
      // FileSystem.exists does in any case and avoid an extra round trip
    }
    return null;
  }

}
