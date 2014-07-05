package com.cloudera.cyclehire.main.common.model;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public enum PartitionFlag {

  _SUCCESS, _PARTITION, _CLENSE, _FAILED, _UNKNOWN, _LOCKED;

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

  public static boolean list(FileSystem hdfs, Path path,
      PartitionFlag partitionFlag) throws IOException {
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

  public static List<PartitionFlag> list(FileSystem hdfs, Path path)
      throws IOException {
    List<PartitionFlag> partitionFlags = new ArrayList<PartitionFlag>();
    try {
      FileStatus pathStatus = hdfs.getFileStatus(path);
      if (pathStatus != null && !isValue(path.getName())) {
        if (!pathStatus.isDirectory()) {
          pathStatus = hdfs.getFileStatus(path.getParent());
        }
        for (PartitionFlag partitionFlag : PartitionFlag.values()) {
          if (hdfs.exists(new Path(pathStatus.getPath(), partitionFlag
              .toString()))) {
            partitionFlags.add(partitionFlag);
          }
        }
      }
    } catch (FileNotFoundException exception) {
      // ignore exception, bad to use exceptions for flow control but this is
      // what FileSystem.exists() does, so OK to avoid an extra round trip
    }
    return partitionFlags;
  }

  public static boolean update(FileSystem hdfs, Path path,
      PartitionFlag partitionFlag) throws IOException {
    FileStatus status = null;
    boolean updated = false;
    boolean isDirectory = true;
    try {
      status = hdfs.getFileStatus(path);
      isDirectory = status.isDirectory();
      for (PartitionFlag partitionFlagToDelete : list(hdfs, path)) {
        if (!partitionFlagToDelete.equals(partitionFlag)) {
          hdfs.delete(new Path(hdfs.getFileStatus(path).isDirectory() ? path
              : path.getParent(), partitionFlagToDelete.toString()), false);
        } else
          updated = true;
      }
    } catch (FileNotFoundException exception) {
      hdfs.mkdirs(path);
    }
    return updated
        || hdfs.createNewFile(new Path(isDirectory ? path : path.getParent(),
            partitionFlag.toString()));
  }

}
