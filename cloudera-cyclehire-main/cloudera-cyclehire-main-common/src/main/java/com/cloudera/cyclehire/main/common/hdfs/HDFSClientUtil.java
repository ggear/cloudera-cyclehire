package com.cloudera.cyclehire.main.common.hdfs;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.ParentNotDirectoryException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.security.AccessControlException;

public class HDFSClientUtil {

  public static boolean canDoAction(FileSystem hdfs, String user,
      String[] groups, Path path, FsAction action) throws IOException {
    FileStatus status = hdfs.getFileStatus(path);
    FsPermission permission = status.getPermission();
    if (permission.getOtherAction().implies(action)) {
      return true;
    }
    for (String group : groups) {
      if (group.equals(status.getGroup())
          && permission.getGroupAction().implies(action)) {
        return true;
      }
    }
    if (user.equals(status.getOwner())
        && permission.getUserAction().implies(action)) {
      return true;
    }
    return false;
  }

  public static boolean copyFromLocalFile(FileSystem hdfs, Path fileSource,
      Path fileDestination, boolean fileSuccessMarker, long fileSize,
      int bufferSizeBytes, int replicationFactor, long blockSizeBytes)
      throws IOException {
    Path filesuccess = fileSuccessMarker ? new Path(
        fileDestination.getParent(), FileOutputCommitter.SUCCEEDED_FILE_NAME)
        : null;
    if (!hdfs.exists(fileDestination)
        && (!fileSuccessMarker || !hdfs.exists(filesuccess))) {
      hdfs.mkdirs(fileDestination.getParent());
      IOUtils
          .copyBytes(new FileInputStream(new File(fileSource.toString())), hdfs
              .create(fileDestination, false, bufferSizeBytes,
                  (short) replicationFactor, blockSizeBytes), bufferSizeBytes,
              true);
      if (fileSuccessMarker) {
        hdfs.createNewFile(new Path(fileDestination.getParent(),
            FileOutputCommitter.SUCCEEDED_FILE_NAME));
      }
      return true;
    } else if (hdfs.exists(fileDestination)
        && hdfs.getFileStatus(fileDestination).getLen() == fileSize
        && (!fileSuccessMarker || hdfs.exists(filesuccess))) {
      return false;
    } else {
      throw new IOException("File [" + fileDestination
          + "] already exists, but is corrupt");
    }
  }

  public static boolean createSymlinkOrCopy(FileSystem hdfs, Path target,
      Path link) throws AccessControlException, FileAlreadyExistsException,
      FileNotFoundException, ParentNotDirectoryException, IOException {
    boolean isSymlinked = FileSystem.areSymlinksEnabled();
    if (isSymlinked) {
      try {
        hdfs.createSymlink(target, link, true);
      } catch (UnsupportedOperationException exception) {
        isSymlinked = false;
      }
    }
    if (!isSymlinked) {
      FileUtil.copy(hdfs, target, hdfs, link, false, hdfs.getConf());
    }
    return isSymlinked;
  }

  public static List<Path> listFiles(FileSystem hdfs, Path path, boolean recurse)
      throws FileNotFoundException, IOException {
    List<Path> files = new ArrayList<Path>();
    try {
      RemoteIterator<LocatedFileStatus> filesIterator = hdfs.listFiles(path,
          recurse);
      while (filesIterator.hasNext()) {
        files.add(filesIterator.next().getPath());
      }
    } catch (FileNotFoundException exception) {
      // ignore
    }
    return files;
  }

}
