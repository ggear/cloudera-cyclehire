package com.cloudera.cycelhire.main.process.partition;

import java.io.BufferedInputStream;
import java.io.IOException;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.cloudera.cycelhire.main.common.model.PartitionKey;

public class PartitionInputFormatTarGzip extends
    FileInputFormat<PartitionKey, Text> {

  private static final Log LOG = LogFactory
      .getLog(PartitionInputFormatTarGzip.class);

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<PartitionKey, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException, InterruptedException {
    return new RecordReader<PartitionKey, Text>() {

      private Path path;
      private ArchiveEntry entry;
      private TarArchiveInputStream stream;
      private PartitionKey partitionKey;

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context)
          throws InterruptedException {
        path = ((FileSplit) split).getPath();
        try {
          stream = new TarArchiveInputStream(new GzipCompressorInputStream(
              new BufferedInputStream(path.getFileSystem(
                  context.getConfiguration()).open(path))));
        } catch (IOException exception) {
          if (LOG.isErrorEnabled()) {
            LOG.error("Could not read file [" + path + "]", exception);
          }
        }
      }

      @Override
      public boolean nextKeyValue() throws IOException {
        return stream == null ? false
            : (entry = stream.getNextTarEntry()) != null ? entry.isDirectory()
                || !(partitionKey = new PartitionKey().batch(
                    path.getParent().getName()).record(entry.getName()))
                    .isValid() ? nextKeyValue() : true : false;
      }

      @Override
      public PartitionKey getCurrentKey() throws IOException,
          InterruptedException {
        return partitionKey;
      }

      @Override
      public Text getCurrentValue() throws IOException, InterruptedException {
        return new Text(IOUtils.toString(stream));
      }

      @Override
      public float getProgress() throws IOException, InterruptedException {
        return entry == null ? 1 : 0;
      }

      @Override
      public void close() throws IOException {
        if (stream != null) {
          stream.close();
        }
      }

    };
  }
}
