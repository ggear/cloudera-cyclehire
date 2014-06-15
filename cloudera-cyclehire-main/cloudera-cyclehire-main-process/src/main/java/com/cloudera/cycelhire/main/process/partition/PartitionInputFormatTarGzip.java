package com.cloudera.cycelhire.main.process.partition;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.regex.Matcher;

import org.apache.commons.compress.archivers.ArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import com.cloudera.cycelhire.main.common.model.PartitionKey;

public class PartitionInputFormatTarGzip extends FileInputFormat<PartitionKey, Text> {

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<PartitionKey, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException, InterruptedException {
    return new RecordReader<PartitionKey, Text>() {

      private Path path;
      private Matcher name;
      private ArchiveEntry entry;
      private TarArchiveInputStream stream;

      @Override
      public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        path = ((FileSplit) split).getPath();
        stream = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(path.getFileSystem(
            context.getConfiguration()).open(path))));
      }

      @Override
      public boolean nextKeyValue() throws IOException {
        return (entry = stream.getNextTarEntry()) != null ? entry.isDirectory()
            || !(name = PartitionKey.REGEX_RECORD.matcher(entry.getName())).matches() ? nextKeyValue() : true : false;
      }

      @Override
      public PartitionKey getCurrentKey() throws IOException, InterruptedException {
        try {
          return new PartitionKey().record(path.getName()).epochGet(1000L * Long.parseLong(name.group(1))).build();
        } catch (Exception e) {
          // TODO Auto-generated catch block
          throw new RuntimeException();
        }
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
