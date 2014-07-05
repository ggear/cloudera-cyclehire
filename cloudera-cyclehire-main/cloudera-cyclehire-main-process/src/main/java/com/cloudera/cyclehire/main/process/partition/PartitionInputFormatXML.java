package com.cloudera.cyclehire.main.process.partition;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.cloudera.cyclehire.main.common.model.PartitionKey;

public class PartitionInputFormatXML extends
    CombineFileInputFormat<PartitionKey, Text> {

  private static final Log LOG = LogFactory
      .getLog(PartitionInputFormatTarGzip.class);

  public PartitionInputFormatXML() {
    super();
    setMaxSplitSize(128 * 1024 * 1024);
  }

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<PartitionKey, Text> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new CombineFileRecordReader<PartitionKey, Text>(
        (CombineFileSplit) split, context, PartitionRecordReaderXML.class);
  }

  public static class PartitionRecordReaderXML extends
      RecordReader<PartitionKey, Text> {

    private Path path;
    private Text value;
    private PartitionKey key;
    private InputStream stream;

    public PartitionRecordReaderXML(CombineFileSplit split,
        TaskAttemptContext context, Integer index) throws IOException {
      path = split.getPath(index);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
        throws IOException, InterruptedException {
      try {
        stream = new BufferedInputStream(path.getFileSystem(
            context.getConfiguration()).open(path));
      } catch (IOException exception) {
        if (LOG.isErrorEnabled()) {
          LOG.error("Could not read file [" + path + "]", exception);
        }
      }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      boolean exhausted = stream == null;
      if (!exhausted) {
        try {
          if (!(exhausted = !(key = new PartitionKey().path(path.toString()))
              .isValid())) {
            value = new Text(IOUtils.toString(stream));
          }
        } catch (IOException exception) {
          if (LOG.isErrorEnabled()) {
            LOG.error("Could not read file [" + path + "]", exception);
          }
        } finally {
          stream = null;
        }
      }
      return !exhausted;
    }

    @Override
    public PartitionKey getCurrentKey() throws IOException,
        InterruptedException {
      return key;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      return stream == null ? 1F : 0F;
    }

    @Override
    public void close() throws IOException {
      if (stream != null) {
        stream.close();
      }
    }

  }

}
