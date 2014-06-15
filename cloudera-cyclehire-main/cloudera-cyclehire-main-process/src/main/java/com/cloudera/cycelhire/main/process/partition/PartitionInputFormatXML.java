package com.cloudera.cycelhire.main.process.partition;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import com.cloudera.cycelhire.main.common.model.PartitionKey;

public class PartitionInputFormatXML extends CombineFileInputFormat<PartitionKey, Text> {

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public RecordReader<PartitionKey, Text> createRecordReader(InputSplit split, TaskAttemptContext context)
      throws IOException {
    return new CombineFileRecordReader<PartitionKey, Text>((CombineFileSplit) split, context,
        PartitionRecordReaderXML.class);
  }

  class PartitionRecordReaderXML extends RecordReader<PartitionKey, Text> {

    public PartitionRecordReaderXML(CombineFileSplit split, TaskAttemptContext context, Integer index)
        throws IOException {
      super();
      // TODO Auto-generated constructor stub
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
      // TODO Auto-generated method stub

    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      return false;
    }

    @Override
    public PartitionKey getCurrentKey() throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public Text getCurrentValue() throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      return null;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
      // TODO Auto-generated method stub
      return 0;
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub

    }

  }

}
