package com.cloudera.cyclehire.main.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Iterator;
import java.util.StringTokenizer;

import junit.framework.Assert;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Utils;
import org.apache.hadoop.mapred.WordCount;
import org.junit.Test;

public class EmbeddedCoreTestCaseTest extends EmbeddedCoreTestCase {

  public EmbeddedCoreTestCaseTest() throws IOException {
    super();
  }

  @Test
  public void testFileSystem() throws IOException {
    String someDir = BaseTestCase.getPathHDFS("/some_dir");
    Assert.assertTrue(FileSystem.get(getFileSystem().getConf()).mkdirs(new Path(someDir)));
    Assert.assertTrue(new File(BaseTestCase.getPathLocal(someDir)).exists());
  }

  // TODO: Convert to org.apache.mapreduce client
  @Test
  public void testMapReduce() throws IOException {

    Path dirInput = new Path(BaseTestCase.getPathHDFS("/tmp/wordcount/input"));
    Path dirOutput = new Path(BaseTestCase.getPathHDFS("/tmp/wordcount/output"));

    Path hdfsFile = new Path(dirInput, "file1.txt");
    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(this.getFileSystem().create(hdfsFile)));
    writer.write("a a a a a\n");
    writer.write("b b\n");
    writer.close();

    JobConf conf = new JobConf(getFileSystem().getConf(), WordCount.class);
    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setMapperClass(MapClass.class);
    conf.setCombinerClass(Reduce.class);
    conf.setReducerClass(Reduce.class);

    FileInputFormat.setInputPaths(conf, dirInput);
    FileOutputFormat.setOutputPath(conf, dirOutput);

    Assert.assertTrue(JobClient.runJob(conf).isSuccessful());

    Path[] outputFiles = FileUtil.stat2Paths(getFileSystem().listStatus(dirOutput,
        new Utils.OutputFileUtils.OutputFilesFilter()));

    Assert.assertEquals(1, outputFiles.length);

    InputStream in = getFileSystem().open(outputFiles[0]);
    BufferedReader reader = new BufferedReader(new InputStreamReader(in));
    Assert.assertEquals("a\t5", reader.readLine());
    Assert.assertEquals("b\t2", reader.readLine());
    assertNull(reader.readLine());
    reader.close();
  }

  public static class MapClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
        throws IOException {
      String line = value.toString();
      StringTokenizer itr = new StringTokenizer(line);
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        output.collect(word, one);
      }
    }
  }

  public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
        Reporter reporter) throws IOException {
      int sum = 0;
      while (values.hasNext()) {
        sum += values.next().get();
      }
      output.collect(key, new IntWritable(sum));
    }
  }

}
