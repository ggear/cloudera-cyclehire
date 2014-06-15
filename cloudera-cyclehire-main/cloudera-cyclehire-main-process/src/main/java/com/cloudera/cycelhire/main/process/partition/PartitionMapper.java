package com.cloudera.cycelhire.main.process.partition;

import java.io.IOException;
import java.io.StringReader;
import java.util.Calendar;

import javax.xml.namespace.QName;
import javax.xml.stream.XMLInputFactory;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import com.cloudera.cycelhire.main.common.Counter;
import com.cloudera.cycelhire.main.common.model.PartitionKey;
import com.cloudera.cyclehire.main.common.mapreduce.MapReduceUtil;

public class PartitionMapper extends Mapper<PartitionKey, Text, PartitionKey, Text> {

  private MultipleOutputs<PartitionKey, Text> multipleOutputs;

  @Override
  public void setup(Context context) {
    multipleOutputs = new MultipleOutputs<PartitionKey, Text>(context);
  }

  @Override
  public void cleanup(Context context) throws IOException, InterruptedException {
    multipleOutputs.close();
  }

  @Override
  protected void map(PartitionKey key, Text value, Context context) throws IOException, InterruptedException {
    context.getCounter(Counter.RECORDS).increment(1);
    long lastUpdate = key.getEpochGet();
    try {
      if (value.getLength() > 0) {
        lastUpdate = Long.parseLong(XMLInputFactory.newInstance()
            .createXMLEventReader(new StringReader(value.toString())).nextTag().asStartElement()
            .getAttributeByName(new QName("lastUpdate")).getValue());
      }
    } catch (Exception exception) {
    }
    Calendar outputCalendar = Calendar.getInstance();
    outputCalendar.setTimeInMillis(lastUpdate);
    PartitionKey outputKey = new PartitionKey().record(key.getRecord()).epochGet(key.getEpochGet())
        .epochUpdate(lastUpdate);
    Text outputValue = new Text(value);
    try {
      multipleOutputs.write(PartitionDriver.NAMED_OUTPUT, outputKey, outputValue, PartitionDriver.NAMED_OUTPUT + "/"
          + MapReduceUtil.getCodecString(context.getConfiguration()) + "/" + outputKey.getRecord() + "/year="
          + outputCalendar.get(Calendar.YEAR) + "/month=" + outputCalendar.get(Calendar.MONTH)
          + "/livecyclehireupdates");
    } catch (IllegalArgumentException exception) {
      // necessary for MRUnit to work with MultipleOutputs
      context.write(outputKey, outputValue);
    }
  }
}