package com.cloudera.cyclehire.main.process;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.Driver;
import com.cloudera.cyclehire.main.common.mapreduce.MapReduceUtil;
import com.cloudera.cyclehire.main.process.partition.PartitionDriver;
import com.cloudera.cyclehire.main.process.table.Table;
import com.cloudera.cyclehire.main.test.BaseTestCase;
import com.cloudera.cyclehire.main.test.EmbeddedHiveTestCase;
import com.google.common.collect.ImmutableMap;

public class TableTest extends EmbeddedHiveTestCase {

  private static final String PATH_HDFS_DIR_PARTITIONED = "file://"
      + PATH_LOCAL_WORKING_DIR + '/'
      + BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED + '/';
  private static final String PATH_HDFS_DIR_PROCESSED = "file://"
      + PATH_LOCAL_WORKING_DIR + '/' + BaseTestCase.PATH_HDFS_DIR_PROCESSED
      + '/';

  private static final Map<Counter, String[]> TABLES = ImmutableMap.of(
      Counter.BATCHES_SUCCESSFUL, new String[] { PATH_HDFS_DIR_PARTITIONED,
          Table.DDL_LOCATION_PARTITIONED_CREATE }, Counter.BATCHES_FAILED,
      new String[] { PATH_HDFS_DIR_PARTITIONED,
          Table.DDL_LOCATION_PARTITIONED_CREATE }, Counter.RECORDS_CLEANSED,
      new String[] { PATH_HDFS_DIR_PROCESSED,
          Table.DDL_LOCATION_PROCESSED_CREATE }, Counter.RECORDS_DUPLICATE,
      new String[] { PATH_HDFS_DIR_PROCESSED,
          Table.DDL_LOCATION_PROCESSED_CREATE }, Counter.RECORDS_MALFORMED,
      new String[] { PATH_HDFS_DIR_PROCESSED,
          Table.DDL_LOCATION_PROCESSED_CREATE });

  @SuppressWarnings("serial")
  List<String[]> TABLES_REWRITE = new ArrayList<String[]>() {
    {
      add(new String[] { Counter.RECORDS_REWRITE.getPath(),
          PATH_HDFS_DIR_PROCESSED,
          Table.DDL_LOCATION_PROCESSED_REWRITE_SEQUENCE, "sequence", "false",
          "none", "org.apache.hadoop.io.compress.SnappyCodec", "BLOCK" });
      add(new String[] { Counter.RECORDS_REWRITE.getPath(),
          PATH_HDFS_DIR_PROCESSED,
          Table.DDL_LOCATION_PROCESSED_REWRITE_SEQUENCE, "sequence", "true",
          "snappy", "org.apache.hadoop.io.compress.SnappyCodec", "BLOCK" });
      add(new String[] { Counter.RECORDS_REWRITE.getPath(),
          PATH_HDFS_DIR_PROCESSED, Table.DDL_LOCATION_PROCESSED_REWRITE_AVRO,
          "avro", "false", "none", "org.apache.hadoop.io.compress.SnappyCodec",
          "BLOCK" });
      add(new String[] { Counter.RECORDS_REWRITE.getPath(),
          PATH_HDFS_DIR_PROCESSED, Table.DDL_LOCATION_PROCESSED_REWRITE_AVRO,
          "avro", "true", "snappy",
          "org.apache.hadoop.io.compress.SnappyCodec", "BLOCK" });
    }
  };

  public TableTest() throws IOException {
    super();
  }

  @Override
  public void setUp() throws Exception {
    super.setUp();
    new BaseTest() {
    }.setUp();
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        new ProcessDriver(getFileSystem().getConf()).runner(new String[] {
            BaseTestCase.PATH_HDFS_DIR_RAW_LANDED,
            BaseTestCase.PATH_HDFS_DIR_RAW_STAGED,
            BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED,
            BaseTestCase.PATH_HDFS_DIR_PROCESSED }));
    getConf().set(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE.varname,
        "nonstrict");
    getConf().set(HiveConf.ConfVars.COMPRESSRESULT.varname, "false");
  }

  @Test
  public void testTableValid() throws TException, IOException {

    Assert.assertEquals(0, executeAndFetchAll("SHOW TABLES").size());
    for (Counter counter : TABLES.keySet()) {
      getConf().set(Table.DDL_CONFIG_TABLE_MODIFIER,
          counter.getPath().replace('/', '_'));
      getConf().set(
          Table.DDL_CONFIG_TABLE_LOCATION,
          TABLES.get(counter)[0] + counter.getPath() + '/'
              + PartitionDriver.OUTPUT_FORMAT + '/'
              + MapReduceUtil.getCodecString(getConf()));
      execute(Table.DDL_LOCATION, TABLES.get(counter)[1]);
    }
    Assert
        .assertEquals(TABLES.size(), executeAndFetchAll("SHOW TABLES").size());

    for (String[] attribute : TABLES_REWRITE) {
      getConf().set(Table.DDL_CONFIG_TABLE_MODIFIER,
          attribute[0].replace('/', '_'));
      getConf()
          .set(
              Table.DDL_CONFIG_TABLE_LOCATION,
              attribute[1] + attribute[0] + '/' + attribute[3] + '/'
                  + attribute[5]);
      getConf().set(Table.DDL_CONFIG_TABLE_CODEC, attribute[5]);
      getConf().set(HiveConf.ConfVars.COMPRESSRESULT.varname, attribute[4]);
      getConf().set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, attribute[6]);
      getConf().set(FileOutputFormat.COMPRESS_TYPE, attribute[7]);
      execute(Table.DDL_LOCATION, attribute[2]);
    }
    Assert.assertEquals(TABLES.size() + TABLES_REWRITE.size(),
        executeAndFetchAll("SHOW TABLES").size());

  }

  @Test
  public void testTableValieRinseRepeat() throws TException, IOException {

    Assert.assertEquals(0, executeAndFetchAll("SHOW TABLES").size());
    for (Counter counter : TABLES.keySet()) {
      getConf().set(Table.DDL_CONFIG_TABLE_MODIFIER,
          counter.getPath().replace('/', '_'));
      getConf().set(
          Table.DDL_CONFIG_TABLE_LOCATION,
          TABLES.get(counter)[0] + counter.getPath() + '/'
              + PartitionDriver.OUTPUT_FORMAT + '/'
              + MapReduceUtil.getCodecString(getConf()));
      execute(Table.DDL_LOCATION, TABLES.get(counter)[1]);
    }
    Assert
        .assertEquals(TABLES.size(), executeAndFetchAll("SHOW TABLES").size());
    for (String table : executeAndFetchAll("SHOW TABLES")) {
      Assert
          .assertTrue(executeAndFetchAll("SHOW PARTITIONS " + table).size() > 0);
      Assert.assertTrue(executeAndFetchAll("SELECT count(1) FROM " + table)
          .size() > 0);
    }

    for (Counter counter : TABLES.keySet()) {
      getConf().set(Table.DDL_CONFIG_TABLE_MODIFIER,
          counter.getPath().replace('/', '_'));
      getConf().set(
          Table.DDL_CONFIG_TABLE_LOCATION,
          TABLES.get(counter)[0] + counter.getPath() + '/'
              + PartitionDriver.OUTPUT_FORMAT + '/'
              + MapReduceUtil.getCodecString(getConf()));
      execute(Table.DDL_LOCATION, TABLES.get(counter)[1]);
    }
    Assert
        .assertEquals(TABLES.size(), executeAndFetchAll("SHOW TABLES").size());
    for (String table : executeAndFetchAll("SHOW TABLES")) {
      Assert
          .assertTrue(executeAndFetchAll("SHOW PARTITIONS " + table).size() > 0);
      Assert.assertTrue(executeAndFetchAll("SELECT count(1) FROM " + table)
          .size() > 0);
    }

  }

}
