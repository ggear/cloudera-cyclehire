package com.cloudera.cyclehire.main.process;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

import parquet.hadoop.ParquetOutputFormat;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.Driver;
import com.cloudera.cyclehire.main.common.hdfs.HDFSClientUtil;
import com.cloudera.cyclehire.main.common.mapreduce.MapReduceUtil;
import com.cloudera.cyclehire.main.common.model.PartitionFlag;
import com.cloudera.cyclehire.main.process.partition.PartitionDriver;
import com.cloudera.cyclehire.main.process.table.Table;
import com.cloudera.cyclehire.main.test.BaseTestCase;
import com.cloudera.cyclehire.main.test.EmbeddedHiveTestCase;

public class TableTest extends EmbeddedHiveTestCase {

  private static final String PATH_HDFS_DIR_PARTITIONED = "file://"
      + PATH_LOCAL_WORKING_DIR + '/'
      + BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED + '/';
  private static final String PATH_HDFS_DIR_PROCESSED = "file://"
      + PATH_LOCAL_WORKING_DIR + '/' + BaseTestCase.PATH_HDFS_DIR_PROCESSED
      + '/';

  @SuppressWarnings("serial")
  private static final List<String[]> TABLES = new ArrayList<String[]>() {
    {
      add(new String[] {
          "cyclehire_raw_partitioned_"
              + Counter.BATCHES_SUCCESSFUL.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PARTITIONED + Counter.BATCHES_SUCCESSFUL.getPath(),
          Table.DDL_LOCATION_PARTITIONED_CREATE });
      add(new String[] {
          "cyclehire_raw_partitioned_"
              + Counter.BATCHES_FAILED.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PARTITIONED + Counter.BATCHES_FAILED.getPath(),
          Table.DDL_LOCATION_PARTITIONED_CREATE });
      add(new String[] {
          "cyclehire_processed_"
              + Counter.RECORDS_CLEANSED.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PROCESSED + Counter.RECORDS_CLEANSED.getPath(),
          Table.DDL_LOCATION_PROCESSED_CREATE });
      add(new String[] {
          "cyclehire_processed_"
              + Counter.RECORDS_DUPLICATE.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PROCESSED + Counter.RECORDS_DUPLICATE.getPath(),
          Table.DDL_LOCATION_PROCESSED_CREATE });
      add(new String[] {
          "cyclehire_processed_"
              + Counter.RECORDS_MALFORMED.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PROCESSED + Counter.RECORDS_MALFORMED.getPath(),
          Table.DDL_LOCATION_PROCESSED_CREATE });
    }
  };

  @SuppressWarnings("serial")
  private static final List<String[]> TABLES_REWRITE = new ArrayList<String[]>() {
    {
      add(new String[] {
          "cyclehire_processed_"
              + Counter.RECORDS_REWRITE.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PROCESSED + Counter.RECORDS_REWRITE.getPath(),
          Table.DDL_LOCATION_PROCESSED_REWRITE_SEQUENCE, "sequence", "true",
          "lz4", "org.apache.hadoop.io.compress.Lz4Codec", "BLOCK", "LZ4" });
      add(new String[] {
          "cyclehire_processed_"
              + Counter.RECORDS_REWRITE.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PROCESSED + Counter.RECORDS_REWRITE.getPath(),
          Table.DDL_LOCATION_PROCESSED_REWRITE_AVRO, "avro", "true", "snappy",
          "org.apache.hadoop.io.compress.SnappyCodec", "BLOCK", "SNAPPY" });
      add(new String[] {
          "cyclehire_processed_"
              + Counter.RECORDS_REWRITE.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PROCESSED + Counter.RECORDS_REWRITE.getPath(),
          Table.DDL_LOCATION_PROCESSED_REWRITE_PARQUET, "parquet", "false",
          "dict", "", "BLOCK", "UNCOMPRESSED" });

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
    for (String[] attribute : TABLES) {
      getConf().set(Table.DDL_CONFIG_TABLE_NAME, attribute[0]);
      getConf().set(
          Table.DDL_CONFIG_TABLE_LOCATION,
          attribute[1] + '/' + PartitionDriver.OUTPUT_FORMAT + '/'
              + MapReduceUtil.getCodecString(getConf()));
      execute(Table.DDL_LOCATION, attribute[2]);
    }
    Assert
        .assertEquals(TABLES.size(), executeAndFetchAll("SHOW TABLES").size());

    for (Path path : HDFSClientUtil.listFiles(getFileSystem(), new Path(
        BaseTestCase.PATH_HDFS_DIR_PROCESSED, Counter.RECORDS_REWRITE.getPath()
            + '/' + Table.DDL_LOCATION_PROCESSED_REWRITE_FORMATS[0]), true)) {
      if (path.getName().equals(PartitionFlag._REWRITE.toString())) {
        getConf().set(Table.DDL_CONFIG_TABLE_PARTITION_YEAR,
            path.getParent().getParent().getName().replace("year=", ""));
        getConf().set(Table.DDL_CONFIG_TABLE_PARTITION_MONTH,
            path.getParent().getName().replace("month=", ""));
        for (String[] attribute : TABLES_REWRITE) {
          getConf().set(Table.DDL_CONFIG_TABLE_NAME,
              attribute[0] + "_" + attribute[3] + "_" + attribute[5]);
          getConf().set(Table.DDL_CONFIG_TABLE_LOCATION,
              attribute[1] + '/' + attribute[3] + '/' + attribute[5]);
          getConf().set(Table.DDL_CONFIG_TABLE_CODEC, attribute[5]);
          getConf().set(HiveConf.ConfVars.COMPRESSRESULT.varname, attribute[4]);
          getConf().set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, attribute[6]);
          getConf().set(ParquetOutputFormat.COMPRESSION, attribute[8]);
          getConf().set(FileOutputFormat.COMPRESS_TYPE, attribute[7]);
          execute(Table.DDL_LOCATION, attribute[2]);
        }
      }
    }
    Assert.assertEquals(TABLES.size() + TABLES_REWRITE.size(),
        executeAndFetchAll("SHOW TABLES").size());

  }

  @Test
  public void testTableValieRinseRepeat() throws TException, IOException {

    Assert.assertEquals(0, executeAndFetchAll("SHOW TABLES").size());

    for (String[] attribute : TABLES) {
      getConf().set(Table.DDL_CONFIG_TABLE_NAME, attribute[0]);
      getConf().set(
          Table.DDL_CONFIG_TABLE_LOCATION,
          attribute[1] + '/' + PartitionDriver.OUTPUT_FORMAT + '/'
              + MapReduceUtil.getCodecString(getConf()));
      execute(Table.DDL_LOCATION, attribute[2]);
    }

    for (Path path : HDFSClientUtil.listFiles(getFileSystem(), new Path(
        BaseTestCase.PATH_HDFS_DIR_PROCESSED, Counter.RECORDS_REWRITE.getPath()
            + '/' + Table.DDL_LOCATION_PROCESSED_REWRITE_FORMATS[0]), true)) {
      if (path.getName().equals(PartitionFlag._REWRITE.toString())) {
        getConf().set(Table.DDL_CONFIG_TABLE_PARTITION_YEAR,
            path.getParent().getParent().getName().replace("year=", ""));
        getConf().set(Table.DDL_CONFIG_TABLE_PARTITION_MONTH,
            path.getParent().getName().replace("month=", ""));
        for (String[] attribute : TABLES_REWRITE) {
          getConf().set(Table.DDL_CONFIG_TABLE_NAME,
              attribute[0] + "_" + attribute[3] + "_" + attribute[5]);
          getConf().set(Table.DDL_CONFIG_TABLE_LOCATION,
              attribute[1] + '/' + attribute[3] + '/' + attribute[5]);
          getConf().set(Table.DDL_CONFIG_TABLE_CODEC, attribute[5]);
          getConf().set(HiveConf.ConfVars.COMPRESSRESULT.varname, attribute[4]);
          getConf().set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, attribute[6]);
          getConf().set(ParquetOutputFormat.COMPRESSION, attribute[8]);
          getConf().set(FileOutputFormat.COMPRESS_TYPE, attribute[7]);
          execute(Table.DDL_LOCATION, attribute[2]);
        }
      }
    }

    Assert.assertEquals(TABLES.size() + TABLES_REWRITE.size(),
        executeAndFetchAll("SHOW TABLES").size());
    for (String table : executeAndFetchAll("SHOW TABLES")) {
      Assert
          .assertTrue(executeAndFetchAll("SHOW PARTITIONS " + table).size() >= 0);
      Assert.assertTrue(executeAndFetchAll("SELECT count(1) FROM " + table)
          .size() >= 0);
    }

  }

}
