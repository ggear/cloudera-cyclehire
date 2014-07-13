package com.cloudera.cyclehire.main.process;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
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

public class TableTest extends EmbeddedHiveTestCase {

  private static final Counter[] PATH_PROCESSED_MODIFIERS = new Counter[] {
      Counter.RECORDS_CLEANSED, Counter.RECORDS_DUPLICATE,
      Counter.RECORDS_MALFORMED };

  private static final String PATH_HDFS_DIR_PROCESSED = PATH_LOCAL_WORKING_DIR
      + '/' + BaseTestCase.PATH_HDFS_DIR_PROCESSED + '/';

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
  }

  @Test
  public void testTableValid() throws TException, IOException {

    Assert.assertEquals(0, executeAndFetchAll("SHOW TABLES").size());
    for (Counter counter : PATH_PROCESSED_MODIFIERS) {
      getConf().set(Table.DDL_CONFIG_TABLE_MODIFIER,
          counter.getPath().replace('/', '_'));
      getConf().set(
          Table.DDL_CONFIG_TABLE_LOCATION,
          PATH_HDFS_DIR_PROCESSED + counter.getPath() + '/'
              + PartitionDriver.OUTPUT_FORMAT + '/'
              + MapReduceUtil.getCodecString(getConf()));
      execute(Table.DDL_LOCATION, Table.DDL_LOCATION_PROCESSED_CREATE);
    }
    Assert.assertEquals(PATH_PROCESSED_MODIFIERS.length,
        executeAndFetchAll("SHOW TABLES").size());

    executeAndFetchAll("SELECT count(1) from cyclehire_processed_cleansed_canonical");
    executeAndFetchAll("SELECT count(1) from cyclehire_processed_erroneous_duplicate");
    executeAndFetchAll("SELECT count(1) from cyclehire_processed_erroneous_malformed");

  }

  @Test
  public void testTableValieRinseRepeat() throws TException, IOException {

    Assert.assertEquals(0, executeAndFetchAll("SHOW TABLES").size());
    for (Counter counter : PATH_PROCESSED_MODIFIERS) {
      getConf().set(Table.DDL_CONFIG_TABLE_MODIFIER,
          counter.getPath().replace('/', '_'));
      getConf().set(
          Table.DDL_CONFIG_TABLE_LOCATION,
          PATH_HDFS_DIR_PROCESSED + counter.getPath() + '/'
              + PartitionDriver.OUTPUT_FORMAT + '/'
              + MapReduceUtil.getCodecString(getConf()));
      execute(Table.DDL_LOCATION, Table.DDL_LOCATION_PROCESSED_CREATE);
    }
    Assert.assertEquals(PATH_PROCESSED_MODIFIERS.length,
        executeAndFetchAll("SHOW TABLES").size());
    for (String table : executeAndFetchAll("SHOW TABLES")) {
      Assert
          .assertTrue(executeAndFetchAll("SHOW PARTITIONS " + table).size() > 0);
      Assert.assertTrue(executeAndFetchAll("SELECT count(1) FROM " + table)
          .size() > 0);
    }

    for (Counter counter : PATH_PROCESSED_MODIFIERS) {
      getConf().set(Table.DDL_CONFIG_TABLE_MODIFIER,
          counter.getPath().replace('/', '_'));
      getConf().set(
          Table.DDL_CONFIG_TABLE_LOCATION,
          PATH_HDFS_DIR_PROCESSED + counter.getPath() + '/'
              + PartitionDriver.OUTPUT_FORMAT + '/'
              + MapReduceUtil.getCodecString(getConf()));
      execute(Table.DDL_LOCATION, Table.DDL_LOCATION_PROCESSED_CREATE);
    }
    Assert.assertEquals(PATH_PROCESSED_MODIFIERS.length,
        executeAndFetchAll("SHOW TABLES").size());
    for (String table : executeAndFetchAll("SHOW TABLES")) {
      Assert
          .assertTrue(executeAndFetchAll("SHOW PARTITIONS " + table).size() > 0);
      Assert.assertTrue(executeAndFetchAll("SELECT count(1) FROM " + table)
          .size() > 0);
    }

    getFileSystem().delete(
        new Path(BaseTestCase.PATH_HDFS_DIR_RAW_PARTITIONED), true);
    getFileSystem()
        .delete(new Path(BaseTestCase.PATH_HDFS_DIR_PROCESSED), true);

    for (Counter counter : PATH_PROCESSED_MODIFIERS) {
      getConf().set(Table.DDL_CONFIG_TABLE_MODIFIER,
          counter.getPath().replace('/', '_'));
      getConf().set(
          Table.DDL_CONFIG_TABLE_LOCATION,
          PATH_HDFS_DIR_PROCESSED + counter.getPath() + '/'
              + PartitionDriver.OUTPUT_FORMAT + '/'
              + MapReduceUtil.getCodecString(getConf()));
      execute(Table.DDL_LOCATION, Table.DDL_LOCATION_PROCESSED_CREATE);
    }
    Assert.assertEquals(PATH_PROCESSED_MODIFIERS.length,
        executeAndFetchAll("SHOW TABLES").size());
    for (String table : executeAndFetchAll("SHOW TABLES")) {
      Assert
          .assertTrue(executeAndFetchAll("SHOW PARTITIONS " + table).size() > 0);
      Assert.assertTrue(executeAndFetchAll("SELECT count(1) FROM " + table)
          .size() > 0);
    }

    for (Counter counter : PATH_PROCESSED_MODIFIERS) {
      getConf().set(Table.DDL_CONFIG_TABLE_MODIFIER,
          counter.getPath().replace('/', '_'));
      execute(Table.DDL_LOCATION, Table.DDL_LOCATION_PROCESSED_DROP);
    }
    Assert.assertEquals(0, executeAndFetchAll("SHOW TABLES").size());

  }

}
