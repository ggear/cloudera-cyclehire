package com.cloudera.cyclehire.main.query;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import parquet.hadoop.ParquetOutputFormat;

import com.cloudera.cyclehire.main.common.Counter;
import com.cloudera.cyclehire.main.common.DfsUtil;
import com.cloudera.cyclehire.main.common.MrUtil;
import com.cloudera.cyclehire.main.common.model.PartitionFlag;
import com.cloudera.cyclehire.main.process.ProcessDriver;
import com.cloudera.cyclehire.main.process.partition.PartitionDriver;
import com.cloudera.cyclehire.main.process.table.Table;
import com.cloudera.cyclehire.main.test.TestConstants;
import com.cloudera.framework.main.common.Driver;
import com.cloudera.framework.main.test.BaseTest;
import com.cloudera.framework.main.test.MiniClusterDfsMrHiveTest;

@RunWith(Parameterized.class)
public class QueryTest extends MiniClusterDfsMrHiveTest implements TestConstants {

  @Parameters()
  public static Iterable<Object[]> paramaters() {
    return Arrays.asList(new Object[][] {
    //
    {
        //
        new String[] {
            //
            BaseTest.REL_DIR_DATA, //
            BaseTest.REL_DIR_DATA, }, //
        new String[] {
            //
            DIR_RAW_LANDED_TAR, DIR_RAW_LANDED_XML, }, //
        new String[] {
            //
            DIR_DS, DIR_DS }, //
        new String[][] {
            //
            { DIR_DS_TAR_NOID }, //
            { DIR_DS_XML_NOID }, //
        }, //
        new String[][][] {
            //
            { { null }, }, //
            { { null }, }, //
        }, //
        new Map[] {
            //
            Collections.emptyMap(), //
            Collections.emptyMap(), //
            Collections.emptyMap(), //
        }, //
    }, //
    });
  }

  private static final MiniClusterDfsMrHiveTest miniCluster = new MiniClusterDfsMrHiveTest();
  private static final String PATH_HDFS_DIR_PARTITIONED = miniCluster.getPathDfs(TestConstants.DIR_RAW_PARTITIONED) + '/';
  private static final String PATH_HDFS_DIR_PROCESSED = miniCluster.getPathDfs(TestConstants.DIR_PROCESSED) + '/';

  @SuppressWarnings("serial")
  private static final List<String[]> TABLES = new ArrayList<String[]>() {
    {
      add(new String[] { "cyclehire_raw_partitioned_" + Counter.BATCHES_SUCCESSFUL.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PARTITIONED + Counter.BATCHES_SUCCESSFUL.getPath(), Table.DDL_LOCATION_PARTITIONED_CREATE });
      add(new String[] { "cyclehire_raw_partitioned_" + Counter.BATCHES_FAILED.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PARTITIONED + Counter.BATCHES_FAILED.getPath(), Table.DDL_LOCATION_PARTITIONED_CREATE });
      add(new String[] { "cyclehire_processed_" + Counter.RECORDS_CLEANSED.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PROCESSED + Counter.RECORDS_CLEANSED.getPath(), Table.DDL_LOCATION_PROCESSED_CREATE });
      add(new String[] { "cyclehire_processed_" + Counter.RECORDS_DUPLICATE.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PROCESSED + Counter.RECORDS_DUPLICATE.getPath(), Table.DDL_LOCATION_PROCESSED_CREATE });
      add(new String[] { "cyclehire_processed_" + Counter.RECORDS_MALFORMED.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PROCESSED + Counter.RECORDS_MALFORMED.getPath(), Table.DDL_LOCATION_PROCESSED_CREATE });
    }
  };

  @SuppressWarnings("serial")
  private static final List<String[]> TABLES_REWRITE = new ArrayList<String[]>() {
    {
      add(new String[] { "cyclehire_processed_" + Counter.RECORDS_REWRITE.getPath().replace('/', '_'),
          PATH_HDFS_DIR_PROCESSED + Counter.RECORDS_REWRITE.getPath(), Table.DDL_LOCATION_PROCESSED_REWRITE_PARQUET,
          "parquet", "false", "none", "", "BLOCK", "UNCOMPRESSED" });
    }
  };

  public QueryTest(String[] sources, String[] destinations, String[] datasets, String[][] subsets, String[][][] labels,
      @SuppressWarnings("rawtypes") Map[] counters) {
    super(sources, destinations, datasets, subsets, labels, counters);
  }

  @Before
  public void setUp() throws Exception {
    Assert.assertEquals(
        Driver.RETURN_SUCCESS,
        new ProcessDriver(getConf()).runner(new String[] { getPathDfs(TestConstants.DIR_RAW_LANDED),
            getPathDfs(TestConstants.DIR_RAW_STAGED), getPathDfs(TestConstants.DIR_RAW_PARTITIONED),
            getPathDfs(TestConstants.DIR_PROCESSED) }));
    Map<String, String> paramaters = new HashMap<String, String>();
    getConf().set(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE.varname, "nonstrict");
    getConf().set(HiveConf.ConfVars.COMPRESSRESULT.varname, "false");
    for (String[] attribute : TABLES) {
      paramaters.put(Table.DDL_CONFIG_TABLE_NAME, attribute[0]);
      paramaters.put(Table.DDL_CONFIG_TABLE_LOCATION,
          attribute[1] + '/' + PartitionDriver.OUTPUT_FORMAT + '/' + MrUtil.getCodecString(getConf()));
      Assert.assertEquals(2, processStatement(Table.DDL_LOCATION, attribute[2], paramaters).size());
    }
    for (Path path : DfsUtil.listFiles(getFileSystem(), new Path(getPathDfs(TestConstants.DIR_PROCESSED),
        Counter.RECORDS_REWRITE.getPath() + '/' + Table.DDL_LOCATION_PROCESSED_REWRITE_FORMATS[0]), true)) {
      if (path.getName().equals(PartitionFlag._REWRITE.toString())) {
        paramaters.put(Table.DDL_CONFIG_TABLE_PARTITION_YEAR,
            path.getParent().getParent().getName().replace("year=", ""));
        paramaters.put(Table.DDL_CONFIG_TABLE_PARTITION_MONTH, path.getParent().getName().replace("month=", ""));
        for (String[] attribute : TABLES_REWRITE) {
          paramaters.put(Table.DDL_CONFIG_TABLE_NAME, attribute[0] + "_" + attribute[3] + "_" + attribute[5]);
          paramaters.put(Table.DDL_CONFIG_TABLE_LOCATION, attribute[1] + '/' + attribute[3] + '/' + attribute[5]);
          paramaters.put(Table.DDL_CONFIG_TABLE_CODEC, attribute[5]);
          getConf().set(HiveConf.ConfVars.COMPRESSRESULT.varname, attribute[4]);
          getConf().set(MRJobConfig.MAP_OUTPUT_COMPRESS_CODEC, attribute[6]);
          getConf().set(ParquetOutputFormat.COMPRESSION, attribute[8]);
          getConf().set(FileOutputFormat.COMPRESS_TYPE, attribute[7]);
          Assert.assertEquals(2, processStatement(Table.DDL_LOCATION, attribute[2], paramaters).size());
        }
      }
    }
    Assert.assertTrue(processStatement("SHOW TABLES").size() > 0);
  }

  @Test
  public void testQueryValid() throws Exception {
    Assert.assertTrue(processStatement(Query.SQL_LOCATION, Query.SQL_LOCATION_SUMMARY).size() > 0);
  }

}
