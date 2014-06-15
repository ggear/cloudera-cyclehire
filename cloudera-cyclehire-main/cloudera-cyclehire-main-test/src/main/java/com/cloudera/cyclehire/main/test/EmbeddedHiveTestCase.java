package com.cloudera.cyclehire.main.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.service.HiveInterface;
import org.apache.hadoop.hive.service.HiveServer;
import org.apache.hadoop.hive.service.HiveServerException;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class EmbeddedHiveTestCase extends EmbeddedCoreTestCase {

  private static Logger log = LoggerFactory.getLogger(EmbeddedHiveTestCase.class);

  private HiveInterface hive;
  private HiveConf hiveConf;

  public EmbeddedHiveTestCase() throws IOException {
    super();
  }

  @Before
  @Override
  public void setUp() throws Exception {
    super.setUp();
    hive = new HiveServer.HiveServerHandler(getConf());
  }

  @After
  @Override
  public void tearDown() throws Exception {
    hive.shutdown();
    hiveConf = null;
    super.tearDown();
  }

  public HiveConf getConf() throws TException {
    return hiveConf == null ? hiveConf = new HiveConf(SessionState.class) : hiveConf;
  }

  public void execute(String query) throws HiveServerException, TException {
    _execute(query);
  }

  public List<String> executeAndFetchAll(String query) throws HiveServerException, TException {
    _execute(query);
    return _fetchAll(query);
  }

  public String executeAndFetchOne(String query) throws HiveServerException, TException {
    _execute(query);
    return _fetchOne(query);
  }

  public void execute(String directory, String file) throws HiveServerException, TException, IOException {
    for (String query : readColonDelimiteredLinesFromFileOnClasspath(directory, file)) {
      _execute(query);
    }
  }

  public List<List<String>> executeAndFetchAll(String directory, String file) throws HiveServerException, TException,
      IOException {
    List<List<String>> rows = new ArrayList<List<String>>();
    for (String query : readColonDelimiteredLinesFromFileOnClasspath(directory, file)) {
      _execute(query);
      rows.add(_fetchAll(query));
    }
    return rows;
  }

  private void _execute(String query) throws HiveServerException, TException {
    if (log.isDebugEnabled()) {
      log.debug("Hive client test pre-execute:\n" + query + "\n");
    }
    hive.execute(query);
    if (log.isDebugEnabled()) {
      log.debug("Hive client test post-execute:\n" + query + "\n");
    }
  }

  private List<String> _fetch(String query, List<String> rows) throws HiveServerException, TException {
    if (log.isDebugEnabled()) {
      StringBuilder rowsString = new StringBuilder();
      rowsString.append("Hive client test fetched results:\n" + query + "\n");
      for (String row : rows) {
        rowsString.append('\n');
        rowsString.append(row);
      }
      rowsString.append('\n');
      log.debug(rowsString.toString());
    }
    return rows;
  }

  private List<String> _fetchAll(String query) throws HiveServerException, TException {
    return _fetch(query, hive.fetchAll());
  }

  private String _fetchOne(String query) throws HiveServerException, TException {
    return _fetch(query, Arrays.asList(new String[] { hive.fetchOne() })).get(0);
  }

  private List<String> readColonDelimiteredLinesFromFileOnClasspath(String directory, String file) throws IOException {
    List<String> lines = new ArrayList<String>();
    URL fileUrl = EmbeddedHiveTestCase.class.getResource(directory + "/" + file);
    if (fileUrl != null) {
      for (String line : FileUtils.readFileToString(new File(fileUrl.getFile())).split(";")) {
        if (!line.trim().equals("")) {
          lines.add(line.trim());
        }
      }
      return lines;
    }
    throw new IOException("Could not load file [" + directory + "/" + file + "] from classpath");
  }
}
