package com.cloudera.cycelhire.main.process.stats;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.cycelhire.main.common.Driver;

public class StatsDriver extends Driver {

  public StatsDriver() {
    super();
  }

  public StatsDriver(Configuration confguration) {
    super(confguration);
  }

  @Override
  public int execute() throws Exception {
    return 0;
  }

}
