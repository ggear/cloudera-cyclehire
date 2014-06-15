package com.cloudera.cycelhire.main.process.clean;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.cycelhire.main.common.Driver;

public class CleanDriver extends Driver {

  public CleanDriver() {
    super();
  }

  public CleanDriver(Configuration confguration) {
    super(confguration);
  }

  @Override
  public int execute() throws Exception {
    return 0;
  }

}
