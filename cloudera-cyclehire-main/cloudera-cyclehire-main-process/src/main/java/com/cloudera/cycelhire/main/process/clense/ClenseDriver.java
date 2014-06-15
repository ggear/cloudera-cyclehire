package com.cloudera.cycelhire.main.process.clense;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.cycelhire.main.common.Driver;

public class ClenseDriver extends Driver {

  public ClenseDriver() {
    super();
  }

  public ClenseDriver(Configuration confguration) {
    super(confguration);
  }

  @Override
  public int execute() throws Exception {
    return 0;
  }

}
