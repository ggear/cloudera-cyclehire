package com.cloudera.cycelhire.main.process.rewrite;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.cycelhire.main.common.Driver;

public class RewriteDriver extends Driver {

  public RewriteDriver() {
    super();
  }

  public RewriteDriver(Configuration confguration) {
    super(confguration);
  }

  @Override
  public int execute() throws Exception {
    return 0;
  }

}
