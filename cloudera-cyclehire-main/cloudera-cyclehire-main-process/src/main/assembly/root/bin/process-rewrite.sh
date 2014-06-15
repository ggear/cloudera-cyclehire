#!/bin/bash

hadoop jar $1/lib/process/lib/*.jar com.cloudera.cycelhire.main.process.rewrite.RewriteDriver $2 $3
