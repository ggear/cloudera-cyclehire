#!/bin/bash

hadoop jar $1/lib/process/lib/*.jar com.cloudera.cycelhire.main.process.clense.ClenseDriver $2 $3
