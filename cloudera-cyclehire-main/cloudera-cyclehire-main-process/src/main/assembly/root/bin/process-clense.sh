#!/bin/bash

hadoop jar $1/lib/process/lib/*.jar com.cloudera.cyclehire.main.process.clense.ClenseDriver $2 $3 $4
