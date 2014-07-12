#!/bin/bash

hadoop jar $1/lib/process/lib/*.jar com.cloudera.cyclehire.main.process.clean.CleanDriver $2 $3
