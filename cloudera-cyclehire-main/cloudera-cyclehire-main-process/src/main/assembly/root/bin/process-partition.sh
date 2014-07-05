#!/bin/bash

hadoop jar $1/lib/process/lib/*.jar com.cloudera.cyclehire.main.process.partition.ParitionDriver $2 $3
