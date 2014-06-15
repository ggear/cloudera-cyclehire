#!/bin/bash

hadoop jar $1/lib/process/lib/*.jar com.cloudera.cycelhire.main.process.partition.ParitionDriver $2 $3
