#!/bin/bash

hadoop jar $1/lib/process/lib/*.jar com.cloudera.cyclehire.main.process.stage.ProcessDriver $2 $3 $3
