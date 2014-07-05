#!/bin/bash

hadoop jar $1/lib/process/lib/*.jar com.cloudera.cyclehire.main.process.stage.StageDriver $2 $3
