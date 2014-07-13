#!/bin/bash

hive --hiveconf cyclehire.table.modifier=cleansed --hiveconf cyclehire.table.location=$2 -f $1/lib/process/lib/com/cloudera/cyclehire/main/process/table/processed_create.sql
hive --hiveconf cyclehire.table.modifier=erroneous_malformed --hiveconf cyclehire.table.location=$2 -f $1/lib/process/lib/com/cloudera/cyclehire/main/process/table/processed_create.sql
hive --hiveconf cyclehire.table.modifier=erroneous_duplicate --hiveconf cyclehire.table.location=$2 -f $1/lib/process/lib/com/cloudera/cyclehire/main/process/table/processed_create.sql