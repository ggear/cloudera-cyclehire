#!/bin/bash

hive --hiveconf cyclehire.table.modifier=valid --hiveconf cyclehire.table.location=$2/valid -f $1/lib/process/lib/com/cloudera/cyclehire/main/process/table/partitioned_create.sql
hive --hiveconf cyclehire.table.modifier=invalid --hiveconf cyclehire.table.location=$2/invalid -f $1/lib/process/lib/com/cloudera/cyclehire/main/process/table/partitioned_create.sql
hive --hiveconf cyclehire.table.modifier=cleansed_canonical --hiveconf cyclehire.table.location=$3/cleansed/canonical -f $1/lib/process/lib/com/cloudera/cyclehire/main/process/table/processed_create.sql
hive --hiveconf cyclehire.table.modifier=erroneous_malformed --hiveconf cyclehire.table.location=$3/erroneous/malformed -f $1/lib/process/lib/com/cloudera/cyclehire/main/process/table/processed_create.sql
hive --hiveconf cyclehire.table.modifier=erroneous_duplicate --hiveconf cyclehire.table.location=$3/erroneous/duplicate -f $1/lib/process/lib/com/cloudera/cyclehire/main/process/table/processed_create.sql
