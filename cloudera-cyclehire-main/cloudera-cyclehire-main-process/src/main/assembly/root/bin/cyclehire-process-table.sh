#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

[ -f $ROOT_DIR/../../bin/cyclehire.env ] && source $ROOT_DIR/../../bin/cyclehire.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_RAW_PARTITIONED=${2:-"$ROOT_DIR_HDFS_RAW_PARTITIONED"}
ROOT_DIR_HDFS_PROCESSED=${3:-"$ROOT_DIR_HDFS_PROCESSED"}
export HIVE_AUX_JARS_PATH="$(echo -n $(ls -m $ROOT_DIR/lib/jar/dep/*.jar)|sed 's/, /:/g')"

hive \
	--hiveconf "cyclehire.table.modifier=valid" \
	--hiveconf "cyclehire.table.location=$ROOT_DIR_HDFS_RAW_PARTITIONED/valid/sequence/none" \
	-f "$ROOT_DIR/lib/ddl/partitioned_create.ddl"

hive \
	--hiveconf "cyclehire.table.modifier=invalid" \
	--hiveconf "cyclehire.table.location=$ROOT_DIR_HDFS_RAW_PARTITIONED/invalid/sequence/none" \
	$CMD_LINE_ARGUMENTS \
	-f "$ROOT_DIR/lib/ddl/partitioned_create.ddl"

hive \
	--hiveconf "cyclehire.table.modifier=cleansed_canonical" \
	--hiveconf "cyclehire.table.location=$ROOT_DIR_HDFS_PROCESSED/cleansed/canonical/sequence/none" \
	$CMD_LINE_ARGUMENTS \
	-f "$ROOT_DIR/lib/ddl/processed_create.ddl"

hive \
	--hiveconf "cyclehire.table.modifier=erroneous_duplicate" \
	--hiveconf "cyclehire.table.location=$ROOT_DIR_HDFS_PROCESSED/erroneous/duplicate/sequence/none" \
	$CMD_LINE_ARGUMENTS \
	-f "$ROOT_DIR/lib/ddl/processed_create.ddl"

hive \
	--hiveconf "cyclehire.table.modifier=erroneous_malformed" \
	--hiveconf "cyclehire.table.location=$ROOT_DIR_HDFS_PROCESSED/erroneous/malformed/sequence/none" \
	$CMD_LINE_ARGUMENTS \
	-f "$ROOT_DIR/lib/ddl/processed_create.ddl"
