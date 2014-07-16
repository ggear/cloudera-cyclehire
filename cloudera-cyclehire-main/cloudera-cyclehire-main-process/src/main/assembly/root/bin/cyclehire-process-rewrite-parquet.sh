#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

[ -f $ROOT_DIR/../../bin/cyclehire.env ] && source $ROOT_DIR/../../bin/cyclehire.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_PROCESSED=${2:-"$ROOT_DIR_HDFS_PROCESSED"}

hive \
	--hiveconf "hive.exec.dynamic.partition.mode=nonstrict" \
	--hiveconf "parquet.compression=SNAPPY" \
	--hiveconf "cyclehire.table.codec=snappy" \
	--hiveconf "cyclehire.table.modifier=cleansed_rewrite" \
	--hiveconf "cyclehire.table.location=$ROOT_DIR_HDFS_PROCESSED/cleansed/canonical/parquet/snappy" \
	$CMD_LINE_ARGUMENTS \
	-f "$ROOT_DIR/lib/ddl/processed_rewrite_parquet.ddl"
