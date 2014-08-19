#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

[ -f $ROOT_DIR/../../bin/cyclehire.env ] && source $ROOT_DIR/../../bin/cyclehire.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_PROCESSED=${2:-"$ROOT_DIR_HDFS_PROCESSED"}
TABLE_PARTITION_SPLIT=${3:-"14000000000"}
TABLE_COMPRESS=${4:-"true"}
TABLE_CODEC=${5:-"snappy"}
TABLE_CODEC_CLASS=${6:-"SNAPPY"}
export HIVE_AUX_JARS_PATH="$(echo -n $(ls -m $ROOT_DIR/lib/jar/dep/*.jar)|sed 's/, /:/g')"

if [ "$TABLE_COMPRESS" = "false" ]; then
	TABLE_CODEC="UNCROMPRESSED"
fi

TABLE_PARTITION_YEAR="2014"
TABLE_PARTITION_MONTH="01"

hive \
	--hiveconf "hive.stats.autogather=false" \
	--hiveconf "hive.exec.dynamic.partition.mode=nonstrict" \
	--hiveconf "parquet.compression=$TABLE_CODEC_CLASS" \
	--hiveconf "cyclehire.table.codec=$TABLE_CODEC" \
	--hiveconf "mapreduce.input.fileinputformat.split.minsize=$TABLE_PARTITION_SPLIT" \
	--hiveconf "mapreduce.input.fileinputformat.split.maxsize=$TABLE_PARTITION_SPLIT" \
	--hiveconf "cyclehire.table.partition.year=$TABLE_PARTITION_YEAR" \
	--hiveconf "cyclehire.table.partition.month=$TABLE_PARTITION_MONTH" \
	--hiveconf "cyclehire.table.modifier=cleansed_rewrite" \
	--hiveconf "cyclehire.table.location=$ROOT_DIR_HDFS_PROCESSED/cleansed/rewrite/parquet/snappy" \
	$CMD_LINE_ARGUMENTS \
	-f "$ROOT_DIR/lib/ddl/processed_rewrite_parquet.ddl"
