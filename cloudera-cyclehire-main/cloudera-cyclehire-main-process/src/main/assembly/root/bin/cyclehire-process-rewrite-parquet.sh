#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

[ -f $ROOT_DIR/../../bin/cyclehire.env ] && source $ROOT_DIR/../../bin/cyclehire.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_PROCESSED=${2:-"$ROOT_DIR_HDFS_PROCESSED"}
TABLE_PARTITION_SPLIT=${3:-"14000000000"}
TABLE_COMPRESS=${4:-"false"}
TABLE_CODEC=${5:-"none"}
TABLE_CODEC_CLASS=${6:-"SNAPPY"}
TABLE_PARTITION_YEAR="$7"
TABLE_PARTITION_MONTH="$8"
export HIVE_AUX_JARS_PATH="$(echo -n $(ls -m $ROOT_DIR/lib/jar/dep/*.jar)|sed 's/, /:/g')"

TABLE_LOCATION=$ROOT_DIR_HDFS_PROCESSED/cleansed/rewrite/parquet/$TABLE_CODEC

if [ "$TABLE_COMPRESS" = "false" ]; then
	TABLE_CODEC_CLASS="UNCOMPRESSED"
	TABLE_CODEC="none"
fi

PARTITION_YEARS=()
PARTITION_MONTHS=()
if [ -z "$TABLE_PARTITION_YEAR" ] || [ -z "$TABLE_PARTITION_MONTH" ]; then
	for PATH_REWRITE in $(hadoop jar $HADOOP_LIB_SEARCH org.apache.solr.hadoop.HdfsFindTool -find $TABLE_LOCATION -name _REWRITE); do
   		PARTITION_YEARS+=("$(basename $(dirname $(dirname $PATH_REWRITE))|cut -c 6-|tr -d '\n')")
   		PARTITION_MONTHS+=("$(basename $(dirname $PATH_REWRITE)|cut -c 7-|tr -d '\n')")
	done
else
	PARTITION_YEARS+=("$TABLE_PARTITION_YEAR")
	PARTITION_MONTHS+=("$TABLE_PARTITION_MONTH")
fi

for((i=0;i<${#PARTITION_YEARS[@]};i++)); do
	hive \
		--hiveconf "hive.stats.autogather=false" \
		--hiveconf "hive.exec.dynamic.partition.mode=nonstrict" \
		--hiveconf "parquet.compression=$TABLE_CODEC_CLASS" \
		--hiveconf "cyclehire.table.codec=$TABLE_CODEC" \
		--hiveconf "mapreduce.input.fileinputformat.split.minsize=$TABLE_PARTITION_SPLIT" \
		--hiveconf "mapreduce.input.fileinputformat.split.maxsize=$TABLE_PARTITION_SPLIT" \
		--hiveconf "cyclehire.table.partition.year=${PARTITION_YEARS[$i]}" \
		--hiveconf "cyclehire.table.partition.month=${PARTITION_MONTHS[$i]}" \
		--hiveconf "cyclehire.table.modifier=cleansed_rewrite" \
		--hiveconf "cyclehire.table.location=$TABLE_LOCATION" \
		$CMD_LINE_ARGUMENTS \
		-f "$ROOT_DIR/lib/ddl/processed_rewrite_parquet.ddl"
done
