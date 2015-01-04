#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

[ -f $ROOT_DIR/../../bin/cyclehire.env ] && source $ROOT_DIR/../../bin/cyclehire.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_PROCESSED=${2:-"$ROOT_DIR_HDFS_PROCESSED"}
TABLE_BLOCKSIZE=${3:-"256000000"}
TABLE_PARTITION_SPLIT=${4:-"250000000"}
TABLE_COMPRESS=${5:-"true"}
TABLE_CODEC=${6:-"none"}
TABLE_CODEC_CLASS=${7:-"DICT"}
TABLE_PARTITION_YEAR="$8"
TABLE_PARTITION_MONTH="$9"
export HIVE_AUX_JARS_PATH="$(echo -n $(ls -m $ROOT_DIR/lib/jar/dep/*.jar)|sed 's/, /:/g')"

TABLE_CODEC_DICT="true"
if [ "$TABLE_COMPRESS" = "false" ]; then
	TABLE_CODEC_CLASS="UNCOMPRESSED"
	TABLE_CODEC="none"
	TABLE_CODEC_DICT="false"
elif [ "$TABLE_CODEC_CLASS" = "DICT" ]; then
	TABLE_COMPRESS="false"
	TABLE_CODEC_CLASS="UNCOMPRESSED"
	TABLE_CODEC="dict"
fi

TABLE_NAME=cyclehire_processed_cleansed_rewrite_parquet_$TABLE_CODEC
TABLE_LOCATION=$ROOT_DIR_HDFS_PROCESSED/cleansed/rewrite/parquet/$TABLE_CODEC

PARTITION_YEARS=()
PARTITION_MONTHS=()
if [ -z "$TABLE_PARTITION_YEAR" ] || [ -z "$TABLE_PARTITION_MONTH" ]; then
	for PATH_REWRITE in $(hadoop jar $HADOOP_LIB_SEARCH org.apache.solr.hadoop.HdfsFindTool -find $TABLE_LOCATION -name _REWRITE 2> /dev/null); do
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
		--hiveconf "dfs.blocksize=$TABLE_BLOCKSIZE" \
		--hiveconf "parquet.block.size=$TABLE_PARTITION_SPLIT" \
		--hiveconf "parquet.page.size=1048576" \
		--hiveconf "parquet.dictionary.page.size=1048576" \
		--hiveconf "parquet.compression=$TABLE_CODEC_CLASS" \
		--hiveconf "cyclehire.table.codec=$TABLE_CODEC" \
		--hiveconf "parquet.enable.dictionary=$TABLE_CODEC_DICT" \
		--hiveconf "mapreduce.input.fileinputformat.split.minsize=$TABLE_PARTITION_SPLIT" \
		--hiveconf "cyclehire.table.partition.year=${PARTITION_YEARS[$i]}" \
		--hiveconf "cyclehire.table.partition.month=${PARTITION_MONTHS[$i]}" \
		--hiveconf "cyclehire.table.name=$TABLE_NAME" \
		--hiveconf "cyclehire.table.location=$TABLE_LOCATION" \
		$CMD_LINE_ARGUMENTS \
		-f "$ROOT_DIR/lib/ddl/processed_rewrite_parquet.ddl"
	hadoop fs -rm -f $TABLE_LOCATION/year=${PARTITION_YEARS[$i]}/month=${PARTITION_MONTHS[$i]}/_REWRITE
done
