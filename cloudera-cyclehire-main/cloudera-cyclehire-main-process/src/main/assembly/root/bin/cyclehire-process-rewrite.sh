#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

[ -f $ROOT_DIR/../../bin/cyclehire.env ] && source $ROOT_DIR/../../bin/cyclehire.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_PROCESSED=${2:-"$ROOT_DIR_HDFS_PROCESSED"}
TABLE_FORMAT=${3:-"parquet"}
TABLE_PARTITION_YEAR=${4:-"_REWRITE"}
TABLE_PARTITION_MONTH=${5:-"_REWRITE"}
TABLE_COMPRESS=${6:-"false"}
TABLE_CODEC=${7:-"none"}
TABLE_CODEC_CLASS=${8:-"SNAPPY"}
TABLE_BLOCKSIZE=${9:-"256000000"}
TABLE_PARTITION_SPLIT=${10:-"250000000"}
export HIVE_AUX_JARS_PATH="$(echo -n $(ls -m $ROOT_DIR/lib/jar/dep/*.jar)|sed 's/, /:/g')"

if [ "$TABLE_COMPRESS" = "false" ]; then
  TABLE_CODEC="none"
fi

TABLE_NAME=cyclehire_processed_cleansed_rewrite_"$TABLE_FORMAT"_"$TABLE_CODEC"
TABLE_LOCATION="$ROOT_DIR_HDFS_PROCESSED"/cleansed/rewrite/"$TABLE_FORMAT"/"$TABLE_CODEC"
TABLE_DDL="$ROOT_DIR"/lib/ddl/processed_rewrite_"$TABLE_FORMAT".ddl
TABLE_LOCATION_CANONICAL="$ROOT_DIR_HDFS_PROCESSED"/cleansed/canonical/sequence/none

PARTITION_YEARS=()
PARTITION_MONTHS=()
if [ "$TABLE_PARTITION_YEAR" = "_REWRITE" ] && [ "$TABLE_PARTITION_MONTH" = "_REWRITE" ]; then
  for PATH_REWRITE in $($ROOT_DIR/../../bin/cyclehire-shell-hadoop.sh "jar $HADOOP_LIB_SEARCH org.apache.solr.hadoop.HdfsFindTool -find $TABLE_LOCATION -name _REWRITE 2> /dev/null"); do
       PARTITION_YEARS+=("$(basename $(dirname $(dirname $PATH_REWRITE))|cut -c 6-|tr -d '\n')")
       PARTITION_MONTHS+=("$(basename $(dirname $PATH_REWRITE)|cut -c 7-|tr -d '\n')")
  done
elif [ "$TABLE_PARTITION_YEAR" = "*" ] && [ "$TABLE_PARTITION_MONTH" = "*" ]; then
  for PATH_SUCCESS in $($ROOT_DIR/../../bin/cyclehire-shell-hadoop.sh "jar $HADOOP_LIB_SEARCH org.apache.solr.hadoop.HdfsFindTool -find $TABLE_LOCATION_CANONICAL -name _SUCCESS 2> /dev/null"); do
       PARTITION_YEARS+=("$(basename $(dirname $(dirname $PATH_SUCCESS))|cut -c 6-|tr -d '\n')")
       PARTITION_MONTHS+=("$(basename $(dirname $PATH_SUCCESS)|cut -c 7-|tr -d '\n')")
  done
else
  PARTITION_YEARS+=("$TABLE_PARTITION_YEAR")
  PARTITION_MONTHS+=("$TABLE_PARTITION_MONTH")
fi

for((i=0;i<${#PARTITION_YEARS[@]};i++)); do
  CMD_LINE_ARGUMENTS_PARTITION=\
" --hivevar cyclehire.table.name=$TABLE_NAME"\
" --hivevar cyclehire.table.location=$TABLE_LOCATION"\
" --hivevar cyclehire.table.codec=$TABLE_CODEC"\
" --hivevar cyclehire.table.partition.year=${PARTITION_YEARS[$i]}"\
" --hivevar cyclehire.table.partition.month=${PARTITION_MONTHS[$i]}"\
" --hiveconf hive.stats.autogather=false"\
" --hiveconf hive.exec.dynamic.partition.mode=nonstrict"\
" --hiveconf dfs.blocksize=$TABLE_BLOCKSIZE"\
" --hiveconf mapreduce.input.fileinputformat.split.minsize=$TABLE_PARTITION_SPLIT"
  if [ "$TABLE_FORMAT" = "parquet" ]; then
    CMD_LINE_ARGUMENTS_PARTITION="$CMD_LINE_ARGUMENTS_PARTITION"\
" --hiveconf parquet.block.size=$TABLE_PARTITION_SPLIT"\
" --hiveconf parquet.page.size=1048576"\
" --hiveconf parquet.enable.dictionary=true"\
" --hiveconf parquet.dictionary.page.size=1048576"
    if [ "$TABLE_COMPRESS" = "true" ]; then
      CMD_LINE_ARGUMENTS_PARTITION="$CMD_LINE_ARGUMENTS_PARTITION"\
" --hiveconf parquet.compression=$TABLE_CODEC_CLASS"
    else
      CMD_LINE_ARGUMENTS_PARTITION="$CMD_LINE_ARGUMENTS_PARTITION"\
" --hiveconf parquet.compression=UNCOMPRESSED"    
    fi
  else
    CMD_LINE_ARGUMENTS_PARTITION="$CMD_LINE_ARGUMENTS_PARTITION"\
" --hiveconf hive.exec.compress.output=$TABLE_COMPRESS"
    if [ "$TABLE_COMPRESS" = "true" ]; then
      CMD_LINE_ARGUMENTS_PARTITION="$CMD_LINE_ARGUMENTS_PARTITION"\
" --hiveconf mapreduce.map.output.compress.codec=$TABLE_CODEC_CLASS"\
" --hiveconf mapreduce.output.fileoutputformat.compress.type=BLOCK"
    fi
  fi
  if $ROOT_DIR/../../bin/cyclehire-shell-hive.sh -e "DESCRIBE $TABLE_NAME" 2> /dev/null; then
    TABLE_IMPALA_REFRESH_DDL="REFRESH $TABLE_NAME"
  else
    TABLE_IMPALA_REFRESH_DDL="INVALIDATE METADATA $TABLE_NAME"
  fi  
  $ROOT_DIR/../../bin/cyclehire-shell-hive.sh \
    $CMD_LINE_ARGUMENTS_PARTITION $CMD_LINE_ARGUMENTS -f $TABLE_DDL && \
    $ROOT_DIR/../../bin/cyclehire-shell-impala.sh -q "$TABLE_IMPALA_REFRESH_DDL" && \
    $ROOT_DIR/../../bin/cyclehire-shell-hadoop.sh "fs -rm -f $TABLE_LOCATION/year=${PARTITION_YEARS[$i]}/month=${PARTITION_MONTHS[$i]}/_REWRITE"
done
