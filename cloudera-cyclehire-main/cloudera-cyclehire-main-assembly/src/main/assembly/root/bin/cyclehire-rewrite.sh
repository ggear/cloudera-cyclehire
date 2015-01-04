#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/cyclehire.env

$ROOT_DIR/lib/process/bin/cyclehire-process-rewrite.sh \
	"" "$ROOT_DIR_HDFS_PROCESSED" "avro" "*" "*" \
	"false" "none" "" "134217728" "5000000000"
$ROOT_DIR/lib/process/bin/cyclehire-process-rewrite.sh \
	"" "$ROOT_DIR_HDFS_PROCESSED" "parquet" "*" "*" \
	"true" "snappy" "SNAPPY" "256000000" "250000000"
$ROOT_DIR/lib/process/bin/cyclehire-process-rewrite.sh \
	"" "$ROOT_DIR_HDFS_PROCESSED" "sequence" "*" "*" \
	"true" "snappy" "org.apache.hadoop.io.compress.SnappyCodec" "134217728" "5000000000"
