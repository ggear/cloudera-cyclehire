#!/bin/bash

export CYCLEHIRE_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $CYCLEHIRE_DIR/bin/cyclehire.env

set -x

$CYCLEHIRE_DIR/lib/process/bin/process.sh $CYCLEHIRE_DIR_HDFS_RAW_LANDED $CYCLEHIRE_DIR_HDFS_RAW_STAGED $CYCLEHIRE_DIR_HDFS_RAW_PARTITIONED $CYCLEHIRE_DIR_HDFS_PROCESSED
