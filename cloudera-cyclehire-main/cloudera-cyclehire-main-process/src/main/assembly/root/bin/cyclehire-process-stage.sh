#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

[ -f $ROOT_DIR/../../bin/cyclehire.env ] && source $ROOT_DIR/../../bin/cyclehire.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_RAW_LANDED=${2:-"$ROOT_DIR_HDFS_RAW_LANDED"}
ROOT_DIR_HDFS_RAW_STAGED=${3:-"$ROOT_DIR_HDFS_RAW_STAGED"}

hadoop \
	jar "$ROOT_DIR/lib/jar/cloudera-cyclehire-main-process-*.jar" \
	-libjars "$(echo -n $(ls -m $ROOT_DIR/lib/jar/dep/*.jar)|sed 's/, /,/g')" \
	com.cloudera.cyclehire.main.process.clean.StageDriver \
	$CMD_LINE_ARGUMENTS \
	"$ROOT_DIR_HDFS_RAW_LANDED" \
	"$ROOT_DIR_HDFS_RAW_STAGED"
