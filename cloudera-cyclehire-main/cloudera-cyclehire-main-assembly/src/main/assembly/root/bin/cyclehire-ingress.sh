#!/bin/bash

export CYCLEHIRE_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $CYCLEHIRE_DIR/bin/cyclehire.env

set -x

$CYCLEHIRE_DIR/lib/ingest/bin/ingress-client.sh $CYCLEHIRE_DIR $CYCLEHIRE_DIR_LOCAL_TAR $CYCLEHIRE_DIR_HDFS_ORIGINAL_TAR

