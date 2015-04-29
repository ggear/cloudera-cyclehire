#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/cyclehire.env

impala-shell -d $IMPALA_IMPALAD_HOST -p $IMPALA_IMPALAD_PORT $*
