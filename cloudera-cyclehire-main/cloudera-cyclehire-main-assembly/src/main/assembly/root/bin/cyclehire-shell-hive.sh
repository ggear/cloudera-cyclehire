#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

source $ROOT_DIR/bin/cyclehire.env

beeline -u "jdbc:hive2://$HIVE_HIVESERVER2_HOST:$HIVE_HIVESERVER2_PORT/$NAME_SPACE;user=$USER_ADMIN" -n $USER_ADMIN $*