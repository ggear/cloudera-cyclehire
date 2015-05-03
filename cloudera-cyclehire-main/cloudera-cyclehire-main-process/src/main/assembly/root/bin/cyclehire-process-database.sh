#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

[ -f $ROOT_DIR/../../bin/cyclehire.env ] && source $ROOT_DIR/../../bin/cyclehire.env

set -x

CMD_LINE_ARGUMENTS="$1"
USER_ADMIN=${2:-"$USER_ADMIN"}
NAME_SPACE_SERVER=${3:-"$NAME_SPACE_SERVER"}
NAME_SPACE_DATABASE=${4:-"$NAME_SPACE_DATABASE"}
ROOT_DIR_HDFS=${5:-"$ROOT_DIR_HDFS"}

if [ $($ROOT_DIR/../../bin/cyclehire-shell-hive.sh -e "SHOW ROLES" 2> /dev/null| grep $USER_ADMIN|wc -l) -eq 0 ]; then
  $ROOT_DIR/../../bin/cyclehire-shell-hive.sh "\
    --hivevar cyclehire.user=$USER_ADMIN \
    --hivevar cyclehire.server.name=$NAME_SPACE_SERVER \
    --hivevar cyclehire.database.name=$NAME_SPACE_DATABASE \
    --hivevar cyclehire.database.location=$ROOT_DIR_HDFS" \
    -f "$ROOT_DIR/lib/ddl/database_grant.ddl"
  $ROOT_DIR/../../bin/cyclehire-shell-hive.sh "\
    --hivevar cyclehire.user=$USER_ADMIN \
    --hivevar cyclehire.server.name=$NAME_SPACE_SERVER \
    --hivevar cyclehire.database.name=$NAME_SPACE_DATABASE \
    --hivevar cyclehire.database.location=$ROOT_DIR_HDFS" \
    -f "$ROOT_DIR/lib/ddl/database_create.ddl"
fi