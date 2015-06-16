#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

[ -f $ROOT_DIR/../../bin/cyclehire.env ] && source $ROOT_DIR/../../bin/cyclehire.env

set -x

CMD_LINE_ARGUMENTS="$1"
USER_ADMIN=${2:-"$USER_ADMIN"}
NAME_SPACE_SERVER=${3:-"$NAME_SPACE_SERVER"}
NAME_SPACE_DATABASE=${4:-"$NAME_SPACE_DATABASE"}
ROOT_DIR_HDFS=${5:-"$ROOT_DIR_HDFS"}

if [ $($ROOT_DIR/../../bin/cyclehire-shell-impala.sh -q "SHOW ROLES" 2> /dev/null| grep $USER_ADMIN|wc -l) -ne 0 ]; then
  $ROOT_DIR/../../bin/cyclehire-shell-hive.sh --outputformat=vertical -e "SHOW TABLES" | grep tab_name | awk '{print $2}' | xargs -I £ $ROOT_DIR/../../bin/cyclehire-shell-hive.sh -e "DROP TABLE £"
  $ROOT_DIR/../../bin/cyclehire-shell-impala.sh -r -q "USE default; DROP DATABASE $NAME_SPACE_DATABASE; DROP ROLE $USER_ADMIN;"
fi
