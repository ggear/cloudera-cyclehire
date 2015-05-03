#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

[ -f $ROOT_DIR/../../bin/cyclehire.env ] && source $ROOT_DIR/../../bin/cyclehire.env

set -x

CMD_LINE_ARGUMENTS="$1"
ROOT_DIR_HDFS_RAW_PARTITIONED=${2:-"$ROOT_DIR_HDFS_RAW_PARTITIONED"}
ROOT_DIR_HDFS_PROCESSED=${3:-"$ROOT_DIR_HDFS_PROCESSED"}
export HIVE_AUX_JARS_PATH="$(echo -n $(ls -m $ROOT_DIR/lib/jar/dep/*.jar)|sed 's/, /:/g')"

TABLES_REAPIR=""
TABLES_NAME=("cyclehire_raw_partitioned_valid" "cyclehire_raw_partitioned_invalid" "cyclehire_processed_cleansed_canonical" "cyclehire_processed_erroneous_duplicate" "cyclehire_processed_erroneous_malformed")
TABLES_LOCATION=("$ROOT_DIR_HDFS_RAW_PARTITIONED/valid" "$ROOT_DIR_HDFS_RAW_PARTITIONED/invalid" "$ROOT_DIR_HDFS_PROCESSED/cleansed/canonical" "$ROOT_DIR_HDFS_PROCESSED/erroneous/duplicate" "$ROOT_DIR_HDFS_PROCESSED/erroneous/malformed")
TABLES_DDL=("partitioned_create.ddl" "partitioned_create.ddl" "processed_create.ddl" "processed_create.ddl" "processed_create.ddl")

for((i=0;i<${#TABLES_NAME[@]};i++)); do
  TABLES_REAPIR="$TABLES_REAPIR""MSCK REPAIR TABLE ${TABLES_NAME[$i]};"
done

if [ $(hive -e "$TABLES_REAPIR" | grep "Tables not in metastore" | wc -l) -gt 0 ]; then
  for((i=0;i<${#TABLES_NAME[@]};i++)); do
    $ROOT_DIR/../../bin/cyclehire-shell-hive.sh "\
      --hivevar cyclehire.table.name=${TABLES_NAME[$i]} \
      --hivevar cyclehire.table.location=${TABLES_LOCATION[$i]}/sequence/none \
      -f "$ROOT_DIR/lib/ddl/${TABLES_DDL[$i]}"
  done
fi