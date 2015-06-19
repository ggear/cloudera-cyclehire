#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

[ -f $ROOT_DIR/../../bin/cyclehire.env ] && source $ROOT_DIR/../../bin/cyclehire.env

set -x

CMD_LINE_ARGUMENTS="$1"
CLUSTER_NODE_USER=${2:-"$CLUSTER_NODE_USER"}
CLUSTER_NODE_KEY=${3:-"$CLUSTER_NODE_KEY"}
MANAGER_SERVER_USER=${4:-"admin"}
MANAGER_SERVER_PWORD=${5:-"admin"}
MANAGER_SERVER_HOST=${6:-"$MANAGER_SERVER_HOST"}
MANAGER_SERVER_PORT=${7:-"$MANAGER_SERVER_PORT"}
HDFS_NAMENODE_HOST=${8:-"$HDFS_NAMENODE_HOST"}
FLUME_AGENT_HOSTS=${9:-"$FLUME_AGENT_HOSTS"}
FLUME_AGENT_DIR_LIB=${10:-"/usr/lib/flume-ng/plugins.d/cyclehire"}
FLUME_AGENT_NAME=${11:-"agent"}
ROOT_DIR_HDFS_RAW_LANDED_XML=${12:-"$ROOT_DIR_HDFS_RAW_LANDED_XML"}

DIR=$ROOT_DIR_HDFS_RAW_LANDED_XML
while [ $DIR != "/" ]; do
  $ROOT_DIR/../../bin/cyclehire-shell-hadoop.sh "fs -mkdir -p $DIR"
  $ROOT_DIR/../../bin/cyclehire-shell-hadoop.sh "fs -chmod 755 $DIR"
  DIR=$(dirname $DIR)
done
$ROOT_DIR/../../bin/cyclehire-shell-hadoop.sh "fs -chmod 777 $ROOT_DIR_HDFS_RAW_LANDED_XML"

FLUME_AGENT_HOSTS_ARRAY=(${FLUME_AGENT_HOSTS//,/ })
for FLUME_AGENT_HOST in "${FLUME_AGENT_HOSTS_ARRAY[@]}"; do
  ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY $CLUSTER_NODE_USER@$FLUME_AGENT_HOST "mkdir -p $FLUME_AGENT_DIR_LIB/lib"
  scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY $ROOT_DIR/lib/jar/*.jar $CLUSTER_NODE_USER@$FLUME_AGENT_HOST:$FLUME_AGENT_DIR_LIB/lib
  ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY $CLUSTER_NODE_USER@$FLUME_AGENT_HOST "mkdir -p $FLUME_AGENT_DIR_LIB/libext"
  scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i $CLUSTER_NODE_KEY $ROOT_DIR/lib/jar/dep/*.jar $CLUSTER_NODE_USER@$FLUME_AGENT_HOST:$FLUME_AGENT_DIR_LIB/libext
done
 
FLUME_AGENT_CONFIG=$(cat ./cloudera-cyclehire-main/cloudera-cyclehire-main-ingress/src/main/resources/flume/flume-conf.properties | sed -e "s/\$HDFS_NAMENODE_HOST/$HDFS_NAMENODE_HOST/g" | sed -e "s|\$ROOT_DIR_HDFS_RAW_LANDED_XML|"$ROOT_DIR_HDFS_RAW_LANDED_XML"|g")

python - "$MANAGER_SERVER_USER" "$MANAGER_SERVER_PWORD" "$MANAGER_SERVER_HOST" "$MANAGER_SERVER_PORT" "$FLUME_AGENT_NAME" "$FLUME_AGENT_CONFIG" << END

import sys
from cm_api import api_client
from cm_api.api_client import ApiResource

def main(argv):
  print ''
  api = ApiResource(argv[3], argv[4], argv[1], argv[2], False, 10);
  for cluster in api.get_all_clusters():
    for service in cluster.get_all_services():
      if service.type == 'FLUME':
        for group in service.get_all_role_config_groups():
          if group.roleType == 'AGENT':
            print 'Updating flume config ...'
            group.update_config({'agent_name': argv[5]})
            group.update_config({'agent_config_file': argv[6]})
        print 'Restarting flume service ...'
        service.restart().wait()
  return 0

if __name__ == '__main__':
  sys.exit(main(sys.argv))

END
