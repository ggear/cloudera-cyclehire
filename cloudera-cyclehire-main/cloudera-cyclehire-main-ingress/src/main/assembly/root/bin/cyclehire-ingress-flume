#!/bin/bash

export ROOT_DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/..

[ -f $ROOT_DIR/../../bin/cyclehire.env ] && source $ROOT_DIR/../../bin/cyclehire.env

set -x

CMD_LINE_ARGUMENTS="$1"
CLUSTER_NODE_USER=${2:-"$CLUSTER_NODE_USER"}
CLUSTER_NODE_KEY=${3:-"$CLUSTER_NODE_KEY"}
MANAGER_SERVER_USER=${4:-"admin"}
MANAGER_SERVER_PWORD=${5:-"admin"}
MANAGER_SERVER_HOST=${6:-"MANAGER_SERVER_HOST"}
MANAGER_SERVER_PORT=${7:-"MANAGER_SERVER_PORT"}
FLUME_AGENT_HOSTS=${8:-"$FLUME_AGENT_HOSTS"}
FLUME_AGENT_DIR_LIB=${9:-"/usr/lib/flume-ng/plugins.d"}

FLUME_AGENT_HOSTS_ARRAY=(${FLUME_AGENT_HOSTS//,/ })
for FLUME_AGENT_HOST in "${FLUME_AGENT_HOSTS_ARRAY[@]}"; do
  ssh -i $CLUSTER_NODE_KEY $CLUSTER_NODE_USER@$FLUME_AGENT_HOST "mkdir -p $FLUME_AGENT_DIR_LIB"
  scp -i $CLUSTER_NODE_KEY $ROOT_DIR/lib/jar/*.jar $ROOT_DIR/lib/jar/dep/*.jar $CLUSTER_NODE_USER@$FLUME_AGENT_HOST:$FLUME_AGENT_DIR_LIB
done

FLUME_AGENT_CONFIG=$(cat $ROOT_DIR/lib/cfg/flume-conf.properties)

python - "$MANAGER_SERVER_USER" "$MANAGER_SERVER_PWORD" "$MANAGER_SERVER_HOST" "$MANAGER_SERVER_PORT" "$FLUME_AGENT_CONFIG" << END

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
            group.update_config({'agent_config_file': argv[5]})
        print 'Restarting flume service ...'
        service.restart().wait()
  return 0

if __name__ == '__main__':
  sys.exit(main(sys.argv))

END
