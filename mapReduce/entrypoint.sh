#!/bin/bash

# jupyter lab --allow-root

# jupyter notebook --no-browser --allow-root --IdentityProvider.token='yourSecretToken' --ServerApp.allow_origin='*' --ServerApp.port=8888 --ServerApp.allow_remote_access=True --ServerApp.ip='0.0.0.0'

SPARK_WORKLOAD=$1

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ];
then
  start-master.sh -p 7077
elif [ "$SPARK_WORKLOAD" == "worker" ];
then
  start-worker.sh spark://spark-master:7077
elif [ "$SPARK_WORKLOAD" == "history" ]
then
  start-history-server.sh
fi