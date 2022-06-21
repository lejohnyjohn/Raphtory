#!/bin/bash
component=$1

#set -eux
#
#declare HOST="$RAPHTORY_PULSAR_ADMIN_ADDRESS/admin/v2/worker/cluster"
#declare STATUS=200
#declare TIMEOUT=1000
#
#HOST=$HOST STATUS=$STATUS timeout --foreground -s TERM $TIMEOUT bash -c \
#    'while [[ ${STATUS_RECEIVED} != ${STATUS} ]];\
#        do STATUS_RECEIVED=$(curl -s -o /dev/null -L -w ''%{http_code}'' ${HOST}) && \
#        echo "received status: $STATUS_RECEIVED" && \
#        sleep 1;\
#    done;
#    echo success with status: $STATUS_RECEIVED'

if [ $1 = "client" ]
then
  java -cp example-lotr-assembly-0.5.jar com.raphtory.examples.lotrTopic.LOTRClient
else
  java -cp example-lotr-assembly-0.5.jar com.raphtory.examples.lotrTopic.LOTRService $component
fi
