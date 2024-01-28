#!/bin/sh

#if [ $# -lt 1 ]; then
#    echo "usage: stop_local.sh [<nbd_dev_name>]"
#    exit 1
#fi

./rocky_stop.sh 
./nbd_client_stop.sh $1
./dynamodb_local_stop.sh
