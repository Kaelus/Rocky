#!/bin/sh

if [ $# -lt 2 ]; then
    echo "usage: run_local.sh <volume_name> <nbd_dev_name> [<config_file>]"
    exit 1
fi

./fdb_create_volume.sh &
./dynamodb_local_start.sh &
{ sleep 1 ; ./nbd_client_start.sh $1 $2; } &
./rocky_start.sh $3
