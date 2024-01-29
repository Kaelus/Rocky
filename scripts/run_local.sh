#!/bin/sh

if [ $# -lt 3 ]; then
    echo "usage: run_local.sh <volume_name> <volume_size> <nbd_dev_name> [<config_file>]"
    exit 1
fi

sudo echo "starting run_local.sh"

./fdb_create_volume.sh $1 $2
if [ $? -gt 0 ]; then
    exit 1
fi
./dynamodb_local_start.sh &
{ sleep 1 ; ./nbd_client_start.sh $1 $3; } &
./rocky_start.sh $4
