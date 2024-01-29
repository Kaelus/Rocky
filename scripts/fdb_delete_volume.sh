#!/bin/sh

rocky_home=..

if [ $# -lt 1 ]; then
    echo "usage: fdb_delete_volume.sh <volume_name>"
    exit 1
fi

echo "Checking if the volume $1 exists before deleting:"
java -jar $rocky_home/nbdfdb/nbdcli.jar list | grep $1
if [ $? -gt 0 ]; then
    echo "There is no Volume=$1 existing!"
else
    echo "Deleting the volume=$1"
    java -jar $rocky_home/nbdfdb/nbdcli.jar delete -n $1
fi
