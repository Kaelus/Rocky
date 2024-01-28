#!/bin/sh

if [ $# -lt 2 ]; then
    echo "usage: create_fdb_volume.sh <volume_name> <volume_size>"
    exit 1
fi

java -jar ../nbdfdb/nbdcli.jar server
echo "Creating the volume=$1 of size=$2"
java -jar ../nbdfdb/nbdcli.jar create -n $1 -s $2
echo "Volumes after creating:"
java -jar ../nbdfdb/nbdcli.jar list
