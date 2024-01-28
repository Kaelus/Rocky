#!/bin/sh

if [ $# -lt 1 ]; then
    echo "usage: delete_fdb_volume.sh <volume_name>"
    exit 1
fi

echo "Volumes before deleting:"
java -jar ../nbdfdb/nbdcli.jar list
echo "Deleting the volume=$1"
java -jar ../nbdfdb/nbdcli.jar delete -n $1
echo "Volumes after deleting:"
java -jar ../nbdfdb/nbdcli.jar list
