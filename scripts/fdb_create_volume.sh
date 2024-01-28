#!/bin/sh

rocky_home=..

if [ $# -lt 2 ]; then
    echo "usage: fdb_create_volume.sh <volume_name> <volume_size>"
    exit 1
fi

#java -jar ../nbdfdb/nbdcli.jar server &
java -jar $rocky_home/nbdfdb/nbdcli.jar list | grep $1
if [ $? -gt 0 ]; then
    echo "Creating the volume=$1 of size=$2.."
    java -jar $rocky_home/nbdfdb/nbdcli.jar create -n $1 -s $2
else
    echo "The volume $1 already exists!"
fi
#echo "Done. Stopping the fdb server.."
#kill $(jps | grep 'nbdcli.jar' | awk '{print $1}')
