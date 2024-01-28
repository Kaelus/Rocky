#!/bin/sh

rocky_home=..

if [ $# -lt 1 ]; then
    echo "usage: rocky_start.sh [<nbd_dev_name>]"
fi

test -f $rocky_home/build/libs/Rocky-all-1.0.jar && echo "rocky jar path is confirmed.."
if [ $? -gt 0 ]; then
    echo "ERROR. $rocky_home/build/libs/Rocky-all-1.0.jar does not exist!"
fi

if [ $# -eq 1 ]; then
    java -jar $rocky_home/build/libs/Rocky-all-1.0.jar rocky.ctrl.RockyController $1
else
    java -jar $rocky_home/build/libs/Rocky-all-1.0.jar rocky.ctrl.RockyController
fi
