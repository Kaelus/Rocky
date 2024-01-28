#!/bin/sh

if [ $# -lt 1 ]; then
    echo "usage: nbd_client_stop.sh [<nbd_dev_name>]"
fi

if [ $# -eq 1 ]; then
    java -jar ../build/libs/Rocky-all-1.0.jar rocky.ctrl.RockyController $1
else
    java -jar ../build/libs/Rocky-all-1.0.jar rocky.ctrl.RockyController
fi

