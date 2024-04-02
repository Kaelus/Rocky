#!/bin/sh

if [ $# -lt 1 ]; then
    echo "usage: recover_local.sh <config_file>"
    exit 1
fi

echo "starting recover_local.sh"

./dynamodb_local_start.sh &
java -jar /home/ben/project/rocky/Rocky/build/libs/Rocky-all-1.0.jar rocky.ctrl.RockyController $1
