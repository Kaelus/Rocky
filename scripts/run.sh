#!/bin/sh

if [ $# -lt 1 ]; then
    echo "usage: run.sh <rocky_path> [<config_file>]"
    exit 1
fi

java -jar $1/build/libs/Rocky-all-1.0.jar rocky.ctrl.RockyController $2
