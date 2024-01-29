#!/bin/sh

working_dir=..

if [ $# -lt 1 ]; then
    echo "usage: clean_local.sh <volume_name>"
    exit 1
fi

sudo echo "clean_local.sh starts"
./fdb_delete_volume.sh $1
rm -f $working_dir/data/0/shared-local-instance.db
rm -rf $working_dir/data/0/testing-*
sudo modprobe -r nbd
