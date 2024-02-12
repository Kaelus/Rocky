#!/bin/sh

working_dir=..

#if [ $# -lt 1 ]; then
#    echo "usage: clean_local.sh <volume_name>"
#    exit 1
#fi

sudo echo "clean_local.sh starts"

echo "removing dynamodb data.."
#rm -f $working_dir/data/replication_broker/shared-local-instance.db
rm -rf $working_dir/data/replication_broker
mkdir -p $working_dir/data/replication_broker

num_node=`ls ${working_dir}/conf | wc -l`
i=0
while [ $i -lt $num_node ]; do
    volume_name=`grep lcvdName ${working_dir}/conf/${i}/rocky_local.cfg | cut -d '=' -f 2`
    echo "removing fdb volume data.."
    ./fdb_delete_volume.sh $volume_name
    echo "removing leveldb data.."
    #rm -rf $working_dir/data/$i/testing-*
    rm -rf $working_dir/data/$i
    mkdir -p $working_dir/data/$i
    echo "removing log data.."
    rm -rf $working_dir/log/$i
    mkdir -p $working_dir/log/$i
    i=`expr $i + 1`
done

sudo modprobe -r nbd
