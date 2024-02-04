#!/bin/sh

working_dir=..

#if [ $# -lt 1 ]; then
#    echo "usage: stop_local.sh [<nbd_dev_name>]"
#    exit 1
#fi

echo "stopping rocky.."
./rocky_stop.sh

num_node=`ls ${working_dir}/conf | wc -l`
i=0
while [ $i -lt $num_node ]; do
    echo "stopping nbd${i}.."
    ./nbd_client_stop.sh nbd${i}
    i=`expr $i + 1`
done

echo "stopping dynamodb.."
./dynamodb_local_stop.sh

echo "stopping is done."
