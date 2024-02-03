#!/bin/sh

if [ $# -lt 2 ]; then
    echo "usage: setup_local.sh <num_node> <working_dir>"
    exit 1
fi

num_node=$1
working_dir=$2
rocky_home=`cd .. && pwd`
conf_dir=$working_dir/conf
script_dir_src=`dirname $0`
script_dir_dst=$working_dir/scripts
conf_template_dir=`dirname $0`/../conf

test -d $script_dir_dst && rm -rf $script_dir_dst
mkdir -p $script_dir_dst
cp ./dynamodb_local_start.sh $script_dir_dst/.
cp ./dynamodb_local_stop.sh $script_dir_dst/.
cp ./fdb_create_volume.sh $script_dir_dst/.
cp ./nbd_client_start.sh $script_dir_dst/.
cp ./nbd_client_stop.sh $script_dir_dst/.
cp ./rocky_start.sh $script_dir_dst/.
cp ./rocky_stop.sh $script_dir_dst/.
cp ./run_local.sh $script_dir_dst/.
cp ./stop_local.sh $script_dir_dst/.
cp ./fdb_delete_volume.sh $script_dir_dst/.
cp ./clean_local.sh $script_dir_dst/.
cp ./recover_local.sh $script_dir_dst/.
cp ./stop_workload.sh $script_dir_dst/.

sed -i -e "3s|.*|rocky_home=$rocky_home|" $script_dir_dst/dynamodb_local_start.sh
sed -i -e "4s|.*|working_dir=$working_dir|" $script_dir_dst/dynamodb_local_start.sh

sed -i -e "3s|.*|rocky_home=$rocky_home|" $script_dir_dst/fdb_create_volume.sh

sed -i -e "3s|.*|rocky_home=$rocky_home|" $script_dir_dst/rocky_start.sh

sed -i -e "3s|.*|rocky_home=$rocky_home|" $script_dir_dst/fdb_delete_volume.sh

sed -i -e "3s|.*|working_dir=$working_dir|" $script_dir_dst/clean_local.sh

i=0
while [ $i -lt $num_node ]; do
    test -d $conf_dir/$i && rm -rf $conf_dir/$i
    mkdir -p $conf_dir/$i
    cp -rf $conf_template_dir/rocky_local.cfg-template $conf_dir/$i/rocky_local.cfg
    cp -rf $conf_template_dir/recover_local.cfg-template $conf_dir/$i/recover_local.cfg
    #cp -rf $scriptdir/zk_log.properties $conf_dir/$i/zk_log.properties

    sed -i -e "7s|.*|workingDir=$working_dir|" $conf_dir/$i/rocky_local.cfg

    test -d $working_dir/log/$i && rm -rf $working_dir/log/$i
    mkdir -p $working_dir/log/$i

    test -d $working_dir/data/$i && rm -rf $working_dir/data/$i
    mkdir -p $working_dir/data/$i

    #sed -i -e "12s|.*|dataDir=$working_dir/data/$i|" $conf_dir/$i/zoo.cfg

    client_ip=`expr 127.0.0.1`
    client_port=`expr 10810 + $i`
    sed -i -e "1s|.*|ip=$client_ip:$client_port|" $conf_dir/$i/rocky_local.cfg

    i=`expr $i + 1`
done


