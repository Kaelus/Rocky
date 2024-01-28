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
cp ./*.sh $script_dir_dst/.

sed -i -e "3s|.*|rocky_home=$rocky_home|" $script_dir_dst/dynamodb_local_start.sh
sed -i -e "4s|.*|working_dir=$working_dir|" $script_dir_dst/dynamodb_local_start.sh

i=0
while [ $i -lt $num_node ]; do
    test -d $conf_dir/$i && rm -rf $conf_dir/$i
    mkdir -p $conf_dir/$i
    cp -rf $conf_template_dir/rocky_local.cfg-template $conf_dir/$i/rocky_local.cfg
    #cp -rf $scriptdir/zk_log.properties $conf_dir/$i/zk_log.properties

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


