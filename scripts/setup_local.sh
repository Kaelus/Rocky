#!/bin/sh

if [ $# -lt 2 ]; then
    echo "usage: setup_local.sh <num_node> <working_dir>"
    exit 1
fi

echo "setting local variables to setup the working environment.."
num_node=$1
working_dir=$2
rocky_home=`cd .. && pwd`
conf_dir=$working_dir/conf
script_dir_src=`dirname $0`
script_dir_dst=$working_dir/scripts
conf_template_dir=`dirname $0`/../conf

echo "copying scripts to ${script_dir_dst}..."
test -d $script_dir_dst && rm -rf $script_dir_dst
mkdir -p $script_dir_dst
cp ./*.sh $script_dir_dst/.

echo "modifying scripts properly.."
sed -i -e "3s|.*|rocky_home=$rocky_home|" $script_dir_dst/dynamodb_local_start.sh
sed -i -e "4s|.*|working_dir=$working_dir|" $script_dir_dst/dynamodb_local_start.sh
sed -i -e "3s|.*|rocky_home=$rocky_home|" $script_dir_dst/fdb_create_volume.sh
sed -i -e "3s|.*|rocky_home=$rocky_home|" $script_dir_dst/rocky_start.sh
sed -i -e "3s|.*|rocky_home=$rocky_home|" $script_dir_dst/fdb_delete_volume.sh
sed -i -e "3s|.*|working_dir=$working_dir|" $script_dir_dst/clean_local.sh
sed -i -e "10s|.*|rocky_home=$rocky_home|" $script_dir_dst/run_local.sh
sed -i -e "3s|.*|working_dir=$working_dir|" $script_dir_dst/stop_local.sh

echo "updating configuration files for each node.."
i=0
while [ $i -lt $num_node ]; do
    echo "copying configuration template file for ${i}-th node to ${conf_dir}/${i}.."
    test -d $conf_dir/$i && rm -rf $conf_dir/$i
    mkdir -p $conf_dir/$i
    cp -rf $conf_template_dir/rocky_local.cfg-template $conf_dir/$i/rocky_local.cfg
    cp -rf $conf_template_dir/recover_local.cfg-template $conf_dir/$i/recover_local.cfg
    #cp -rf $scriptdir/zk_log.properties $conf_dir/$i/zk_log.properties

    echo "updating configuration file for ${i}-th node for networking.."
    client_ip=`expr 127.0.0.1`
    client_port=`expr 12300 + $i`
    sed -i -e "1s|.*|ip=$client_ip:$client_port|" $conf_dir/$i/rocky_local.cfg

    nbd_port=`expr 10810 + $i`
    sed -i -e "14s|.*|nbdPort=$nbd_port|" $conf_dir/$i/rocky_local.cfg
    
    sed -i -e "2s|.*|lcvdName=testinglocal${i}|" $conf_dir/$i/rocky_local.cfg
    sed -i -e "7s|.*|workingDir=${working_dir}|" $conf_dir/$i/rocky_local.cfg

    echo "calculating peerAddress for local testing.."
    peer_address=""
    j=0
    while [ $j -lt $num_node ]; do
	new_port=`expr 12300 + $j`
	if [ ! $client_port -eq $new_port ]; then
	    if [ "$peer_address" = "" ]; then
		peer_address="127.0.0.1:${new_port}"
	    else
		peer_address="${peer_address},127.0.0.1:${new_port}"
	    fi
	fi
	j=`expr $j + 1`
    done
    sed -i -e "13s|.*|peerAddress=${peer_address}|" $conf_dir/$i/rocky_local.cfg

    echo "check if volume (i.e. lcvdName) exists.."
    java -jar $rocky_home/nbdfdb/nbdcli.jar list | grep testinglocal${i}
    if [ $? -gt 0 ]; then
	echo "ASSERT: The volume name testinglocal${i} does not exist yet."
	echo "Create the volume testinglocal${i} first by using fdb_create_volume.sh"
	exit 1
    fi

    echo "removing log or data directory for ${i}-th node which is from the previous run.."
    test -d $working_dir/log/$i && rm -rf $working_dir/log/$i
    mkdir -p $working_dir/log/$i

    test -d $working_dir/data/$i && rm -rf $working_dir/data/$i
    mkdir -p $working_dir/data/$i

    #sed -i -e "12s|.*|dataDir=$working_dir/data/$i|" $conf_dir/$i/zoo.cfg

    i=`expr $i + 1`
done

echo "Setup is done now. Have fun!"
