#!/bin/sh

if [ $# -lt 1 ]; then
    echo "usage: run_local.sh <config_file>"
    exit 1
fi

sudo echo "starting run_local.sh"

rocky_home=
volume_name=`grep lcvdName $1 | cut -d '=' -f 2`
nbd_port=`grep nbdPort $1 | cut -d '=' -f 2`
nbd_dev_name="nbd"`expr $((nbd_port)) - 10810`

echo "rocky_home=$rocky_home"
echo "volume_name=$volume_name"
echo "nbd_port=$nbd_port"
echo "nbd_dev_name=$nbd_dev_name"
echo

echo "checking if ${volume_name} exists.."
java -jar $rocky_home/nbdfdb/nbdcli.jar list | grep $volume_name
if [ $? -gt 0 ]; then
    echo "ASSERT: The volume name $volume_name does not exist yet."
    echo "Create the volume first by using fdb_create_volume.sh"
    exit 1
fi
echo

echo "checking if dynamodb is running.."
is_dynamodb_running=`jps | grep DynamoDBLocal.jar`
if [ ! $? -eq 0 ]; then
    echo "dynamodb is not running locally."
    echo "starting dynamodb.."
    ./dynamodb_local_start.sh &
fi

echo "starting nbd client with 1 sec delay to start up rocky first.."
{ sleep 1 ; ./nbd_client_start.sh $volume_name $nbd_port $nbd_dev_name; } &

echo "starting rocky.."
./rocky_start.sh $1
