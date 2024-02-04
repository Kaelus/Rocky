#!/bin/sh

rocky_home=..
working_dir=..

test -d $working_dir/data && echo "data path is confirmed.."
if [ $? -gt 0 ]; then
    echo "ERROR.$working_dir/data does not exist!"
    exit 1
fi

test -d $rocky_home/dynamoDB && echo "rocky home dynamodb path is confirmed.."
if [ $? -gt 0 ]; then
    echo "ERROR. $rocky_home/dynamoDB does not exist!"
    exit 1
fi

mkdir -p $working_dir/data/replication_broker
java -Djava.library.path=$rocky_home/dynamoDB/DynamoDBLocal_lib -jar $rocky_home/dynamoDB/DynamoDBLocal.jar -dbPath $working_dir/data/replication_broker -sharedDb
