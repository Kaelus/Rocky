#!/bin/sh

rocky_home=..
working_dir=..

test -d $working_dir/data && echo "data path is confirmed.."
if [ $? -gt 0 ]; then
    echo "ERROR. Check if ../data exists!"
fi

test -d $rocky_home/dynamoDB && echo "rocky home dynamodb path is confirmed.."
if [ $? -gt 0 ]; then
    echo "ERROR. Check if $rocky_home/dynamoDB exists!"
fi

mkdir -p $working_dir/data/0
java -Djava.library.path=$rocky_home/dynamoDB/DynamoDBLocal_lib -jar $rocky_home/dynamoDB/DynamoDBLocal.jar -dbPath $working_dir/data/0 -sharedDb
