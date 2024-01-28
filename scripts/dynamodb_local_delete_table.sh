#!/bin/sh

if [ $# -lt 1 ]; then
    echo "usage: dynamodb_local_delete_table.sh <table_name>"
    exit 1
fi

aws dynamodb delete-table --table-name $1 --endpoint-url http://localhost:8000
