#!/bin/sh

kill $(jps | grep 'DynamoDBLocal.jar' | awk '{print $1}')

