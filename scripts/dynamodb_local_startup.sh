#!/bin/sh

java -Djava.library.path=../dynamoDB/DynamoDBLocal_lib -jar ../dynamoDB/DynamoDBLocal.jar -sharedDb
