#!/bin/sh

kill $(jps | grep 'Rocky.*.jar' | awk '{print $1}')

