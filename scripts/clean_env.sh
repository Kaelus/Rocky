#!/bin/sh
kill $(jps | grep 'Rocky.*.jar' | awk '{print $1}')
sudo nbd-client -d /dev/${1}
