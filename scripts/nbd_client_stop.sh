#!/bin/sh

#if [ $# -lt 1 ]; then
#    echo "usage: nbd_client_stop.sh [<nbd_dev_name>]"
#fi

if [ $# -eq 1 ]; then
    sudo nbd-client -d /dev/${1}
else
    sudo nbd-client -d /dev/nbd0
fi
#sudo modprobe -r nbd
