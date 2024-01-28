#!/bin/sh

if [ $# -lt 2 ]; then
    echo "usage: nbd_client_start.sh <volume_name> <nbd_dev_name>"
    exit 1
fi

sudo modprobe nbd
sudo lsmod | grep nbd
sudo nbd-client -g -N $1 localhost /dev/$2
