#!/bin/sh

if [ $# -lt 3 ]; then
    echo "usage: nbd_client_start.sh <volume_name> <nbd_port> <nbd_dev_name>"
    exit 1
fi

sudo modprobe nbd
sudo lsmod | grep nbd
sudo nbd-client -g -N $1 localhost $2 /dev/$3
