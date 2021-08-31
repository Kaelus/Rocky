#!/bin/bash


#BLKSIZE=512

echo "number of arguments given=$#"

IOTYPE=$1
if [[ $2 == '' ]]; then
    NUMITER=4
elif [[ $# -gt 1 ]]; then
    NUMITER=$2
fi
echo "Number of Iteration given=$NUMITER"


#DEFAULT_SEEKSKIP=16

#echo "echo 3 | sudo tee /proc/sys/vm/drop_caches"
echo "echo 3 > /proc/sys/vm/drop_caches"
echo 3 > /proc/sys/vm/drop_caches

for i in $(seq 1 $NUMITER)
do
    echo "Iteration $i"
    echo "Given iotype=$IOTYPE"
    if [[ $IOTYPE == "write" ]]; then
	#SEEKOFFSET=$(($DEFAULT_SEEKSKIP+($i-1)*4))
	SEEKOFFSET=$((($i-1)*4))
	echo "seek to ($SEEKOFFSET * 2)-th blocks in nbd0"
	echo "dd if=/dev/zero of=/dev/nbd0 bs=1024 count=4 seek=$SEEKOFFSET oflag=direct"
	dd if=/dev/zero of=/dev/nbd0 bs=1024 count=4 seek=$SEEKOFFSET oflag=direct
	wait
    elif [[ $IOTYPE == "read" ]]; then
	#SKIPOFFSET=$(($DEFAULT_SEEKSKIP+($i-1)*4))
	SKIPOFFSET=$((($i-1)*4))
	echo "skip to ($SKIPOFFSET * 2)-th blocks in nbd0"
	echo "dd if=/dev/nbd0 of=/dev/null bs=1024 count=4 skip=$SKIPOFFSET iflag=nocache"
	dd if=/dev/nbd0 of=/dev/null bs=1024 count=4 skip=$SKIPOFFSET iflag=nocache
	wait
    else
	echo "Unknown iotype given=$IOTYPE"
    fi
done

if [[ $IOTYPE == "write" ]]; then
    NUMBLOCKS=$(($NUMITER*2*4))
    echo "$NUMBLOCKS blocks are written"
elif [[ $IOTYPE == "read" ]]; then
    NUMBLOCKS=$(($NUMITER*2*4))
    echo "$NUMBLOCKS blocks are read"
else
    echo "0 block is written"
fi
