[Overview]

Rocky provides an block device that replicates blocks on end devices via a cloud service. Rocky does not fully rely on the cloud service. If the cloud service becomes unavailable or the user decides to switch to a different service provider, Rocky can allow the user to restore the full disk image and migrate to another service only with end devices. In a case some blocks are lost and irrecoverable due to the unexpected cloud service failure, Rocky allows the user to still restore the disk image that is conherent but the latest version before the lost blocks are written.  

[How to build]

$ gradle clean fatJar

[Prerequisites]
0. OS: the followings are tested on Ubuntu 16.04

1. FoundationDB needs to be installed.
Follow the instruction at https://apple.github.io/foundationdb/getting-started-linux.html

2. ndb-client needs to be installed.
$ sudo apt-get update
$ sudo apt-get install nbd nbd-client

[How to run]

1. Run Rocky Controller (NBD server)
$ java -jar `pwd`/build/libs/rocky-code-all-1.0.jar rocky.ctrl.RockyController

2. Prepare the Rocky Block Device (nbd module & nbd client)
$ sudo modprobe nbd
$ sudo lsmod | grep nbd
$ sudo nbd-client -N <name> localhost /dev/nbd0

To disconnect the Rocky Block Device from the Rocky server,
$ sudo nbd-client -d

To remove Rocky Block Device module from the kernel,
$ sudo modprobe -r nbd


[To Test]
$ sudo mkfs.ext4 /dev/nbd0
$ sudo mount /dev/nbd0 /tmp
$ ls
$ sudo umount /tmp


