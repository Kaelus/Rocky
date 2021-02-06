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

3. Need to create a foundationdb volume in advance.

Clone the nbd on foudationdb, and go to the project home
$ git clone https://github.com/spullara/nbd.git

Need to update pom.xml before build:
- Find the line for foundationdb
- Fix it to direct to the correct repository by referring:
  https://mvnrepository.com/artifact/org.foundationdb/fdb-java/5.2.5

Then, build (This will create nbdcli.jar under the directory 'target')
$ mvn package

To create the volume, follow the instruction at (https://github.com/spullara/nbd)
$ java -jar nbdcli.jar server
$ java -jar nbdcli.jar create -n testing -s 1G
(Note 'testing' can be replaced with any volume name)
(Also, note that nbdcli.jar has other commands to delete, list, etc. for the volumes)
(Finally, note that once you run RockyController, don't need to start spullara's server to use nbdcli.jar to manage volumes)

[How to run]

1. Run Rocky Controller (NBD server)
$ java -jar `pwd`/build/libs/rocky-code-all-1.0.jar rocky.ctrl.RockyController

2. Prepare the Rocky Block Device (nbd module & nbd client)
$ sudo modprobe nbd
$ sudo lsmod | grep nbd
$ sudo nbd-client -N <volume name> localhost /dev/nbd0
(testing is one of volume names)

To disconnect the Rocky Block Device from the Rocky server,
$ sudo nbd-client -d /dev/nbd0

To remove Rocky Block Device module from the kernel,
$ sudo modprobe -r nbd

[To Test]
$ sudo mkfs.ext4 /dev/nbd0
$ sudo mount /dev/nbd0 /tmp
$ ls
$ sudo umount /tmp

[To Run multiple Rocky instances on a single host]
In the directory 'conf', there is an example rocky.conf configuration file.
Use it at your discretion after setting port and lcvdName accordingly.
Those configuration parameters should be assigned with a unique value for
each rocky instance.

It's good idea to copy and paste the conf/rocky.conf in another directory
for each Rocky instance to run. For instance, we may have two files under
the directory run: run/rocky.conf.1 and run/rocky.conf.2
We should modify those configuration files accorinngly.

run/rocky.conf.1 sets port=10811 and lcvdName=testing1 and the first Rocky
instance will use /dev/nbd1 as the Rocky device driver.
Then, execute following commands:
# Run a Rocky instance with the correct configuration file path name.
$ java -jar `pwd`/build/libs/rocky-code-all-1.0.jar rocky.ctrl.RockyController run/rocky.conf.1
# Run nbd-client for the Rocky instance with correct parameters.
$ sudo nbd-client -N testing1 localhost 10811 /dev/nbd1

Likewise, suppose run/rocky.conf.2 sets port=10812 and lcvdName=testing2
Also, say /dev/nbd2 is the Rocky device driver instance to use.
$ java -jar `pwd`/build/libs/rocky-code-all-1.0.jar rocky.ctrl.RockyController run/rocky.conf.2
$ sudo nbd-client -N testing2 localhost 10812 /dev/nbd2

