# Overview

Rocky is a distributed replicated block device for tamper and failure resistant EdgeVDI. Rocky uses a replication broker to synchronize disk images across cloudlets, which is the edge datacenter sitting between endpoints and the cloud. Rocky replicates changes made to the disk images as a consistent immutable mutation history consisting of a totally-ordered write sequence. Therefore, it allows data recovery against tampering attacks such as ransomware or the wiper malware attacks or permanent failures such as fire, earthquake or disk worn out.

# Build Environment

We tested with the following versions of software:

1. Ubuntu 16.04

2. Java 8

3. Gradle 2.10

4. foundationdb-client_6.3.15-1_amd64, foundationdb-server_6.3.15-1_amd64

5. Apache Maven 3.3.9

# How to build

`gradle clean fatJar`

# Prerequisites

1. FoundationDB needs to be installed.
   - Follow the instruction at https://apple.github.io/foundationdb/getting-started-linux.html

2. ndb-client needs to be installed.
   - `sudo apt-get update`
   - `sudo apt-get install nbd nbd-client`
     - No for disconnecting all nbd-client devices.

3. Need to create a foundationdb volume in advance.
   - Clone the nbd on foudationdb, and go to the project home
     - `git clone https://github.com/spullara/nbd.git`
   - Need to update pom.xml before build:
     - Find the line for foundationdb
       - Fix it to direct to the correct repository by referring to https://mvnrepository.com/artifact/org.foundationdb/fdb-java/5.2.5
       - Then, build (This will create nbdcli.jar under the directory 'target') by `mvn package`
   - To create the volume, follow the instruction at https://github.com/spullara/nbd
     - `java -jar target/nbdcli.jar server`
     - `java -jar target/nbdcli.jar create -n testing -s 1G`
       - Note 'testing' can be replaced with any volume name
       - Also, note that nbdcli.jar has other commands to delete, list, etc. for the volumes
       - Finally, note that once you run RockyController, don't need to start spullara's server to use nbdcli.jar to manage volumes

# How to run

0. Setup a Connector-Cloudlet, a replication broker
   - We support two types of the backend: dynamoDBLocal and dynamoDBSeoul
     - if testing with dynamoDBLocal, download dynamoDB first and then do the following in the dynamodb home
       - `java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -sharedDb`
     - if using dynamoDBSeoul, one needs to appropriately setup the environment to use aws

1. Run Rocky Controller (NBD server)
   - `java -jar \`pwd\`/build/libs/Rocky-all-1.0.jar rocky.ctrl.RockyController`

2. Prepare the Rocky Block Device (nbd module & nbd client)
   - `sudo modprobe nbd`
   - `sudo lsmod | grep nbd`
   - `sudo nbd-client -N <volume name> localhost /dev/nbd0`
     - (testing is one of volume names)

To disconnect the Rocky Block Device from the Rocky server, `sudo nbd-client -d /dev/nbd0`

To remove Rocky Block Device module from the kernel, `sudo modprobe -r nbd`

## To Test

- `sudo mkfs.ext4 /dev/nbd0`
- `sudo mount /dev/nbd0 /tmp`
- `ls`
- `sudo umount /tmp`

# To Run multiple Rocky instances on a single host

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
- Run a Rocky instance with the correct configuration file path name.
  - `java -jar `pwd`/build/libs/rocky-code-all-1.0.jar rocky.ctrl.RockyController run/rocky.conf.1`
- Run nbd-client for the Rocky instance with correct parameters.
  - `sudo nbd-client -N testing1 localhost 10811 /dev/nbd1`

Likewise, suppose run/rocky.conf.2 sets port=10812 and lcvdName=testing2
Also, say /dev/nbd2 is the Rocky device driver instance to use.
- `java -jar \`pwd\`/build/libs/rocky-code-all-1.0.jar rocky.ctrl.RockyController run/rocky.conf.2`
- `sudo nbd-client -N testing2 localhost 10812 /dev/nbd2`

