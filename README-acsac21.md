# ACSAC21 Artifact for Rocky

## Title

Rocky: Replicating Block Devices for Tamper and Failure Resistant Edge-based Virtualized Desktop Infrastructure

## Authors

Beom Heyn Kim \<beomheyn.kim@gmail.com\>

Hyoungshick Kim \<hyoung@skku.edu\>

## Abstract

This includes both source code of the prototype Rocky and evaluation data.

1. Software
   - Rocky Controller source code is included
   - Other components of Rocky Endpoint are included as executable or .deb
   - Additionally, DynamoDB is included which is used as a Connector-Cloudlet (replication broker)

2. Data
   - Throughput measurement result (section 5.2) is in 363 bytes large file eval/throughput.csv which is in the comma separated value format
     - The first column contains the percentages of blocks present locally
     - The second and the third columns shows write throughput in KB/s to compare between the baseline NBD and Rocky implementation
     - The fourth and the fifth columns shows read throughput in KB/s to compare between the baseline NBD and Rocky implementation
   - A 4.1MB high-resolution .jpg file for the reduction rate measurement (section 5.3)

See the 'Build Environment' section below for dependencies and requirements.


# Overview

Rocky is a distributed replicated block device for tamper and failure resistant EdgeVDI. Rocky uses a replication broker to synchronize disk images across cloudlets, which is the edge datacenter sitting between endpoints and the cloud. Rocky replicates changes made to the disk images as a consistent immutable mutation history consisting of a totally-ordered write sequence. Therefore, it allows data recovery against tampering attacks such as ransomware or the wiper malware attacks or permanent failures such as fire, earthquake or disk worn out.

# Build Environment

We tested with the following versions of software:

1. Ubuntu 16.04

2. Java 8

3. Gradle 2.10

# Prerequisites

Replace \<RockyHome\> below with the directory path where you cloned the Rocky git repo.

1. FoundationDB needs to be installed.
   - Reference: https://apple.github.io/foundationdb/getting-started-linux.html
   - There are two files to install in <RockyHome>/foundationdb: foundationdb-clients_6.2.19-1_amd64.deb, foundationdb-server_6.2.19-1_amd64.deb
   - `sudo dpkg -i foundationdb-clients_6.2.19-1_amd64.deb`
   - `sudo dpkg -i foundationdb-server_6.2.19-1_amd64.deb`

2. ndb-client needs to be installed.
   - `sudo apt-get update`
   - `sudo apt-get install nbd nbd-client`
     - Choose default value, 'yes,' for disconnecting all nbd-client devices.

3. Need to create a foundationdb volume in advance.
   - There is nbdfdb/nbdcli.jar to prepare a volume.
     - `java -jar nbdfdb/nbdcli.jar server`
     - `java -jar nbdfdb/nbdcli.jar create -n testing -s 1G`
   - (Not necessary. Just use nbdfdb/nbdcli.jar) If wish to do this by building from the scratch:
      - Need Apache Maven 3.3.9 to build the tool from the source
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
	    - Other useful commands for nbdcli.jar is described in its usage messages:
	    ---
	    '''
	    Usage: nbdfdb.cli.NBDCLI [command name]
	    create: Usage: nbdfdb.cli.CreateCommand
	      -exportName (-n) [String] Name of the volume to create
	      -size (-s) [String] Size in bytes of the volume, can use K, M, G, or T units.
	      -blockSize (-b) [Integer] Block size of the volume (512)
	    delete: Usage: nbdfdb.cli.DeleteCommand
	      -exportName (-n) [String] Name of the volume to delete
	    snapshot: Usage: nbdfdb.cli.SnapshotCommand
	      -exportName (-n) [String] Name of the snapshot volume to create
	      -volumeName (-v) [String] Name of the volume to snapshot
	    list: Usage: nbdfdb.cli.ListCommand
	    server: Usage: nbdfdb.cli.ServerCommand
	    '''
	    ---		

# How to build

`gradle clean fatJar`

# How to run

1. Setup a Connector-Cloudlet, a.k.a. a replication broker
   - We support two types of the backend in conf/rocky.conf for the parameter backendStorageType: dynamoDBLocal and dynamoDBSeoul
     - If testing with dynamoDBLocal, download dynamoDB first, setup the environment as the following link https://tinyurl.com/k34xbtm8 and then do the following
       - `java -Djava.library.path=./dynamoDB/DynamoDBLocal_lib -jar ./dynamoDB/DynamoDBLocal.jar -sharedDb`
     - If using dynamoDBSeoul, one needs to appropriately setup the environment to use aws.
       - Refer to AWS documentation (https://tinyurl.com/4d2rvxmj)
       	 - Learn about how to signing up for AWS, getting AWS Access Key and configuring AWS credentials using AWS CLI
       

2. Run Rocky Controller (NBD server)
   - `java -jar <RockyHome>/build/libs/Rocky-all-1.0.jar rocky.ctrl.RockyController`
   - To run with configuration file conf/rocky.conf:
     - `java -jar <RockyHome>/build/libs/Rocky-all-1.0.jar rocky.ctrl.RockyController conf/rocky.conf`
     - It will print out configuration parameters and their values

3. Prepare the Rocky Block Device (nbd module & nbd client)
   - `sudo modprobe nbd`
   - `sudo lsmod | grep nbd`
   - `sudo nbd-client -N <volume name> localhost /dev/nbd0`
     - (testing is one of volume names)

4. Switching roles of the Rocky Controller
   - Once you started the Rocky Controller successfully, you will get a list of commands for you to control the Rocky Controller via ControlUserInterface.
   - type '2' and enter. It will show the current role.
   - If 'None' then type '2' to switch to 'NonOwner'
   - If 'NonOwner' then type '3' to switch to 'Owner'
   - Make sure the role of RockyController to be Owner before generating any I/O


To disconnect the Rocky Block Device from the Rocky Controller, `sudo nbd-client -d /dev/nbd0`
Note: when you disconnect and try to connect again, it may fail because of "Volume testing is already leased." This is because the lease for the underlying foundataionDB volume 'testing' has not been released yet. Wait for a minute or so, and try again.

To remove Rocky Block Device module from the kernel, `sudo modprobe -r nbd`

## To Test: making file system on the block device

- `sudo mkfs.ext4 /dev/nbd0`
- `sudo mount /dev/nbd0 /tmp`
- `ls /tmp`
- Should be able to see the directory lost+found
- `sudo umount /tmp`

# ACSAC21 Evaluation

## To Reproduce the throughput measurement in Section 5.2

Resulting data can be found eval/throughput.csv

1. Make sure the role of the Rocky Controller to be Owner before generating any I/O. Also, try this with dynamoDBSeoul
   - If screw up, bring down the Rocky Controller and disconnect the Rocky Controller from the Rocky block device. Then, restart the Rocky Controller and connect it with the block device again.

2. To initialize the disk to avoid reading null, `echo 3 | sudo tee /proc/sys/vm/drop_caches; sudo dd if=/dev/zero of=/dev/nbd0 bs=10K count=300 oflag=direct`

3. Configure how much percentage of the blocks present locally to avoid fetching from the replication broker
   - Select 4 for the Rocky Controller ControlUserInterface
   - Type in the percentage (e.g., 70 for seventy percent of blocks being present locally)

4. Using 'dd,' generate I/O
   - To write, `echo 3 | sudo tee /proc/sys/vm/drop_caches; sudo dd if=/dev/zero of=/dev/nbd0 bs=10K count=200 oflag=direct`
   - To read, `echo 3 | sudo tee /proc/sys/vm/drop_caches; sudo dd if=/dev/nbd0 of=/dev/zero bs=10K count=200 iflag=nocache`

5. Repeat 3 and 4 for different percentages of blocks present locally and for different operation types, either write or read.

## To Reproduce the reduction ratio measurement in Section 5.3

1. Make the file system on the Rocky block device and mount it to /tmp, as described in the section 'To Test:...' above.

2. Copy a file photo/alvaro-palacios-FCdR-3_9AZk-unsplash.jpg into the /tmp

3. In Rocky Controller ControlUserInterface, type '8' and '1' to print out the statistics showing how many blocks were written cumulatively ('Number of requested block writes') and how many blocks to be flushed as mutation snapshots ('Number of blocks for a Mutation Snapshot').

# (Not necessary) To Run multiple Rocky instances on a single host

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
  - `java -jar <RockyHome>/build/libs/Rocky-all-1.0.jar rocky.ctrl.RockyController run/rocky.conf.1`
- Run nbd-client for the Rocky instance with correct parameters.
  - `sudo nbd-client -N testing1 localhost 10811 /dev/nbd1`

Likewise, suppose run/rocky.conf.2 sets port=10812 and lcvdName=testing2
Also, say /dev/nbd2 is the Rocky device driver instance to use.
- `java -jar <RockyHome>/build/libs/Rocky-all-1.0.jar rocky.ctrl.RockyController run/rocky.conf.2`
- `sudo nbd-client -N testing2 localhost 10812 /dev/nbd2`

