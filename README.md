# Rocky: Tamper-Resistant Distributed Replicated Block Device for EdgeVDI

## Authors

Beom Heyn Kim \<beomheyn.kim@gmail.com\> at Cloud System Lab, Hanyang University ERICA

# Overview

Rocky is a distributed replicated block device for tamper and failure resistant EdgeVDI. Rocky uses a replication broker to synchronize disk images across cloudlets, which is the edge datacenter sitting between endpoints and the cloud. Rocky replicates changes made to the disk images as a consistent immutable mutation history consisting of a totally-ordered write sequence. Therefore, it allows data recovery against tampering attacks such as ransomware or the wiper malware attacks or permanent failures such as fire, earthquake or disk worn out.

Details of its design is further discussed in the research paper "[Rocky: Replicating Block Devices for Tamper and Failure Resistant Edge-based Virtualized Desktop Infrastructure](https://dl.acm.org/doi/abs/10.1145/3485832.3485886)," presented at ACSAC '21

# Build Environment

Tested with the following versions of software:

1. Ubuntu 22.04

2. Java 11

3. Gradle 8.5

# Prerequisites

Replace \<RockyHome\> below with the directory path where you cloned the Rocky git repo.
NOTE: You need to name the local repo you cloned as 'Rocky'. That is, your git repo cloned should be at /home/<USERNAME>/project/Rocky (Replace <USERNAME> properly).

1. FoundationDB needs to be installed.
   - Reference: https://apple.github.io/foundationdb/getting-started-linux.html
   - There are two files to install in \<RockyHome\>/foundationdb: foundationdb-clients_6.2.19-1_amd64.deb, foundationdb-server_6.2.19-1_amd64.deb
   - `sudo dpkg -i foundationdb-clients_6.2.19-1_amd64.deb`
   - `sudo dpkg -i foundationdb-server_6.2.19-1_amd64.deb`

2. ndb-client needs to be installed. Get it from the repo of your distro. For example (Ubuntu 'apt-get'):
   - `sudo apt-get update`
   - `sudo apt-get install nbd-server nbd-client`
     - Choose default value, 'yes,' for disconnecting all nbd-client devices.

3. Install aws:
   - Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
   - `sudo apt install awscli`
   - `aws configure`
     - AWS Access Key ID: "fakeMyKeyId"
       AWS Secret Access Key: "fakeSecretAccessKey"
       Default Region Name: "fakeRegion"

# How to build

`gradle clean fatJar`

# How to prepare to run

1. Create a volume for foundationDB to use locally.
   - To create the volume name 'testinglocal0' whose size is 10MB
     - `./fdb_create_volume.sh testinglocal0 10MB`

2. Deploy necessary artifact to a working directory.
   - There is a script setup_local.sh in \<RockyHome\>, which deploys necessary components into a working directory.
     - To deploy to the working directory ~/working_dir/rocky/local/:
       - `./setup_local.sh 1 ~/working_dir/rocky/local`

3. [Optional] Configure Rocky
   - In your working directory, there are two configuration files in conf directory: rocky_local.conf and recovery_local.conf
   - Meaning of each configuration parameter is as follows:
     - ip: Should have the value of the format "\<IP address\>:\<Port number\>" and it is going to be used as the ID for the Rocky endpoint. 
     - lcvdName: The backend volume name
     - rockyMode: It can be either origin, rocky or recovery. Use rocky mode to run a VM atop. Use recovery to recover the tampered rocky node. Origin is used for the evaluation purpose.
     - backendStorageType: Determines which backend to be used. e.g., dynamoDBLocal.
     - epochPeriod: Time in ms. It determines how frequently the owner flushes the block snapshots to the replication broker.
     - prefetchPeriod: Time in ms. It determines how frequently the non-owner prefetches mutation snapshots from the replication broker.
     - workingDir: The absolute path to the working directory. e.g. ~/working_dir/rocky/local
     - debugFlag: Turn on or off the verbose mode for more detailed messages printed out.
     - coordinator: The node ID of the coordinator. It should be in a formate that is same as the ID's value. This should be set for recovery.
     - e_a: The first epoch number for which the tampering writes are issued. This should be set for recovery. Adjust this accordingly.
     - hasCloudFailed: This tells if the cloud is alive or failed. This should be set for recovery.

# How to run Rocky

1. Use the script run_local.sh
   - To run Rocky node with the volume name "testinglocal" of the size 10MB with the nbd device name "nbd0" by using the configuration file "rocky_local.conf" in the conf directory:
     - ./run_local.sh ../conf/0/rocky_local.conf

2. Switching roles of the Rocky Controller
   - Once you started the Rocky Controller successfully, you will get a list of commands for you to control the Rocky Controller via ControlUserInterface.
   - type '2' and enter. It will show the current role.
   - If 'None' then type '2' to switch to 'NonOwner'
   - If 'NonOwner' then type '3' to switch to 'Owner'
   - Make sure the role of RockyController to be Owner before generating any I/O

3. Run the following workloads in another terminal to generate some disk I/O to the nbd0 to test (You may run a VM, ransomware in it, for a more comprehensive test):
   - `sudo mkfs.ext4 /dev/nbd0`
     - if the above does not work, try:
       - `sudo mkfs.ext2 /dev/nbd0`
   - `sudo mount /dev/nbd0 /tmp`
   - `ls /tmp`
   - Should be able to see the directory lost+found
   - Create or copy some files into /tmp
     - `sudo touch /tmp/hello`
     - `echo 'Hello, World!' | sudo tee /tmp/hello`
   - Type 9 in the terminal where you are running the Rocky node to enable verbose mode. (You can type 9 anytime to disable it)
   - Type 5 in the terminal where you are running the Rocky node to forcefully flush changes made so far to the replication broker.
     - This makes the benign writes to be flushed during the epoch 1.
   - At last, corrupt the files. (e.g. simply write some threatening messages like "Pay me if you want your data!!!!" in one of text file you created or copied into the /tmp)
     - `echo 'Pay me if you want your data!!!!' | sudo tee /tmp/hello`
   - Now, flush to the replication broker once more by typing 5. Also, remember the epoch number printed on the console (it should be 2)
     - By doing this flushing, the tampering writes are flushed during the epoch 2. So, the epoch 2 is going to be the e_a which you want to rollback.
   - `sudo umount /tmp`

5. To finish running Rocky:
   - Type -1 in the terminal where a Rocky node is running
   - To stop other components, use the script stop_local.sh:
     - `./stop_local.sh`

6. To recover the corrupted Rocky:
   - Use the script recover_local.sh:
     - `./recover_local.sh ../conf/0/recover_local.conf`
   - Stop 
     - `./stop_local.sh`

7. Confirm the recovery is made correctly:
   - Run the rocky node again in the same way as described in Step 1 and 2.
   - `sudo mount /dev/nbd0 /tmp`
   - Then, check the contents of the corrupted files are restored correctly. e.g., you may cat the text file and see if its contents is same as it was tampered by threatening messages.

8. To clean and start over again:
   - Quit the Rocky node by typing -1 or ctrl-x
   - In the working directory's script directory:
     - `./stop_local.sh`
     - `./clean_local.sh`
   - Then, go 'How to Prepare to Run' step 1.
