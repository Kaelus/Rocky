# Rocky: Tamper-Resistant Distributed Replicated Block Device for EdgeVDI

## Authors

Beom Heyn (Ben) Kim \<beomheynkim@hanyang.ac.kr\> at Critical System Lab, Hanyang University ERICA

# Overview

Rocky is a distributed replicated block device for tamper and failure resistant EdgeVDI. Rocky uses a replication broker to synchronize disk images across cloudlets, which is the edge datacenter sitting between endpoints and the cloud. Rocky replicates changes made to the disk images as a consistent immutable mutation history consisting of a totally-ordered write sequence. Therefore, it allows data recovery against tampering attacks such as ransomware or the wiper malware attacks or permanent failures such as fire, earthquake or disk worn out.

Details of its design is further discussed in the research paper "[Rocky: Replicating Block Devices for Tamper and Failure Resistant Edge-based Virtualized Desktop Infrastructure](https://dl.acm.org/doi/abs/10.1145/3485832.3485886)," presented at ACSAC '21

# Build Environment

Tested with the following versions of software:

1. Ubuntu 22.04

2. Java 11

3. Gradle 8.5

4. (optional for Python gRPC client stub)
   - Python 3.10.12
   - pip 22.0.2

# Prerequisites

Replace \<RockyHome\> below with the directory path where you cloned the Rocky git repo.
NOTE: You need to name the local repo you cloned as 'Rocky'. That is, your git repo cloned should be at /home/\<USERNAME\>/project/Rocky (Replace \<USERNAME\> properly).

1. FoundationDB needs to be installed.
   - Reference: https://apple.github.io/foundationdb/getting-started-linux.html
   - There are two files to install in \<RockyHome\>/foundationdb: foundationdb-clients_6.2.19-1_amd64.deb, foundationdb-server_6.2.19-1_amd64.deb
   - `sudo dpkg -i foundationdb-clients_6.2.19-1_amd64.deb`
   - `sudo dpkg -i foundationdb-server_6.2.19-1_amd64.deb`

2. ndb-client needs to be installed. Get it from the repo of your distro. For example (Ubuntu 'apt-get'):
   - `sudo apt-get update`
   - `sudo apt-get install nbd-server nbd-client`
     - Choose default value, 'yes,' for disconnecting all nbd-client devices.

3. Set up for AWS DynamoDB:
   - The jar file "DynamoDBLocal.jar" needed can be found in \<RockyHome\>/dynamoDB
   - Reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.DownloadingAndRunning.html
   - `sudo apt install awscli`
   - `aws configure`
     - AWS Access Key ID: fakeMyKeyId
       AWS Secret Access Key: fakeSecretAccessKey
       Default Region Name: fakeRegion
       Default output format: fakeOutputFormat

4. (optional for Python gRPC client stub) Install python3 and setup virtualenv properly
   - `sudo apt install -y python3-pip`
   - `sudo apt install -y build-essential libssl-dev libffi-dev python3-dev`
   - `sudo apt install -y python3-venv`
   - Go to \<RockyHome\>
   - `python3 -m venv rocky_venv`
   - `source rocky_venv/bin/activate`
   - `python -m pip install grpcio`
   - `python -m pip install grpcio-tools`
   - `deactivate`

# How to build

When you are in \<RockyHome\>

1. Build to generate gRPC stuffs first.
   - `./gradlew clean generateProto`

2. Build to generate fatJar to produce the executable Jar including every dependencies needed.
   - `./gradlew clean fatJar`

3. (optional for Python gRPC client stub) Generate gRPC Python client stub
   - `source rocky_venv/bin/activate`
   - `cd src/main/python/rocky_tt_client`
   - `mkdir src/main/python/grpc_generated`
   - `python -m grpc_tools.protoc -I./src/main/python/grpc_generated --python_out=./src/main/python/grpc_generated --grpc_python_out=./src/main/python/grpc_generated --proto_path=./src/main/proto/ rockytimetravel.proto`
   - `export PYTHONPATH=$PYTHONPATH:\`pwd\`/src/main/python/grpc_generated:\`pwd\`/rocky_venv/lib/python3.10/site-packages`

# How to prepare to run

1. Create a volume for foundationDB to use locally.
   - To create the volume name "testinglocal0" whose size is 10MB
     - `./fdb_create_volume.sh testinglocal0 10MB`
   - If you want to list up existing volumes:
     - `./fdb_list_volume.sh`
   - If you want to delete a specific existing volume (e.g., testinglocal0):
     - `./fdb_delete_volume.sh testinglocal0`

2. Deploy necessary artifact to a working directory.
   - There is a script "setup_local.sh" in "\<RockyHome\>/scripts", which deploys necessary components into a working directory.
     - To deploy to the working directory "~/working_dir/rocky/local/":
       - `./setup_local.sh 1 ~/working_dir/rocky/local`

3. [Optional] Configure Rocky
   - Under the 'conf' directory of your working directory (e.g., ~/working_dir/rocky/local/conf), there are folders whose names are their node ID (e.g., 0). Under those folders, there are two configuration files: rocky_local.cfg and recover_local.cfg
   - Meaning of each configuration parameter in those files is as follows:
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

# How to run Rocky (only one instance)

1. Use the script run_local.sh in "scripts" under the your working directory
   - For example, to run Rocky node with the volume name "testinglocal0" of the size 10MB with the nbd device name "nbd0" by using the configuration file "rocky_local.cfg" in the "conf" directory under the working directory:
     - `./run_local.sh ../conf/0/rocky_local.cfg`

2. Switching roles of the Rocky Controller
   - Once you started the Rocky Controller successfully, you will get a list of commands for you to control the Rocky Controller via ControlUserInterface.
   - type '2' and enter. It will show the current role.
   - If 'None' then type '1' to switch to 'NonOwner'
   - If 'NonOwner' then type '0' to switch to 'Owner'
   - Make sure the role of RockyController to be Owner before generating any I/O

3. Run the following workloads in another terminal to generate some disk I/O to the nbd0 to test (You may run a VM, ransomware in it, for a more comprehensive test):
   - `sudo mkfs.ext4 /dev/nbd0`
     - if the above does not work, try:
       - `sudo mkfs.ext2 /dev/nbd0`
   - `sudo mount /dev/nbd0 /tmp`
   - `ls /tmp`
   - Should be able to see the directory lost+found
   - Create or copy some files into /tmp
     - For example,
       - `sudo touch /tmp/hello`
       - `echo 'Hello, World!' | sudo tee /tmp/hello`
   - If you need more informative messages to be printed out, type '9' in the terminal where you are running the Rocky node to enable verbose mode.
     - You can type '9' once more anytime to turn it off again.
   - Type '5' in the terminal where you are running the Rocky node to forcefully flush changes made so far to the replication broker.
     - This makes the benign writes to be flushed during the last epoch (currently, the last epoch should be epoch 1).
   - At last, corrupt files.
     -For instance, as follows, simply write some threatening messages like "Pay me if you want your data!!!!" in one of text file you created or copied into the /tmp
       - `echo 'Pay me if you want your data!!!!' | sudo tee /tmp/hello`
   - Now, flush to the replication broker once more by typing '5'. Also, remember the epoch number printed on the console (If you have been faithfully following this instruction, it should be epoch number 2 that is printed out.)
     - By doing this flushing, the tampering writes are flushed during the epoch 2. So, the epoch 2 is going to be the e_a which you want to rollback.
   - `sudo umount /tmp`

5. To finish running Rocky:
   - Type -1 in the terminal where a Rocky node is running
   - To stop other components, use the script stop_local.sh:
     - `./stop_local.sh`

6. To recover the corrupted Rocky:
   - Use the script recover_local.sh:
     - `./recover_local.sh ../conf/0/recover_local.cfg`
   - Stop 
     - `./stop_local.sh`

7. Confirm the recovery is made correctly:
   - Run the rocky node again in the same way as described in Step 1 and 2 above.
   - `sudo mount /dev/nbd0 /tmp`
   - Then, check the contents of the corrupted files are restored correctly. e.g., you may cat the text file and see if its contents is same as it was tampered by threatening messages.

8. To clean and start over again:
   - Unmount the nbd0:
     - `sudo umount /tmp`
   - Quit the Rocky node by typing -1 or ctrl-x
   - In the working directory's script directory:
     - `./stop_local.sh`
     - `./clean_local.sh`
   - Then, go 'How to Prepare to Run' step 1.

# How to prepare to run multiple instances (on the same localhost)

1. Assuming "How to prepare to run" and "How to run Rocky (only one instance)" are already done, prepare volumes to use. Basically, we need volumes as many as the number of instances we want to run.
   - Check if all volumes to use exist:
     - `./fdb_list_volume.sh`
   - If not all volumes to use exist already, create as many volumes as needed (e.g., to run 2 instances and none of volumes to use for them exist, create the volume name "testinglocal0" and "testinglocal1 whose sizes are 10MB)
     - `./fdb_create_volume.sh testinglocal0 10MB`
     - `./fdb_create_volume.sh testinglocal1 10MB`

2. Run a script to set up the working directory for instances as many as you want to run.
   - For instance, to deploy 2 instances on the same localhost where the working directory is "~/working_dir/rocky/local/":
     - `./setup_local.sh 2 ~/working_dir/rocky/local`
   - Confirm that there are subdirectories for each instance under conf, data, and log directories in "~/working_dir/rocky/local". The subdirectories are named after the node ID associated with each instance.

3. [Optional] Configure Rocky
   - Refer to the step 3 of "How to prepare to run" above
   
# How to run Rocky (multiple instances on the same localhost)

1. Use the script run_local.sh in "scripts" under the your working directory
   - Run each instance by providing proper path to the configuration file to use.
     - For example, to run Rocky instance for the node ID 0 and 1, do the followings:
       - Open a terminal for node 0:
       	 - `./run_local.sh ../conf/0/rocky_local.cfg`
       - Open another terminal for node 1:
       	 - `./run_local.sh ../conf/1/rocky_local.cfg`

2. Switching roles of the Rocky Controllers
   - Once you started the Rocky Controllers successfully, you will get a list of commands for you to control the Rocky Controllers via ControlUserInterface.
   - Make the instance to be used by an application become "Owner", while other instances should become "NonOwner" (e.g. node 0 becomes "Owner", while node 1 becomes "NonOwner")
     - For node 0: 
       - type '2' and enter. It will show the current role.
       - If 'None' then type '1' to switch to 'NonOwner'
       - If 'NonOwner' then type '0' to switch to 'Owner'
     - For node 1:
       - type '2' and enter. It will show the current role.
       - If 'None' then type '1' to switch to 'NonOwner'
       - Stop here.

3. Open a new terminal to run workloads and do the followings to see writes generated on the node 0 can be seen by the node 1:
   - Create subdirectories as mount points for node 0 and node 1 under /tmp.
     - `mkdir /tmp/mp0`
     - `mkdir /tmp/mp1`
   - For node 0:
     - `sudo mkfs.ext4 /dev/nbd0`
       - if the above does not work, try:
       	 - `sudo mkfs.ext2 /dev/nbd0`
     - `sudo mount /dev/nbd0 /tmp/mp0`
     - `ls /tmp/mp0`
       - Should be able to see the directory lost+found
     - Create or copy some files into "/tmp/mp0"
       - For example,
         - `sudo touch /tmp/mp0/hello`
         - `echo 'Hello, World!' | sudo tee /tmp/mp0/hello`
       - Feel free to create some more if you want.
     - Recall if you need more informative messages to be printed out, type '9' in the terminal where you are running the Rocky node to enable verbose mode.
       - You can type '9' once more anytime to turn it off again.
     - Type '5' in the terminal where you are running the Rocky node to forcefully flush changes made so far to the replication broker.
       - This makes the benign writes to be flushed during the last epoch (currently, the last epoch should be epoch 1).
   - For node 1: 
     - Go to the node 1's terminal and make node 1 prefetch all recent changes by typing '6' (recall, you can type '9' to toggle the verbose mode)
     - After prefetch completes, go back to the original terminal and mount "nbd1" to "/tmp/mp1" as read-only and also do not load the journal on mounting.
       - `sudo mount -o ro,noload,nobarrier,noexec,nosuid,nodev /dev/nbd1 /tmp/mp1`
     - Then, check if the content written on nbd0 is visible from nbd1.
       - `cat /tmp/mp1/hello`
     - `sudo umount /tmp/mp1`

4. To finish running Rocky:
   - Type -1 in the terminal where a Rocky node is running
   - To stop other components, use the script stop_local.sh:
     - `./stop_local.sh`

# How to run Rocky TimeTraveler Python Client

1. Assuming you have already done "How to prepare to run multiple instances (on the same localhost)", do the following.
   - Run each instance by providing proper path to the configuration file to use.
     - For example, to run Rocky instance for the node ID 0 and 1, do the followings:
       - Open a terminal for node 0:
       	 - `./run_local.sh ../conf/0/rocky_local.cfg`
       - Open another terminal for node 1:
       	 - `./run_local.sh ../conf/1/rocky_local.cfg`


2. Switching the role of the first Rocky instance to be 'Owner' (Say "node 0")
   - type '2' and enter. It will show the current role.
   - If 'None' then type '1' to switch to 'NonOwner'
   - If 'NonOwner' then type '0' to switch to 'Owner'

3. Generate workloads and flush changes to the cloud to move the epoch forward multiple times (You may run a VM, ransomware in it, for a more comprehensive test):
   - Open a new terminal to execute a workload below.
   - `sudo mkfs.ext4 /dev/nbd0`
     - if the above does not work, try:
       - `sudo mkfs.ext2 /dev/nbd0`
   - Type '5' in the test loop of the Owner's terminal to flush. 
     - This will create mutation snapshots for epoch 1.
   - `sudo mount /dev/nbd0 /tmp/mp0`
   - `ls /tmp/mp0`
   - Should be able to see the directory "lost+found" in "/tmp/mp0"
   - Create some file into "/tmp/mp0"
     - `sudo touch /tmp/mp0/hello`
     - `echo 'Hello, World!' | sudo tee /tmp/hello`
   - Type '5' in the test loop of the Owner's terminal to flush.
     - This will create mutation snapshots for epoch 2.
   - Create another file into "/tmp"
     - `sudo touch /tmp/mp0/goodbye`
     - `echo 'Goodbye, World!' | sudo tee /tmp/hello`
   - Type '5' in the test loop of the Owner's terminal to flush.
     - This will create mutation snapshots for epoch 3.

4. Use the second Rocky instance (say "Node 1") to conduct tests for rewind and replay
   - Switching the role of Node 1 to be "NonOwner"
     - type '2' and enter. It will show the current role.
     - If 'None' then type '1' to switch to 'NonOwner'
     - Stop here.
   - Open another terminal. 
     - Go to "\<RockyHome\>"
     - Activate python virtual environment for rocky.
       - `source rocky_venv/bin/activate`
     - Go to the directory containing the gRPC Python client and run it along with a configuration file used to run the Node 1 (e.g., "~/working_dir/rocky_local/conf/1/rocky_local.cfg")
       - `cd src/main/python/rocky_tt_client`
       - `python rocky_timetravel_client.py ~/working_dir/rocky/local/conf/1/rocky_local.cfg`
     - Have node 1 incrementally replay.
       - replay from epoch 0 to epoch 1
       	 - mount the device and check there exist file systems on the device
           - `sudo mount -o ro,noload,nobarrier,noexec,nosuid,nodev /dev/nbd1 /tmp/mp1`
       - replay from epoch 1 to epoch 2
       	 - mount the device and check there exist hello file and check its content
       - replay from epoch 2 to epoch 3
       	 - mount the device and check there exist goodbye file and check its content
       - Now, try to rewind to epoch 1.
       	 - mount the device and check there exist file systems on the device
	 - NOTE: Currently, rewinding is limited. We are not undoing IOs but redoing IOs starting from the beginning which is the epoch 1. As we are replaying IOs without rolling back changes, it is actually replaying IOs against the current block device state. It works because the first writes to the device are formatting the device with a new file system. This effectively delete any changes made afterwards by replaying IOs during the epoch 1. If the first writes were not that, then there will be problems expected. Later we may use a snapshot and replay IOs based on the snapshot.
     - After finishing with testing, exit the rocky time traveler client by typing '5'.
     - Then, exit the python virtual environment.
       - `deactivate`

5. To finish running Rocky:
   - Type -1 in the terminal where a Rocky node is running
   - To stop other components, use the script stop_local.sh:
     - `./stop_local.sh`
