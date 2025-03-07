# Copyright 2015 gRPC authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""The Python implementation of the GRPC rocky.timetraver.RockyTimeTraveler client."""

from __future__ import print_function

import argparse
import logging
import sys
import grpc
import rockytimetravel_pb2
import rockytimetravel_pb2_grpc

def parse_config(file_path):
    config = {}
    try:
        with open(file_path, "r") as file:
            for line in file:
                line = line.strip()
                if line and "=" in line:
                    key, value = line.split("=", 1)
                    config[key.strip()] = value.strip()
    except Exception as e:
        print(f"Error reading configuration file: {e}")
        sys.exit(1)
    
    return config

def test_get_status(stub):
    print(">> Testing GetStatus")
    response = stub.GetStatus(rockytimetravel_pb2.GetStatusRequest())
    print("Received response ackMsg=" + response.ackMsg)
    roleStr = "Unknown"
    roleInt = response.role
    if roleInt == 0:
        roleStr = "Owner"
    elif roleInt == 1:
        roleStr = "NonOwner"
    elif roleInt == 2:
        roleStr = "None"
    
    if roleStr == "Unknown":
        print("ERROR: roleStr is Unknown")
        logging.error("An exception was thrown!", exc_info=True)
        sys.exit(1)
    else:
        print("roleStr is", roleStr)
    
    curEpoch = response.epoch
    latestEpoch = response.latestEpoch
    print("role=%s" % roleStr)
    print("epoch=%d" % curEpoch)
    print("latestEpoch=%d" % latestEpoch)
    print("[TEST PASS] GetStatus Test ends.")
    return roleInt, curEpoch, latestEpoch

def test_switch_role(stub):
    while True:
        try:
            role_from = int(input("Enter current role (0: Owner, 1: NonOwner, 2: None): "))
            role_to = int(input("Enter new role (0: Owner, 1: NonOwner, 2: None): "))
            if role_from in [0, 1, 2] and role_to in [0, 1, 2]:
                break
            else:
                print("Invalid input. Please enter a valid role number.")
        except ValueError:
            print("Invalid input. Please enter integer values.")
    
    print(">> Testing SwitchRole")
    response = stub.SwitchRole(rockytimetravel_pb2.SwitchRoleRequest(
        roleFrom=role_from, roleTo=role_to))
    print("Received response ackMsg=" + response.ackMsg)
    print("New role after switch: %d" % response.roleNew)
    print("[TEST PASS] SwitchRole Test ends.")

def test_rewind(stub):
    while True:
        try:
            epoch_from = int(input("Enter epochFrom for Rewind: "))
            epoch_to = int(input("Enter epochTo for Rewind: "))
            break
        except ValueError:
            print("Invalid input. Please enter integer values.")
    
    print(">> Testing Rewind")
    response = stub.Rewind(rockytimetravel_pb2.RewindRequest(
        epochFrom=epoch_from, epochTo=epoch_to))
    print("Received response ackMsg=" + response.ackMsg)
    print("New epoch after rewind: %d" % response.epochNew)
    print("[TEST PASS] Rewind Test ends.")

def test_replay(stub):
    while True:
        try:
            epoch_from = int(input("Enter epochFrom for Replay: "))
            epoch_to = int(input("Enter epochTo for Replay: "))
            break
        except ValueError:
            print("Invalid input. Please enter integer values.")
    
    print(">> Testing Replay")
    response = stub.Replay(rockytimetravel_pb2.ReplayRequest(
        epochFrom=epoch_from, epochTo=epoch_to))
    print("Received response ackMsg=" + response.ackMsg)
    print("New epoch after replay: %d" % response.epochNew)
    print("[TEST PASS] Replay Test ends.")

def main():
    parser = argparse.ArgumentParser(description="gRPC Client for RockyTimeTraveler")
    parser.add_argument("config_path", help="Path to the configuration file")
    args = parser.parse_args()
    
    # Check if the config_path argument is provided
    if not args.config_path:
        print("Error: Configuration file path is required.")
        print(f"Usage: {sys.argv[0]} <config_path>")
        sys.exit(1)

    config = parse_config(args.config_path)
    
    grpc_port = config.get("gRPCPort")
    if grpc_port is None:
        print("Error: gRPCPort not found in configuration file. Please check your config.")
        sys.exit(1)
    
    print(f"Connecting to gRPC server on port {grpc_port}...")
    with grpc.insecure_channel(f"localhost:{grpc_port}") as channel:
        stub = rockytimetravel_pb2_grpc.TimeTravelerStub(channel)
        role, epoch, latestEpoch = test_get_status(stub)
        
        while True:
            print("\nSelect an option:")
            print("1: GetStatus")
            print("2: SwitchRole")
            print("3: Rewind")
            print("4: Replay")
            print("5: Exit")
            
            choice = input("Enter option number: ")
            
            if choice == "1":
                role, epoch, latestEpoch = test_get_status(stub)
            elif choice == "2":
                test_switch_role(stub)
            elif choice == "3":
                test_rewind(stub)
            elif choice == "4":
                test_replay(stub)
            elif choice == "5":
                print("Exiting...")
                break
            else:
                print("Invalid option. Please choose again.")
    
if __name__ == "__main__":
    logging.basicConfig()
    main()

