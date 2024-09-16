import os
from netaddr import *
import ipaddress
# TODO: Should run with python3.10
import os
import time
# for pretty printing of JSONs
from pprint import pprint
# client to connect to the infrastructure
from netunicorn.client.remote import RemoteClient, RemoteClientException
# basic abstraction for experiment creation and management
from netunicorn.base.experiment import Experiment, ExperimentStatus
from netunicorn.base.pipeline import Pipeline
# task to be executed in the pipeline
# you can write your own tasks, but now let's use simple predefined one
from netunicorn.library.tasks.basic import SleepTask
import json
from collections import defaultdict
import subprocess


def runCommandAsRoot(command):
    command = "sudo -S " + command  # Include -S option

    try:
        # Run the command with sudo, providing the password via stdin
        result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, input="surfjaber\n", check=True)
    
        # Print the command's output
        print("Command output:")
        print(result.stdout)
    
    except subprocess.CalledProcessError as e:
        print(f"Error running the command: {e}")
        print("Error output:")
        print(e.stderr)



# Find all public IPv4 addresses of each pinot node
def check_publics(node,valid_nodes):
    name = node.name
    IP_Addresses = node.properties["ipv4"]
    for ip in IP_Addresses:
        if not ipaddress.ip_address(ip).is_private and not IPAddress(ip).is_loopback():
            if name not in valid_nodes:
                valid_nodes[name] = [ip]
            else:
                valid_nodes[name].append(ip)
    return valid_nodes

def getNodesAndIPs():

    pipeline = Pipeline()
    # Notice, that executor will first in parallel execute `sleep 5` and `sleep 3`...
    pipeline = pipeline.then([
        SleepTask(5),
        SleepTask(3)
    ]).then(
        # ...and after they finished (after 5 second in total) will execute `sleep 10`
        SleepTask(10)
    )

    # API connection endpoint
    endpoint =  "https://pinot.cs.ucsb.edu/netunicorn"

    # user login
    login = "jaber"

    # user password
    password = 'allyouneedtoknow'

    # let's create a client with these parameters
    client = RemoteClient(endpoint=endpoint, login=login, password=password)
    client.healthcheck()

    nodes = client.get_nodes()
    print(nodes[1])
    # List of all Pinot nodes
    nodes= nodes[1]



    # Find all pinot nodes with public ipv4 address
    valid_nodes = defaultdict(list)
    for i in range(len(nodes)):
        valid_nodes = check_publics(nodes[i], valid_nodes)

    for node, ips in valid_nodes.items():
        if not node.startswith('raspi'):
            del valid_nodes[node]

    return valid_nodes

def generateNodeTunnelPairs(valid_nodes):
    subnet = "192.168.1."
    print(len(valid_nodes))
    if len(valid_nodes) > 61:
        print("You need larger subnet")
        exit()
    counter = 9
    tunnel_info = {}
    for key in valid_nodes:
        counter +=4
        server_tunnel_addr = subnet + str(counter) + "/30"
        pinot_tunnel = subnet + str(counter + 1) + "/30"
        tunnel_subnet = subnet + str(counter - 1) + "/30"
        # valid_nodes[key] = [valid_nodes[key],
        tunnel_info[key] = { 'pinotPublicIP' : valid_nodes[key][0], 'serverTunnelAddr': server_tunnel_addr, 'pinotTunnelAddr' :
                           pinot_tunnel , 'tunnelSubnet' : tunnel_subnet}
    for item in tunnel_info:
        print(item)
        print(tunnel_info[item])
    with open("TunnelInfo.json", "w") as outfile:
         json.dump(tunnel_info, outfile)
    
def createServerSideTunnel(tunnel_info_file):
    file = open(tunnel_info_file, 'r')
    tunnel_info = json.load(file)

    greCounter = 10
    cnt = 0
    for node in tunnel_info:

        greCounter +=1
        remote = tunnel_info[node]['pinotPublicIP']
        interfaceName = "gre" + str(greCounter)
        greAddr = tunnel_info[node]['serverTunnelAddr']
        runCommandAsRoot(f"iptunnel add {interfaceName} mode gre remote {remote} local 128.111.5.228 ttl 255")
        runCommandAsRoot(f"ip link set {interfaceName} netns ns1")
        runCommandAsRoot(f"ip netns exec ns1 ip addr add {greAddr} dev {interfaceName}")
        runCommandAsRoot(f"ip netns exec ns1 ip link set {interfaceName} up")
        print(f"iptunnel add {interfaceName} mode gre remote {remote} local 128.111.5.228 ttl 255")
        print(f"ip link set {interfaceName} netns namespace1")
        print(f"ip netns exec ns1 ip addr add {greAddr} dev {interfaceName}")
        print(f"ip netns exec ns1 ip link set {interfaceName} up")



def deleteServerSideGreTunnel(tunnel_info_file):
    file = open(tunnel_info_file, 'r')
    tunnel_info = json.load(file)
    greCounter = 10
    cnt = 0
    for node in tunnel_info:

        greCounter +=1
        interfaceName = "gre" + str(greCounter)
        runCommandAsRoot(f"ip netns exec ns1 ip link set {interfaceName} netns 1")
        runCommandAsRoot(f"ip link del {interfaceName}")

if __name__ == "__main__":
    createServerSideTunnel('TunnelInfo.json')