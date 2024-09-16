from integrationCommon import isIpv4Permitted, fixSubnet, NetworkGraph, NetworkNode, NodeType
import json


file = open('TunnelInfo.json','r')
tunnel_info = json.load(file)
# Creating empty network graph
net = NetworkGraph()

for node in tunnel_info:
     # tunnel_info[key] = { 'pinotPublicIP' : valid_nodes[key][0], 'serverTunnelAddr': server_tunnel_addr, 'pinotTunnelAddr' : 
     #                   pinot_tunnel , 'tunnelSubnet' : tunnel_subnet}
    name = node
    ip = tunnel_info[node]['pinotTunnelAddr'].split('/')[0]
    # The deviceID would be the last two part of the tunnel ip address on pinot side, eg 192.168.1.53 would have 153 as ID
    deviceID = "device-" + '.'.join(ip.split('.')[-2:])
    clientID = "client-" + '.'.join(ip.split('.')[-2:])

    # Creating the client
    customer = NetworkNode(
    id=clientID,
    displayName=clientID,
    type=NodeType.client,
    download=10, # Download is in Mbit/second
    upload=10 , # Upload is in Mbit/second
    # TODO: Change the location later
    address="1 My Road, My City, My State")
    net.addRawNode(customer)

    # Creating the device
        # Give them a device
    device = NetworkNode(
    id=deviceID,
    displayName= deviceID,
    parentId=clientID, # must match the customer's ID
    type=NodeType.device,
    ipv4= [ip],
    ipv6=[], 
    mac = ""
    )
    net.addRawNode(device)
net.prepareTree() # This is required, and builds parent-child relationships.
net.createNetworkJson() # Create `network.json`
net.createShapedDevices() # Create the `ShapedDevices.csv` file.
print("reached here)")