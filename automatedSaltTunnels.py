#TODO: read the json files, loop through it, get the paprameters and run the commands 
import subprocess
import json
def runCommandAsRoot(command):
    command = "sudo " + command  # Include -S option

    try:
        # Run the command with sudo, providing the password via stdin
        result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, check=True)
    
        # Print the command's output
        print("Command output:")
        print(result.stdout)
    
    except subprocess.CalledProcessError as e:
        print(f"Error running the command: {e}")
        print("Error output:")
        print(e.stderr)

file = open('TunnelInfo.json','r')
tunnel_info = json.load(file)


for node in tunnel_info:
    PublicIP = tunnel_info[node]["pinotPublicIP"]
    MINION=node
    TunnleIP=tunnel_info[node]["pinotTunnelAddr"]

# MINION="raspi-e4:5f:01:56:d9:0a"
# PublicIP="169.231.63.211"
#TunnelIP="192.168.1.6/30"
    TABLE='docToGre'
    runCommandAsRoot(f'salt {MINION} cmd.run "sudo modprobe ip_gre"')
    runCommandAsRoot(f'salt {MINION} cmd.run "lsmod | grep gre"')
    runCommandAsRoot(f'salt {MINION} cmd.run "sudo apt install -y iptables iproute2"')
    runCommandAsRoot(f'salt {MINION} cmd.run "sudo echo \'net.ipv4.ip_forward=1\' >> /etc/sysctl.conf"')
    runCommandAsRoot(f'salt {MINION} cmd.run "sudo sysctl -p"')
    runCommandAsRoot(f'salt {MINION} cmd.run "sudo iptunnel add gre1 mode gre remote 128.111.5.228 local {PublicIP} ttl 255"')
    runCommandAsRoot(f'salt {MINION} cmd.run "sudo ip addr add {TunnleIP} dev gre1"')
    runCommandAsRoot(f'salt {MINION} cmd.run "sudo ip link set gre1 up"')
    runCommandAsRoot(f'salt {MINION} cmd.run "lsmod | grep gre"')
    runCommandAsRoot(f'salt {MINION} cmd.run "lsmod | grep gre"')
    runCommandAsRoot(f'salt {MINION} cmd.run "lsmod | grep gre"')
    break
    print(PublicIP)
    print(TunnleIP)


# sudo salt $MINION cmd.run "sudo apt install -y iptables iproute2"
# sudo salt $MINION cmd.run "sudo apt install -y iptables iproute2"
# sudo salt $MINION cmd.run "sudo sysctl -p"
# sudo salt $MINION cmd.run "sudo iptunnel add gre1 mode gre remote 128.111.5.228 local $PublicIP ttl 255"
# sudo salt $MINION cmd.run "sudo ip addr add $TunnelIP dev gre1"
# sudo salt $MINION cmd.run "sudo ip link set gre1 up"
# sudo salt $MINION cmd.run "echo '300\t$TABLE' >> /etc/iproute2/rt_tables"
# sudo salt $MINION cmd.run "sudo ip route add default dev gre1 table $TABLE"
# sudo salt $MINION cmd.run "sudo ip rule add from 172.59.0.0/16 table $TABLE"
#
# sudo salt $MINION cmd.run "docker network create --subnet $Subnet greBr"