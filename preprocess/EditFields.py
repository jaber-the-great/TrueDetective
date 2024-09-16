import subprocess

import pandas as pd
from multiprocessing import Pool
import os
import ipaddress
import warnings
from pandas.errors import SettingWithCopyWarning
import hashlib
import shortuuid
from collections import defaultdict 
import json

def short_hash_ip(ip_address):
    # Create a hashlib object with SHA-256 algorithm
    hasher = hashlib.sha256()
    # Update the hasher with the bytes representation of the IP address
    hasher.update(ip_address.encode('utf-8'))
    # Get the hexadecimal digest of the hash
    full_hash = hasher.hexdigest()
    # Use shortuuid to convert the full hash to a shorter UUID
    short_hash = shortuuid.uuid(name=full_hash)[:8]
    return short_hash

def checkInternalIP(IP):
    networks = ['128.111.0.0/16' ,  '169.231.0.0/16' , '192.150.216.0/23' , '192.35.222.0/24' , '199.120.153.0/24']
    for network in networks:
        if ipaddress.ip_address(IP) in ipaddress.ip_network(network):
            return True
    return False

def feed_packets(input_dir, output_dir):
    arg_list = []
    cnt = 0
    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".csv"):
                lst = ( input_dir + '/' + file, output_dir + '/' + file)
                arg_list.append(lst)
                cnt +=1
    #print(arg_list)
    _paralell_process(groupAndEditCICFlow, arg_list)

def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)

def groupAndEditCICFlow(input_dir, output_dir):


    bothInternal = 0
    userFlows = defaultdict(list)
    internalFlows = defaultdict(list)
    cnt = 0

    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".csv"):
                cnt +=1
                if cnt%1000 ==0:
                    print(cnt)
                inputFile = input_dir + '/' + file
                outputFile = output_dir + '/' + file
                try:
                    BothInternalFlows = 0
                    flow = pd.read_csv(inputFile)
                    # Skips the cics with no information which correspond to flows with only one packet.
                    # Then remove it later on (do the grouping on edited cicflow)
                    if flow.empty:
                        continue
                    flow =flow.head(1)
                    srcIP = flow.loc[0, 'Src IP']
                    dstIP = flow.loc[0, 'Dst IP']
                    # if src is internal ip, the direction is 1, otherwise the direction is zero
                    direction = 0
                    if checkInternalIP(srcIP):
                        if checkInternalIP(dstIP):
                            # Both are internal IPs
                            BothInternalFlows= 1
                            internalFlows[srcIP].append([inputFile,dstIP])
                        InternalIP = srcIP
                        ExternalIP = dstIP
                        direction = 1
                    else:
                        InternalIP = dstIP
                        ExternalIP = srcIP
                        direction = 0
                    if not BothInternalFlows:
                        userFlows[InternalIP].append(inputFile)
                    del flow['Src IP']
                    del flow['Dst IP']
                    flow['Direction'] = direction
                    flow['Outside IP'] = ExternalIP
                    flow['User IP'] = InternalIP
                    flow['Internal traffic'] = bothInternal
                    #fileName = inputFile.split('/')[-1]
                    flow['Flow ID'] = file
                    flow.to_csv(outputFile,index=False)
                except:
                    print(inputFile)
    if not BothInternalFlows:
        userFlows[InternalIP].append(inputFile)
    index = input_dir.split('/')[-1]
    with open(f'/home/jaber/userGroups/UserFlows_{index}.json', "w") as jsonfile:
        json.dump(userFlows,jsonfile)
    with open(f'/home/jaber/userGroups/InternalFlows_{index}.json', "w") as jsonfile:
        json.dump(internalFlows,jsonfile)

if __name__ == "__main__":
    warnings.simplefilter(action='ignore', category=(SettingWithCopyWarning))
    dirs =  ['s2f0', 's2f1', 's2f2', 's2f3', 's3f0' , 's3f1' ,'s3f2' ,'s3f3' ]
    arglist = []
    for item in dirs:
        arg = ("/home/jaber/cic/" + item , "/home/jaber/editedcicflow/" + item)
        arglist.append(arg)
    print(arglist)
    _paralell_process(groupAndEditCICFlow, arglist)
    # groupAndEditCICFlow("/home/jaber/cic/s3f1" , "/home/jaber/editedcicflow/s3f1")
    # readAndEditFlow("/home/jaber/TrueDetective/cicFilteredPcaps/1/s3-2022-04-29-1215-ens4f1-2022-04-29-1215-365895.pcap_Flow.csv" ,"/home/jaber/TrueDetective/editedCICFilteredPcaps/1")
    # for i in range(1,112):
    #     print(i)
    #     feed_packets("/mnt/md0/jaber/cicflow/"+ str(i) , "/mnt/md0/jaber/editedcicflow/"+ "/" + str(i))
    #     #subprocess.getoutput(f'mkdir /mnt/md0/jaber/editedcicflow/{str(i)}')
    # feed_packets("/home/jaber/cic/s3f1" , "/home/jaber/editedcicflow/s3f1")
