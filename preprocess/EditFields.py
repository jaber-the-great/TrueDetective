import subprocess

import pandas as pd
from multiprocessing import Pool
import os
import ipaddress
import warnings
from pandas.errors import SettingWithCopyWarning
import hashlib
import shortuuid

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
    networks = ['128.111.0.0/16' ,  '169.231.0.0/16' , '192.150.216.0/23' , '92.35.222.0/24' , '199.120.153.0/24']
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
    _paralell_process(readAndEditFlow, arg_list)

def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)

def readAndEditFlow(inputFile, outputFile):

    # reading the file
    try:
        flow = pd.read_csv(inputFile)
        flow =flow.head(1)
        srcIP = flow.loc[0, 'Src IP']
        dstIP = flow.loc[0, 'Dst IP']
        # Using hash of internal IP as user label and destination IP as the service
        # if src is internal ip, the direction is 1, otherwise the direction is zero
        direction = 0
        if checkInternalIP(srcIP):
            srcHash = short_hash_ip(srcIP)
            outsideIP = dstIP
            direction = 1
        else:
            srcHash = short_hash_ip(dstIP)
            outsideIP = srcIP
            direction = 0
        del flow['Src IP']
        del flow['Dst IP']
        flow['Direction'] = direction
        flow['Outside IP'] = outsideIP
        flow['User Label'] = srcHash

        flow.to_csv(outputFile,index=False)
    except:
        print(inputFile)
        # subprocess.getoutput(f"rm {inputFile}")



if __name__ == "__main__":
    warnings.simplefilter(action='ignore', category=(SettingWithCopyWarning))

    # readAndEditFlow("/home/jaber/TrueDetective/cicFilteredPcaps/1/s3-2022-04-29-1215-ens4f1-2022-04-29-1215-365895.pcap_Flow.csv" ,"/home/jaber/TrueDetective/editedCICFilteredPcaps/1")
    for i in range(1,746):
        print(i)
        feed_packets("/home/jaber/TrueDetective/cicFilteredPcaps/"+ str(i) , "/home/jaber/TrueDetective/editedCICFilteredPcaps"+ "/" + str(i))
        #subprocess.getoutput(f'mkdir /home/jaber/TrueDetective/editedCICFilteredPcaps/{str(i)}')