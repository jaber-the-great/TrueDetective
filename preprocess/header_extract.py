import scapy.layers.tls.record
from scapy.utils import *
from scapy import *
import pandas as pd
from multiprocessing import Pool
import ipaddress
import warnings
from scapy.main import load_layer
# from torch.multiprocessing.pool import Pool
import subprocess
from subprocess import call
import logging
logging.getLogger("scapy.runtime").setLevel(logging.ERROR)
import sys
sys.stderr = os.devnull


numberOfICMP = 0
numberofIPOptions = 0
def convert_int_to_byte(value,number_of_bytes):

    res = value.to_bytes(number_of_bytes,'big')
    return res


def checkInternalIP(IP):
    networks = ['128.111.0.0/16' ,  '169.231.0.0/16' , '192.150.216.0/23' , '92.35.222.0/24' , '199.120.153.0/24']
    for network in networks:
        if ipaddress.ip_address(IP) in ipaddress.ip_network(network):
            return True
    return False

def HeaderExtract(input_pcap, output_dir, numOfPackets):
    global numberOfICMP
    global  numberofIPOptions
    # Reading the pcap file
    flow =rdpcap(input_pcap)
    ipHeaders = []
    transportHeaders = []
    counter = 0
    for pkt in flow:
        try:
            if pkt.haslayer("IP"):
                if len(pkt["IP"].options) > 0:
                    print("########################################################################################")
                    print(f'IP option exists for file {input_pcap}')
                    numberofIPOptions += 1

            if pkt.haslayer("TCP"):
                transportHeader = list(bytes(pkt["TCP"]))[:20]

            elif pkt.haslayer("UDP"):
                transportHeader = list(bytes(pkt["UDP"]))[:8]
            elif pkt.haslayer("ICMP"):
                numberOfICMP+=1
                print("############################################################################################")
                print(f'ICMP detected for file {input_pcap}')
                return

            else:
                continue
            ipHeader = list(bytes(pkt["IP"]))[:20]
            # Padding with zero to match the number of bytes required
            ipHeader.extend([0] * (20 - len(ipHeader)))
            transportHeader.extend([0] * (20 - len(transportHeader)))
            ipHeaders.append(ipHeader)
            transportHeaders.append(transportHeader)
            counter+=1
            if counter == numOfPackets:
                # Change the directory from number to user separated based on ip address
                break


        except:
            print(f'Problem with pcap file {input_pcap}')
    if counter > 1:
        save_data(ipHeaders, transportHeaders, output_dir , counter, numOfPackets)



def save_data(ipHeaders, transportHeaders, output_loc , headerCount, MaxHeader):
    try:
        result = []
        if not ipHeaders or not transportHeaders:
            return
        for i in range(len(transportHeaders)):
            result.extend(ipHeaders[i])
            result.extend(transportHeaders[i])
        if len(transportHeaders) < MaxHeader:
            paddingLength = MaxHeader - len(transportHeaders)
            paddingLength*= (len(transportHeaders[0]) + len(transportHeaders[1]) )
            result.extend([0] * paddingLength)
        result = pd.DataFrame(result).T
        result.to_csv(output_loc,mode = 'w',index=False, header=False)
    except:
        print(f'Error saving {output_loc}')



def feed_packets(input_dir, output_dir, numOfPackets):
    arg_list = []
    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".pcap"):
                

                outputSubdir = subdir.split('/')[-1]
                newName = file.split('.')[0] + ".csv"
                lst = ( input_dir + '/' + file, output_dir +  '/' + outputSubdir +'/' + newName, numOfPackets)
                arg_list.append(lst)

                # determine_handshake(subdir + '/' + file, "alaki")

                # HeaderExtract(subdir+'/' + file , output_dir+ '/' + section + '/' + newName, NumOfBytes)
    # # Parallel processing of all pcap files and converting them to csv
    # print(arg_list)
    _paralell_process(HeaderExtract, arg_list)
def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)


def feedPacketsPerUser(input_dir, output_dir, numOfPackets):
    arg_list = []
    # Creating directory for each user in output_dir
    for subdir, dirs, files in os.walk(input_dir):
        if dirs == []:
            break
        for dir in dirs:
            call(f'mkdir -p {output_dir}{dir}', shell=True)
    print("finished")
    cnt = 0
    for subdir, dirs, files in os.walk(input_dir):

        for file in files:
            if file.endswith(".pcap"):
                cnt+=1
                user = subdir.split('/')[-1]
                filename = f'{output_dir}{user}/{file}.csv'
                # filename = output_dir + "/" + file + ".npt"
                lst = (subdir + "/" + file, filename, numOfPackets)
                arg_list.append(lst)
    # print(arg_list)
    _paralell_process(HeaderExtract, arg_list)


def byte_extract_tls(packet, NumberOfBytes):
    tls = packet.payload.payload.payload
    # 5 for messageType(22)-1byte, Version-2bytes, msgLength-2bytes
    pktLenght = tls.len + 5
    # considering number of bytes is 600 in this case(for curtain)
    if pktLenght < NumberOfBytes:
        # Only getting the bytes for the first tls
        data = list(bytes(tls))[:pktLenght]
        # Padding with zero to match the number of bytes required
        data.extend([0] * (NumberOfBytes - pktLenght))
    else:
        data = list( bytes(tls)[:NumberOfBytes])
    return data
if __name__=="__main__":
    # numberOfBytesToExtract = 12
    # PacketsPerflow = 3
    # inputFiles = "/home/jaber/TrueDetective/filteredPcaps/"
    # outputDirectory = "/home/jaber/TrueDetective/headers"
    # for i in range(1,746):
    #     print(i)
    #     feed_packets(inputFiles + str(i),outputDirectory, PacketsPerflow)
#     HeaderExtract("/home/jaber/TrueDetective/pcaps/file-0001.pcap","/home/jaber/TrueDetective/test.csv" )
    SeveralHeaderExtract("/home/jaber/TrueDetective/s2f0_00000_20221205201501-0004.pcap" , "/home/jaber/TrueDetective/", 3)


if __name__=="__main__":
    warnings.filterwarnings("ignore")
    PacketsPerflow = 5
    inputDirectory = "/mnt/md0/jaber/groupedPcap/"
    outputDirectory = "/mnt/md0/jaber/groupedHeader/"
    feedPacketsPerUser(inputDirectory,outputDirectory, PacketsPerflow)
    print(f'Number of ICMP is {numberOfICMP} and number of IP packtets with option is {numberofIPOptions}')
    # HeaderExtract("/mnt/md0/jaber/groupedPcap/169.231.6.145/s2-2022-12-05-1200-ens4f0-2022-12-05-1215=5-109205.pcap", "/home/jaber/", PacketsPerflow)
