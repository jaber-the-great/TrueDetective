import scapy.layers.tls.record
from scapy.utils import *
from scapy import *
import pandas as pd
from multiprocessing import Pool
from scapy.main import load_layer
# from torch.multiprocessing.pool import Pool
import subprocess
def convert_int_to_byte(value,number_of_bytes):

    res = value.to_bytes(number_of_bytes,'big')
    return res
def HeaderExtract(input_pcap, output_dir):
    # Reading the pcap file
    flow =rdpcap(input_pcap)
    ipHeaders = []
    transportHeaders = []
    counter = 0
    for pkt in flow:
        if pkt.haslayer("TCP"):
            transportHeader = list(bytes(pkt["TCP"]))[:20]

        elif pkt.haslayer("UDP"):
            transportHeader = list(bytes(pkt["UDP"]))[:8]
        else:
            continue
        ipHeader = list(bytes(pkt["IP"]))[:20]
        # Padding with zero to match the number of bytes required
        ipHeader.extend([0] * (20 - len(ipHeader)))
        transportHeader.extend([0] * (20 - len(transportHeader)))
        ipHeaders.append(ipHeader)
        transportHeaders.append(transportHeader)
        counter+=1
        if counter > 0:
            break


    save_data(ipHeaders, transportHeaders,output_dir)


def SeveralHeaderExtract(input_pcap, output_dir, numOfPackets):

    # Reading the pcap file
    flow =rdpcap(input_pcap)
    ipHeaders = []
    transportHeaders = []
    counter = 0
    for pkt in flow:
        if pkt.haslayer("TCP"):
            transportHeader = list(bytes(pkt["TCP"]))[:20]

        elif pkt.haslayer("UDP"):
            transportHeader = list(bytes(pkt["UDP"]))[:8]
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
            save_data(ipHeaders, transportHeaders, output_dir)
            return
def save_data(ipHeaders, transportHeaders, output_loc ):


    result = []
    if not ipHeaders or not transportHeaders:
        return
    for i in range(len(transportHeaders)):
        result.extend(ipHeaders[i])
        result.extend(transportHeaders[i])

    result = pd.DataFrame(result).T
    result.to_csv(output_loc,mode = 'w',index=False, header=False)


def feed_packets(input_dir, output_dir, numOfPackets):
    arg_list = []
    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".pcap"):
                newName = file.split('.')[0] + ".csv"

                outputSubdir = subdir.split('/')[-1]
                lst = ( input_dir + '/' + file, output_dir +  '/' + outputSubdir +'/' + newName, numOfPackets)
                arg_list.append(lst)

                # determine_handshake(subdir + '/' + file, "alaki")

                # HeaderExtract(subdir+'/' + file , output_dir+ '/' + section + '/' + newName, NumOfBytes)
    # # Parallel processing of all pcap files and converting them to csv
    # print(arg_list)
    _paralell_process(SeveralHeaderExtract, arg_list)
def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)
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

