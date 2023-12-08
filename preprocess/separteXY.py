import scapy.layers.tls.record
from scapy.utils import *
from scapy import *
import pandas as pd
from scapy.layers.tls.record import TLS
from scapy.main import load_layer
# from torch.multiprocessing.pool import Pool

"""       hello_request(0), client_hello(1), server_hello(2),
          certificate(11), server_key_exchange (12),
          certificate_request(13), server_hello_done(14),
          certificate_verify(15), client_key_exchange(16),
          finished(20), (255)
          HandShakeType = {1:"client_hello", 2:"server_hello", 11:"certificate"}
"""


def convert_int_to_byte(value,number_of_bytes):
    """

    Args:
        value: the filed value to be converted
        number_of_bytes: number of bytes assigned to that filed -->
        padding with zero if required

    Returns: A list of bytes

    Converts the TLS filed to a list of bytes
    Every byte would be used as a column in dataset for training the model
    """
    res = value.to_bytes(number_of_bytes,'big')
    return res


def handshake_extract(input_pcap, output_dir, NumOfBytes):
    """

    Args:
        input_pcap: The pcap file for each flow
        output_dir: The location in which you desire to store the result

    Returns: Nothing
    Get the first n bytes of p client hello message sand q server hello messages
    in a flow. Store the result in a csv file. each byte of these n bytes are converted
    to an integer and would be a column in .csv file

    """
    # Reading the pcap file
    flow =rdpcap(input_pcap)
    # Counters for number of server hello and client hello messages in each flow
    server_cnt = 0
    client_cnt =0

    # Each element of the following list contain the first n bytes of server hello
    # and client hello message. Each element is represented as a list of integers(each
    # integer is one byte)
    client_headers = []
    server_headers = []

    # Iterating through the packets in pcap file
    for pkt in flow:
        # Checking the packet is TCP or not, if yes, it may have TLS header
        if pkt.haslayer("TCP"):
            # Check the type of payload is TLS or not
            if type(pkt.payload.payload.payload ) == scapy.layers.tls.record.TLS:
                # If type of TLS packet is client hello, retrieve the first n bytes
                if type(pkt.payload.payload.payload.msg[0]) == scapy.layers.tls.handshake.TLSClientHello:
                    # Define the max number of client hello messages you want to consider
                    client_cnt +=1
                    if client_cnt < 2:
                        data = byte_extract_tls(pkt,NumOfBytes)
                        client_headers.append(data)

                    # msgType = pkt.payload.payload.payload.type # 1 byte,
                    # version = pkt.payload.payload.payload.version # 2 byte
                    # version = convert_int_to_byte(version,2)
                    # length = pkt.payload.payload.payload.len # 2 byte
                    # length = convert_int_to_byte(length,2)
                    # HelloType = pkt.payload.payload.payload.msg[0].msgtype # 1 byte
                    # hellolength = pkt.payload.payload.payload.msg[0].msglen  # 3 Byte
                    # hellolength = convert_int_to_byte(hellolength,3)
                    # HelloVersion = pkt.payload.payload.payload.msg[0].version # 2 byte
                    # HelloVersion = convert_int_to_byte(HelloVersion,2)
                    # lastbyte = bytes(pkt.payload.payload.payload)[11]
                    # lastbyte = convert_int_to_byte(lastbyte,1)
                    #
                    # # Creating a list of first 12 bytes in integer form ( a list of lenght = 12)
                    # first12 = [ msgType,version[0], version[1],length[0], length[1],HelloType,hellolength[0],
                    # hellolength[1], hellolength[2],HelloVersion[0], HelloVersion[1],lastbyte[0]]
                    # client_headers.append(first12)
                # If type of TLS packet is server hello, retrieve the first n bytes
                elif type(pkt.payload.payload.payload.msg[0]) == scapy.layers.tls.handshake.TLSServerHello:
                    # Define the max number of server hello messages you want to consider
                    server_cnt +=1
                    if client_cnt < 2:
                        data = byte_extract_tls(pkt,NumOfBytes)
                        server_headers.append(data)
                    # msgType = pkt.payload.payload.payload.type # 1 byte
                    # version = pkt.payload.payload.payload.version # 2 byte
                    # version = convert_int_to_byte(version,2)
                    # length = pkt.payload.payload.payload.len # 2 byte
                    # length = convert_int_to_byte(length,2)
                    # HelloType = pkt.payload.payload.payload.msg[0].msgtype # 1 byte
                    # hellolength = pkt.payload.payload.payload.msg[0].msglen  # 3 Byte
                    # hellolength = convert_int_to_byte(hellolength,3)
                    # HelloVersion = pkt.payload.payload.payload.msg[0].version # 2 byte
                    # HelloVersion = convert_int_to_byte(HelloVersion,2)
                    # lastbyte = bytes(pkt.payload.payload.payload)[11]
                    # lastbyte = convert_int_to_byte(lastbyte, 1)
                    # # Creating a list of first 12 bytes in integer form ( a list of lenght = 12)
                    # first12 = [ msgType,version[0], version[1],length[0], length[1],HelloType,hellolength[0],
                    # hellolength[1], hellolength[2],HelloVersion[0], HelloVersion[1],lastbyte[0]]
                    # server_headers.append(first12)
    # Saving the data into a file

    save_data(client_headers, server_headers,output_dir)
def save_data(clientRecords, serverRecords, output_loc ):
    """

    Args:
        clientRecords: The first x bytes of n client hello messages
        which is presented in 2D list ( each sublist has lenght x)
        serverRecords: The first x bytes of m server hello messages
        which is presented in 2D list ( each sublist has lenght x)
        output_loc: Determines the directory and name of the file in one string

    Returns: Nothing
    Saves the input data to a file

    """

    result = []
    if not clientRecords or not serverRecords:
        return
    for recrod in clientRecords:
        result.extend(recrod)
    for recrod in serverRecords:
        result.extend(recrod)
    result = pd.DataFrame(result).T
    result.to_csv(output_loc,mode = 'w',index=False, header=False)


def feed_packets(outer_input_dir, output_dir, NumOfBytes):
    arg_list = []
    for idx in range(11):
        input_dir = outer_input_dir+"/"+str(idx)
        for subdir, dirs, files in os.walk(input_dir):
            # Goes through the root directory and for each subdirectory:
            # finds all the files in that subdirectory
            for file in files:
                if file.endswith(".pcap"):
                    # used for the location of output file (csvs of each lable are in different subdir)
                    section = subdir[-1]
                    # Changing the name of the file
                    newName = file.split('.')[0] + ".csv"
                    # Creating a tuple for input and output file
                    lst = (subdir + '/' + file, output_dir + '/' + section + '/' + newName)
                    #adding the input file loc and output file loc to the list
                    arg_list.append(lst)
                    # determine_handshake(subdir + '/' + file, "alaki")
                    # breakpoint()
                    handshake_extract(subdir+'/' + file , output_dir+ '/' + section + '/' + newName, NumOfBytes)
    # Parallel processing of all pcap files and converting them to csv
    print(arg_list)
    #_paralell_process(determine_handshake, arg_list)

# def _paralell_process(func, input_args, cores=0):
#     if cores == 0:
#         cores = os.cpu_count()
#     with Pool(cores) as p:
#         return p.starmap(func, input_args)



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
    numberOfBytesToExtract = 12
    inputFiles = "/data/UCSB_Labelled_ET-BERT"
    outputDirectory = "/data/UCSBHandshake"
    feed_packets(inputFiles,outputDirectory , numberOfBytesToExtract)
    #handshake_extract("/home/jaber/UCSB_labelled_Pcaps/0/2022-12-02-1400-merged-1010811.pcap", "/home/jaber/bytes.csv",600)