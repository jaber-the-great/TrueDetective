import pandas as pd
from multiprocessing import Pool
import os
from collections import defaultdict
from subprocess import call
import subprocess

def removePcaps(input_dir, numOfPackets):

    cnt = 0
    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".pcap"):

                pcapfile = subdir + '/' + file
                output = subprocess.getoutput(f'tcpdump -r {pcapfile} 2>/dev/null | wc -l')
                if int(output) < numOfPackets:
                    output = subprocess.getoutput(f'rm {pcapfile}')



def feedPcaps(inputDir, numOfPackets):
    arg_list = []
    for i in range(1, 766):
        lst = (inputDir + '/' + str(i), numOfPackets)
        arg_list.append(lst)
    print(arg_list)


    _paralell_process(removePcaps, arg_list)


def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)


if __name__ == "__main__":
    pcapDir = "/home/jaber/new15min/"
#    dirs = ["s2f0", "s2f1", "s2f2", "s2f3", "s3f0", "s3f1", "s3f2", "s3f3"]
    dirs = ['s3f0']
    arg_list = []
    for item in dirs:
        lst = (pcapDir + item, 2)
        arg_list.append(lst)

    print(arg_list)
    _paralell_process(removePcaps, arg_list)


    minimumNumberOfPackets = 2
    #removePcaps(pcapDir, minimumNumberOfPackets)
    #feedPcaps(pcapDir, minimumNumberOfPackets)
    # groupingPcapsforPP("a", "b")
