l# Splitting the pacps 
# CICFlowMeter has momory error when a single pcap file is too big
from pcap_splitter.splitter import PcapSplitter
import time
from multiprocessing import Pool, Process, current_process
import functools
import subprocess
import shlex
import os


def split(input_file, output_folder):
    ps = PcapSplitter(input_file)
    start = time.time()
    print(ps.split_by_session(output_folder))
    end = time.time()
    print("Time taken is :", start - end)

#input_file = "/mnt/md0/jaber/gateway/inputpcap"
input_file = "output1.pcap"
#output_folder = "/mnt/md0/jaber/gateway/outputpcap/"
output_folder = "/home/jaber/"
split(input_file, output_folder)
