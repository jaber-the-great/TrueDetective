import pandas as pd
from multiprocessing import Pool
import os
from collections import defaultdict
from subprocess import call
import subprocess

def removePcaps(input_dir):

    cnt = 0
    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".pcap"):
                pcapfile = subdir + '/' + file

                output = subprocess.getoutput(f'tcpdump -r {pcapfile} 2>/dev/null | wc -l')
                print(f'here is the {output}')

                cnt +=1
                print(cnt)
                if cnt > 100:
                    break


if __name__ == "__main__":
    pcapDir = "/home/jaber/TrueDetective/15minFirst"
    removePcaps(pcapDir)