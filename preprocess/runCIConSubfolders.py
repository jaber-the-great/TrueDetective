import pandas as pd
from multiprocessing import Pool
import os

from subprocess import call
import subprocess

def batchCommands():
    arg_list = []
    for i in range(9,746):
        command = 'docker run -v /home/jaber/TrueDetective/filteredPcaps/'+ str(i) + ':/tmp/server_ndt -v /home/jaber/TrueDetective/cicFilteredPcaps/' + str(i)+':/tmp/output/ --entrypoint /bin/bash --rm --memory=100g pinot.cs.ucsb.edu/cicflowmeter:latest -c \"ls /tmp/server_ndt/*.pcap | parallel java -Djava.library.path=/CICFlowMeter/jnetpcap/linux/jnetpcap-1.4.r1425/ -jar build/libs/CICFlowMeter-4.0.jar {} /tmp/output/\"'
        subprocess.run(command, shell=True)

    #     lst = (command,i)
    #     arg_list.append(lst)
    # print(arg_list)
    # DO NOT parallel process
    #_paralell_process(runCICDocker, arg_list)

def runCICDocker(command, folderName):
    print(folderName)
   # subprocess.run(command, shell=True)
    # don't use this
def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)

if __name__ == "__main__":
#    batchCommands()
    batchCommands()

