from multiprocessing import Pool
import os
from subprocess import call
import subprocess


def iterateAndRunTranlyzer(input_dir,outputDir):
    cnt = 0

    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".pcap"):
                user = subdir.split('/')[-1]
                command = f'tranalyzer -r {subdir}/{file} -w {outputDir}/'
                print(command)
                call(command,shell=True)

                if cnt > 2:
                    break
                # Todo: create the output direcotry --> is it needed or it would automatically create
                # TODO: run tranalyzer code here




def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)

if __name__ == "__main__":
    iterateAndRunTranlyzer('/mnt/md0/jaber/nprintPcap/10','/mnt/md0/jaber/tran')
