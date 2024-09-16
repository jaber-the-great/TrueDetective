import json
from multiprocessing import Pool
import os
from subprocess import call
import subprocess


def runTranlyzerUserGroups(input_dir,outputDir):
    cnt = 0
    argList = []
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
                command = f'tranalyzer -r {subdir}/{file} -w {outputDir}/{user}/ >  /dev/null 2> output_errors.txt'
                # print(command)
                # call(command, shell=True)
                argList.append([command])
                cnt +=1
                print(cnt)
                if cnt > 1000:
                    _paralell_process(runCommand, argList)
                    return

def runTranalzyserPairsDataset(usersPairFile, outputDir):
    argList = []
    cnt = 0
    subDir = usersPairFile.split('/')[-1]
    call(f'mkdir -p {outputDir}/{subDir}', shell=True)
    file = open(usersPairFile)
    userPairs = json.load(file)
    for user , pairs in userPairs.items():
        cnt +=1
        print(cnt)

        for pair in pairs:
            pair[0] = pair[0].replace('_Flow.csv', '')
            pair[1] = pair[1].replace('_Flow.csv', '')
            command1 = f'tranalyzer -r /mnt/md0/jaber/groupedPcap/{pair[0]} -w {outputDir}/{subDir}/{user}/ > /dev/null 2> tranalyzer_errors.txt'
            command2 = f'tranalyzer -r /mnt/md0/jaber/groupedPcap/{pair[1]} -w {outputDir}/{subDir}/{user}/ > /dev/null 2> tranalyzer_errors.txt'
            argList.append([command1])
            argList.append([command2])

    _paralell_process(runCommand, argList)

def runCommand(cmd):
    call(cmd, shell=True)


def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)

if __name__ == "__main__":
    iterateAndRunTranlyzer('/mnt/md0/jaber/nprintPcap/10','/mnt/md0/jaber/tran')

    #runTranlyzerUserGroups('/mnt/md0/jaber/groupedPcap','/mnt/md0/jaber/groupedTranalyze')
    runTranalzyserPairsDataset('/mnt/md0/jaber/pairsDatasets/0_5.json_AllRandom' , '/mnt/md0/jaber/groupedTranalyze')
