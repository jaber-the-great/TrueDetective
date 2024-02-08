import pandas as pd
from multiprocessing import Pool
import os
from collections import defaultdict
from subprocess import call
import json

def feed_packets(UserFlowsDict, output_dir):

    arg_list = []

    for key,value in UserFlowsDict.items():

        lst = ( key, value, output_dir)
        arg_list.append(lst)
    print(arg_list)
    _paralell_process(moveUserFlowsToDirectory, arg_list)

def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)

# go through the files and find user ID
# chose user ID as dictionary key and add the file name to it
# crate directory with userID and move all the items to that
def FindUsersFlows(input_dir):
    UsersFlows = defaultdict(list)
    cnt = 0
    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            try:
                if file.endswith(".csv"):

                    df = pd.read_csv(subdir + '/'+ file)
                    label = df.loc[0, "User IP"]
                    UsersFlows[label].append(subdir + '/'+ file)
                    cnt +=1


            except:
                print(subdir + '/'+ file)
                continue
        print(subdir)

    with open('/mnt/md0/jaber/groupings.json', 'w') as jsonfile:
        json.dump(dict(UsersFlows), jsonfile)
    return UsersFlows


def userFlowsToDictionary(input_dir):
    groupings = {}
    cnt = 0
    for subdir, dirs, files in os.walk(input_dir):
        flows = []
        cnt +=1 
        print(cnt)
        for file in files:
            try:
                if file.endswith(".csv"):
                    flows.append(file)

            except:
                print(subdir + '/'+ file)
                continue
        if flows!= []:
            groupings[subdir.split('/')[-1]] = flows
    with open('/mnt/md0/jaber/groupings.json', 'w') as jsonfile:
        json.dump(dict(groupings), jsonfile)
    return groupings

def moveUserFlowsToDirectory(userLabel, files, output_dir):
    call(f'mkdir {output_dir}/{userLabel}', shell=True)
    for file in files:
        call(f'cp {file} {output_dir}/{userLabel}/', shell=True)


if __name__ == "__main__":


    # inputDir = "/mnt/md0/jaber/mergedCICandHeader"
    # userFlowsToDictionary(inputDir)

    # feed_packets(UserFlows, outputDir)

    file = open("/mnt/md0/jaber/groupings.json")
    Lengths = []
    UserFlow = json.load(file)
    # counter = 0
    # arg_list = []
    # for key, value in UserFlow.items():
    #     counter+=1
    #     print(counter)
    #     moveUserFlowsToDirectory(key, value, outputDir)


    for key in UserFlow:
        Lengths.append(len(UserFlow[key]))
    Lengths.sort()
    print(Lengths)
    print(len(Lengths))
    print(sum(Lengths))
    print(sum(Lengths)//len(Lengths))
    import statistics
    print(statistics.median(Lengths))
