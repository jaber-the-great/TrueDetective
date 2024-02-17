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



def movePcapCICToUserLabel(userGroupFileName, interface):
    pcapDir = "/mnt/md0/jaber/groupedPcap/"
    cicDir = "/mnt/md0/jaber/groupedCIC/"

    file = open(userGroupFileName)
    userGroup = json.load(file)
    cnt = 0
    for user , flows in userGroup.items():
        try:
            cnt +=1
            if cnt% 1000 == 0:
                print(cnt)


            call(f'mkdir -p {pcapDir}{user}', shell=True)
            call(f'mkdir -p {cicDir}{user}', shell=True)
            for file in flows:
                file = file.split('/')[-1]
                cicFile = "/home/jaber/editedcicflow/" + interface + "/" + file
                call(f'cp {cicFile} {cicDir}{user}/', shell=True)
                pcapFile = "/mnt/md0/jaber/new15min/" + interface + "/" + file
                pcapFile = pcapFile.removesuffix("_Flow.csv")
                call(f'cp {pcapFile} {pcapDir}{user}/', shell=True)
        except:
            print(f'{user} Had some errors')



# /mnt/md0/jaber/new15min/s2f0

if __name__ == "__main__":

    interfaceList = ['s2f0', 's2f1', 's2f2', 's2f3', 's3f0' , 's3f1' ,'s3f2' ,'s3f3' ]
    argList = []
    for interface in interfaceList:
        file = f'/home/jaber/userGroups/UserFlows_{interface}.json'
        argList.append((file,interface))

    _paralell_process(movePcapCICToUserLabel, argList)

