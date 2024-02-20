import pandas as pd
from multiprocessing import Pool
import os
from collections import defaultdict
from subprocess import call
import json
from collections import Counter
from itertools import combinations
import random
import ipaddress
import statistics
import numpy as np


def userFlowStats(userFlows):
    # Number of flows per user
    # Number of total users
    Lengths = []
    for key in userFlows:
        if len(userFlows[key]) < 2:
            continue
        Lengths.append(len(userFlows[key]))
        if not is_valid_ip(key):
            print("error")
            print(key)
    Lengths.sort()
    # print(Lengths)
    # Calculates how many users have n numbre of flows
    freq = Counter(Lengths)
    for item , frequency in freq.items():
        print(f"{item}: {frequency} times")
    print(statistics.mean(Lengths))
    print(statistics.median(Lengths))
    print(statistics.stdev(Lengths))
    print(min(Lengths))
    print(max(Lengths))
    print(sum(Lengths))

    with open("NumberofFlowsPerUser.txt", "w") as file:
        for item in Lengths:
            file.write(f'{item},')

    # with open("FreqOfNumFlowsPerUsers.json", "w") as jsonfile:
    #     json.dump(freq,jsonfile)


def datasetPairGenerate(userFlows, outputFile, numberOfUsers,  maxFlows):
    # Random sample: selection without replacement, but random choice is with replacement
    print(f'Generating pairs for {outputFile}')
    combinedList = {}
    pairsPerClass = int(maxFlows*(maxFlows+1)//2)
    cnt= 0



    sampledKeys = random.sample(list(userFlows.keys()), numberOfUsers)
    datasetUsers = {key: userFlows[key] for key in sampledKeys}
    userList =  list(datasetUsers.keys())
    weightList= [len(value) for value in datasetUsers.values()]



    for user , flows in datasetUsers.items():
        try:
            pairs = []
            for i in range(pairsPerClass):
                pair = random.sample(flows, 2)
                pair[0]=   user + '/'  + pair[0].split('/')[-1]
                pair[1] =   user + '/' + pair[1].split('/')[-1]
                pairs.append(pair + [1])


            for i in range(pairsPerClass):
                otherUser = random.choices(userList)[0]
                while otherUser == user:
                    otherUser = random.choices(userList)[0]

                pair = [random.choice(flows), random.choice(userFlows[otherUser]), 0]
                pair[0] = user + '/' + pair[0].split('/')[-1]
                pair[1] =  otherUser + '/' + pair[1].split('/')[-1]
                pairs.append(pair)
            combinedList[user] = pairs
        except:
            print(f'Error for user {user}')
    with open(f'{outputFile}' , "w") as jsonfile:
        json.dump(combinedList,jsonfile)

def groupOfDatasetGenerator(userFlowsFile, outputDir, minFlows, maxFlows):
    arg_list = []
    file = open(userFlowsFile)
    userFlows = json.load(file)
    usersToRemove = []
    for user , flows in userFlows.items():
        if len(flows) < minFlows:
            usersToRemove.append(user)
    for user in usersToRemove:
        del userFlows[user]

    for i in range(1, 50):
        numberOfUsers = random.randint(100, 2000)
        outputFile = f'{outputDir}/{i}_{numberOfUsers}.json'
        arg = (userFlows,outputFile,numberOfUsers , maxFlows)
        arg_list.append(arg)

    #print(arg_list)
    _paralell_process(datasetPairGenerate,arg_list)

def readAndMergePairFiles( userPairsFile, OutputFile):
    dataset = []
    cols = []
    file = open(userPairsFile)
    userPairs = json.load(file)

    cnt = 0
    for user , pairs in userPairs.items():
        cnt += 1
        print(cnt)
        for pair in pairs:
            try:

                firstFlow = pd.read_csv(f'/mnt/md0/jaber/groupedCIC/{pair[0]}')
                secFlow = pd.read_csv(f'/mnt/md0/jaber/groupedCIC/{pair[1]}')
                firstHeader = pd.read_csv(f'/mnt/md0/jaber/groupedHeader/{pair[0].replace("_Flow.csv", ".csv")}', header=None)
                secondHeader=  pd.read_csv(f'/mnt/md0/jaber/groupedHeader/{pair[1].replace("_Flow.csv", ".csv")}', header=None)
                label = pair[2]
                joined = firstFlow.values.tolist()[0] + firstHeader.values.tolist()[0] + secFlow.values.tolist()[0] + secondHeader.values.tolist()[0]+ [label]
                dataset.append(joined)
                if cols == []:
                    FirstCols = list(firstFlow.columns) + [ str(i) for i in range (len(firstHeader.columns))]

                    SecCols = list(secFlow.columns)
                    SecCols = ["x" + s for s in SecCols]
                    for i in range(len(secondHeader.columns)):
                        SecCols.append(f'x{str(i)}')

                    cols = FirstCols + SecCols   + ['label']

            except Exception as e:
                print(user)
                print(e)
                # print(pairs)

        df = pd.DataFrame(dataset , columns=cols)
        df.to_csv(OutputFile + ".json", index = False)

def parallelReadAndMergePairs(inputDir, outputDir):

    argList = []
    for subdir, dirs, files in  os.walk(inputDir):
        for file in files:
            if file.endswith(".json"):
                name = file.split('.')[0]
                arg = (inputDir + file,outputDir + name)
                argList.append(arg)
    print(argList)
    _paralell_process(readAndMergePairFiles, argList)


def combineUserFlows(userFlowFile):
    combinedList = {}
    userList = list(userFlows.keys())
    weightList= [len(value) for value in userFlows.values()]
    cnt = 0
    # Skip the users with less than 6 and more than 300 flows
    # Then creating the same amount of combination with other users which are randomly chose
    # IMPORTANT: Be careful about the location of the files (it is before batching them together but not need to change it at this stage)
    for user, flows in userFlows.items():
        cnt+=1
        print(cnt)
        try:
            if len(flows) < 2:
                continue

            if len(flows) <= 20:
                selected_flows = flows
            else:
                selected_flows = random.sample(flows, 20)
            pairs = list(combinations(selected_flows, 2))
            # Labeling pairs as 1
            pairs = [(p[0], p[1], 1) for p in pairs]
            #print(pairs)

            # We use length of pairs because we want to have the same amount of records with 0 label
            for i in range(len(pairs)):
                ## Random choise based on the weight of each user, the weight is number of flows associated with each user
                otherUser = random.choices(userList, weights=weightList, k=1 )[0]
                # Making shure about incorrect possible incorrect labelling
                while otherUser == user :
                    otherUser = random.choice(userList)
                newPair = (random.choice(selected_flows), random.choice(userFlows[otherUser]), 0)
                pairs.append(newPair)
            combinedList[user] = pairs
        except:
            print(f'error for user {user}')



    with open("Combine20Flows.json", "w") as jsonfile:
        json.dump(combinedList,jsonfile)

def feedPairs(inputFile):
    arg_list = []
    userGroups = {}
    with open(inputFile) as file:
        flowPairs = json.load(file)

    cnt = 0
    fileNameCounter = 0
    for user, pairs in flowPairs.items():
        # create list of dictionaries where each dictionary contins 1000 user  
        cnt +=1
        if cnt % 1000 == 0 or cnt == len(flowPairs):
            fileNameCounter += 1    
            
            lst = (userGroups, fileNameCounter)
            arg_list.append(lst)
            userGroups = {}


        userGroups[user] = pairs

    print(arg_list)
    _paralell_process(createDatasetFromFlows, arg_list)

def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)

def createDatasetFromFlows(flowPairs, index ):
    dataset = []
    cols = []
    cnt = 0
    for user, pairs in flowPairs.items():
        cnt+=1
        print(cnt)
        for pair in pairs:
            try:

                firstFlow = pd.read_csv(f'/mnt/md0/jaber/tempfile/{pair[0]}')
                secFlow = pd.read_csv(f'/mnt/md0/jaber/tempfile/{pair[1]}')
                label = pair[2]
                joined = firstFlow.values.tolist()[0] + secFlow.values.tolist()[0] + [label]
                dataset.append(joined)
                if not cols:
                    cols = list(firstFlow.columns) + list(secFlow.columns) + ['label']
            except:
                print(user)
                #print(pairs)

    df = pd.DataFrame(dataset, columns=cols)
    df.to_pickle(f'/mnt/md0/jaber/dataset/{index}.pkl')


def MergeUserGroups(userGroupsDir):
    cnt = 0
    interfaceList = []
    AllUsers = defaultdict(list)
    for subdir, dirs, files in  os.walk(userGroupsDir):
        for file in files:
            if file.startswith("UserFlows"):
                currentFile =  open(f'{userGroupsDir}/{file}')
                currentUser = json.load(currentFile)
                interfaceList.append(currentUser)
    for usergp in interfaceList:
        for user , flows in usergp.items():
            for flow in flows:
                AllUsers[user].append(flow)

    with open(f'{userGroupsDir}/AllUsers.json', 'w') as jsonfile:
        json.dump(AllUsers,jsonfile)

def is_valid_ip(ip_str):
    try:
        ipaddress.ip_address(ip_str)
        return True
    except ValueError:
        return False

if __name__ == "__main__":
    # inputFile = ("/mnt/md0/jaber/Combine20Flows.json")
    # feedPairs(inputFile)
    # file = open("/home/jaber/userGroups/UserFlows_s2f1.json")
    # userFlows = json.load(file)
    # userFlowStats(userFlows)
    # combineUserFlows(userFlows)
    # file = open("CombinedPairs6Flows.json")
    # flowPairs = json.load(file)
    # createDatasetFromFlows(flowPairs)
    # file = open("/mnt/md0/jaber/Combine150Flows.json")
    # datasetPairs = json.load(file)
    # lengths = []
    # for key , value in datasetPairs.items():
    #     lengths.append(len(value))
    # print(lengths)
    # print(sum(lengths))

    # MergeUserGroups('/home/jaber/userGroups')
    # file = open('/home/jaber/userGroups/AllUsers.json')
    # userFlows = json.load(file)
    #userFlowStats(userFlows)
    #datasetPairGenerate(userFlows, 'temp', 1000, 5,30)
    #groupOfDatasetGenerator('/home/jaber/userGroups/AllUsers.json', '/mnt/md0/jaber/pairsDatasets',5 , 30)
    # readAndMergePairFiles('/mnt/md0/jaber/pairsDatasets/49_292.json', '/mnt/md0/jaber/datasets/first.pkl')
    parallelReadAndMergePairs("/mnt/md0/jaber/pairsDatasets/", "/mnt/md0/jaber/datasets/")



