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
import itertools

def datasetPairGenerateAllRandom(userFlows, outputFile, numberOfUsers,  maxFlows):
    # Random sample: selection without replacement, but random choice is with replacement
    print(f'Generating pairs for {outputFile}')
    combinedList = {}
    pairsPerClass = int(maxFlows*(maxFlows-1)//2)
    cnt= 0

    # sampledKeys = random.sample(list(userFlows.keys()), numberOfUsers)
    # datasetUsers = {key: userFlows[key] for key in sampledKeys}
    datasetUsers = userFlows
    userList =  list(datasetUsers.keys())
    weightList= [len(value) for value in datasetUsers.values()]



    for user , flows in datasetUsers.items():
        try:
            pairs = []
            all_pairs = list(combinations(flows, 2))
            if len(all_pairs) < pairsPerClass:
                pairs = all_pairs
            else:
                pairs = random.sample(all_pairs, pairsPerClass)

            pairs = [(user + '/'  + p[0].split('/')[-1], user + '/' + p[1].split('/')[-1], 1) for p in pairs]
            currentNumberOfPairs = len(pairs)

            for i in range(currentNumberOfPairs):
                otherUser = random.choice(userList)
                while otherUser == user:
                    otherUser = random.choice(userList)

                pair = [random.choice(flows), random.choice(datasetUsers[otherUser]), 0]
                pair[0] = user + '/' + pair[0].split('/')[-1]
                pair[1] =  otherUser + '/' + pair[1].split('/')[-1]
                pairs.append(pair)
            combinedList[user] = pairs
        except:
            print(f'Error for user {user}')
    with open(f'{outputFile}_AllRandom' , "w") as jsonfile:
        json.dump(combinedList,jsonfile)


def datasetPairGenerateNoRepetition(userFlows, outputFile, numberOfUsers,  maxFlows):
    # Random sample: selection without replacement, but random choice is with replacement
    print(f'Generating pairs for {outputFile}')
    combinedList = {}
    pairsPerClass = int(maxFlows*(maxFlows-1)//2)
    cnt= 0

    # sampledKeys = random.sample(list(userFlows.keys()), numberOfUsers)
    # datasetUsers = {key: userFlows[key] for key in sampledKeys}
    datasetUsers = userFlows
    userList =  list(datasetUsers.keys())
    weightList= [len(value) for value in datasetUsers.values()]



    for user , flows in datasetUsers.items():
        try:
            pairs = []

            if len(flows) % 2 != 0:
                flows = flows[:-1]
            random.shuffle(flows)
            pairs = [(flows[i], flows[i + 1]) for i in range(0, len(flows), 2)]

            if len(pairs) > pairsPerClass:
                pairs = random.sample(pairs,pairsPerClass)

            pairs = [(user + '/'  + p[0].split('/')[-1], user + '/' + p[1].split('/')[-1], 1) for p in pairs]
            currentNumberOfPairs = len(pairs)

            for i in range(currentNumberOfPairs):
                otherUser = random.choice(userList)
                while otherUser == user:
                    otherUser = random.choice(userList)

                pair = [random.choice(flows), random.choice(datasetUsers[otherUser]), 0]
                pair[0] = user + '/' + pair[0].split('/')[-1]
                pair[1] =  otherUser + '/' + pair[1].split('/')[-1]
                pairs.append(pair)
            combinedList[user] = pairs
        except:
            print(f'Error for user {user}')
    with open(f'{outputFile}_NoRepetition' , "w") as jsonfile:
        json.dump(combinedList,jsonfile)


def datasetPairGenerateFullCombination(userFlows, outputFile, numberOfUsers, maxFlows):
    print(f'Generating pairs for {outputFile}')
    combinedList = {}

    pairsPerClass = int(maxFlows * (maxFlows - 1) // 2)
    # sampledKeys = random.sample(list(userFlows.keys()), numberOfUsers)
    # datasetUsers = {key: userFlows[key] for key in sampledKeys}
    datasetUsers = userFlows
    userList = list(datasetUsers.keys())
    weightList = [len(value) for value in datasetUsers.values()]

    # weightList= [len(value) for value in datasetUsers.values()]
    cnt = 0

    for user, flows in datasetUsers.items():
        cnt += 1
        try:
            if len(flows) <= maxFlows:
                selected_flows = flows
            else:
                selected_flows = random.sample(flows, maxFlows)
            pairs = list(combinations(selected_flows, 2))
            # Labeling pairs as 1
            pairs = [(user + '/' + p[0].split('/')[-1], user + '/' + p[1].split('/')[-1], 1) for p in pairs]

            # We use length of pairs because we want to have the same amount of records with 0 label
            for i in range(len(pairs)):
                ## Random choise based on the weight of each user, the weight is number of flows associated with each user
                otherUser = random.choice(userList)
                # Making sure about incorrect possible incorrect labelling
                while otherUser == user:
                    otherUser = random.choice(userList)

                pair = [random.choice(selected_flows), random.choice(datasetUsers[otherUser]), 0]
                pair[0] = user + '/' + pair[0].split('/')[-1]
                pair[1] =  otherUser + '/' + pair[1].split('/')[-1]
                pairs.append(pair)
            combinedList[user] = pairs
        except Exception as e:
            print(e)
            print(f'Error for user {user}')

    with open(f'{outputFile}_FullCombination', "w") as jsonfile:
        json.dump(combinedList, jsonfile)

def groupOfDatasetPairGenerator(userFlowsFile, outputDir, minFlows, maxFlows):
    arg_list = []
    file = open(userFlowsFile)
    userFlows = json.load(file)
    usersToRemove = []
    for user , flows in userFlows.items():
        if len(flows) < minFlows:
            usersToRemove.append(user)
    for user in usersToRemove:
        del userFlows[user]
    usersLength = [5, 20, 50, 100, 500, 1000]
    for i in range(0, 6):
        #numberOfUsers = random.randint(5, 5)
        numberOfUsers = usersLength[i]
        outputFile = f'{outputDir}/{i}_{numberOfUsers}.json'
        sampledKeys = random.sample(list(userFlows.keys()), numberOfUsers)
        datasetUsers = {key: userFlows[key] for key in sampledKeys}
        arg = (datasetUsers,outputFile,numberOfUsers , maxFlows)
        arg_list.append(arg)

    #print(arg_list)
        _paralell_process(datasetPairGenerateAllRandom,arg_list)
        _paralell_process(datasetPairGenerateFullCombination, arg_list)
        _paralell_process(datasetPairGenerateNoRepetition, arg_list)

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
    df.to_csv(OutputFile + ".csv", index = False)

def parallelReadAndMergePairs(inputDir, outputDir):

    argList = []
    for subdir, dirs, files in  os.walk(inputDir):
        for file in files:
            # if file.endswith(".json"):
            # name = file.split('.')[0]
            name = file
            arg = (inputDir + file,outputDir + name)
            argList.append(arg)
    print(argList)
    _paralell_process(readAndMergePairFiles, argList)

def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)




if __name__ == "__main__":

    #datasetPairGenerate(userFlows, 'temp', 1000, 5,30)
    # groupOfDatasetPairGenerator('/home/jaber/userGroups/AllUsers.json', '/mnt/md0/jaber/pairsDatasets',5 , 30)
    #readAndMergePairFiles('/mnt/md0/jaber/pairsDatasets/1_5.json_FullCombination', '/mnt/md0/jaber/datasets/first.csv')
    parallelReadAndMergePairs("/mnt/md0/jaber/pairsDatasets/", "/mnt/md0/jaber/datasets/")




