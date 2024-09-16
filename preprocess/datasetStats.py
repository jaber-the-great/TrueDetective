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

def is_valid_ip(ip_str):
    try:
        ipaddress.ip_address(ip_str)
        return True
    except ValueError:
        return False


def userPairsStat(inputDir, outputFile):
    outFile = open(outputFile,"w")
    cnt = 0
    for subdir, dirs, files in  os.walk(inputDir):
        for file in files:
            # if file.endswith(".json"):
            if True:
                print(file)
                f = open(subdir+file)
                userPairs = json.load(f)
                numOfPairs = []
                for user , pairs in userPairs.items():
                    numOfPairs.append(len(pairs))

                outFile.write(f'Dataset name: {file} \n')
                outFile.write(f'Min number of pairs per user: {min(numOfPairs)} \n')
                outFile.write(f'Max number of pairs per user: {max(numOfPairs)} \n')
                outFile.write(f'Average number of pairs per user: {statistics.mean(numOfPairs)} \n')
                outFile.write(f'Median number of pairs per user: {statistics.median(numOfPairs)} \n')
                outFile.write(f'Sum of number of pairs per user: {sum(numOfPairs)} \n')

    outFile.close()

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

def checkTestTrainOverlap(testFile, trainFileDir):
    file1 = open(testFile)
    testPairs = json.load(file1)
    for subdir, dirs , files in os.walk(trainFileDir):
        for file in files:
            jsonFile = open(subdir + file)
            trainPairs = json.load(jsonFile)
            for user in testPairs:
                if user in trainPairs:
                    print(file)
                    print(user)
                    print(len(testPairs[user]))
    # Removing the single outlier from the dataset
    del testPairs['169.231.110.64']
    with open('/mnt/md0/jaber/testSetsPairs/valid20users.json', 'w') as jsonfile:
        json.dump(testPairs, jsonfile)

def numberOfFlows(userFile):
    # Counting the total number of flows beside checking consistency between cic files and header and pcap files
    # Some flows removed from cic, they were corresponding to icmp (around 110 flows)
    file = open(userFile)
    userFlows = json.load(file)
    file.close()
    totalFlows = 0
    itemsToRemove = []
    for user , flows in userFlows.items():
        totalFlows +=len(flows)
        for flow in flows:

            name = flow.split('/')[-1]
            name = f'{user}/{name}'
            cicName = f'/mnt/md0/jaber/groupedCIC/{name}'
            headerName = f'/mnt/md0/jaber/groupedHeader/{name}'
            headerName = headerName.replace("_Flow.csv",".csv")
            pcapName = f'/mnt/md0/jaber/groupedPcap/{name}'
            pcapName = pcapName.rstrip("_Flow.csv")

            if not os.path.exists(cicName) or not os.path.exists(headerName) or  not os.path.exists(pcapName):
                if not os.path.exists(cicName):
                    print("cicIssue")
                if not os.path.exists(headerName) :
                    print("headerIssue")
                if not os.path.exists(pcapName) :
                    print("pcapIssue")
                print(user)
                print(flow)
                print(name)
                itemsToRemove.append([user, flow])

    print(totalFlows)
    for user, flow in itemsToRemove:
        userFlows[user].remove(flow)

    print(f'Total number of flows is: {totalFlows}')
    with open(userFile + "test", "w") as jsonfile:
        json.dump(userFlows, jsonfile)
if __name__ == "__main__":
    #MergeUserGroups('/home/jaber/userGroups')
    # file = open('/home/jaber/userGroups/AllUsers.json')
    # userFlows = json.load(file)
    #userFlowStats(userFlows)
    #userPairsStat('/mnt/md0/jaber/pairsDatasets/', '/home/jaber/TrueDetective/preprocess/PairsStat.txt')

    # checkTestTrainOverlap('/mnt/md0/jaber/testSetsPairs/1_20.json_AllRandom' , '/mnt/md0/jaber/pairsDatasets/',)
    numberOfFlows('/home/jaber/userGroups/AllUsers.json')