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
            if file.endswith(".json"):
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

if __name__ == "__main__":
    # MergeUserGroups('/home/jaber/userGroups')
    # file = open('/home/jaber/userGroups/AllUsers.json')
    # userFlows = json.load(file)
    #userFlowStats(userFlows)
    # userPairsStat('/mnt/md0/jaber/pairsDatasets/', '/home/jaber/TrueDetective/preprocess/PairsStat.txt')
    pass