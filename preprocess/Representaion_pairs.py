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


# TODO: Don't forget to drop src and dest IP from headers dataset
# TODO: Look in each direcotry and find the number of items --> just use the dictionary
# TODO: How many pairs with True in each directory: n (n-1)/2. Create all those pairs
# TODO: Randmoly slec
def userFlowStats(userFlows):
    # Number of flows per user
    # Number of total users
    Lengths = []
    topUsers = []
    for key in userFlows:
        Lengths.append(len(userFlows[key]))
        if len(userFlows[key]) > 2000:
            topUsers.append(key)
    Lengths.sort()
    print(Lengths)
    # Calculates how many users have n numbre of flows
    freq = Counter(Lengths)
    for item , frequency in freq.items():
        print(f"{item}: {frequency} times")
    df = pd.DataFrame(topUsers)
    df.to_csv("TopUsers.csv", index=False)

    with open("NumberofFlowsPerUser.txt", "w") as file:
        for item in Lengths:
            file.write(f'{item},')

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
    usersLength = [5, 20, 50, 100]
    for i in range(0, 4):
        #numberOfUsers = random.randint(5, 5)
        numberOfUsers = usersLength[i]
        outputFile = f'{outputDir}/{i}_{numberOfUsers}.json'
        sampledKeys = random.sample(list(userFlows.keys()), numberOfUsers)
        datasetUsers = {key: userFlows[key] for key in sampledKeys}
        arg = (datasetUsers,outputFile,numberOfUsers , maxFlows)
        arg_list.append(arg)

    #print(arg_list)
        _paralell_process(datasetPairGenerateAllRandom,arg_list)
        #_paralell_process(datasetPairGenerateFullCombination, arg_list)
        #_paralell_process(datasetPairGenerateNoRepetition, arg_list)

def testSetPairGeneratorAllRandom(userFlows, usersToExclude ,outputFile, numberOfPairs):
    # Random sample: selection without replacement, but random choice is with replacement
    print(f'Generating test set pair for {outputFile}')
    combinedList = {}
    cnt= 0

    for user in usersToExclude:
        del userFlows[user]

    userList =  list(userFlows.keys())
    weightList= [len(value) for value in userFlows.values()]


    for i in range(numberOfPairs):
        try:
            pairs = []
            user = random.choice(userList)
            flows = userFlows[user]
            pair = random.sample(flows, 2)
            pair = [user + '/'  + pair[0].split('/')[-1], user + '/' + pair[1].split('/')[-1], 1]
            pairs.append(pair)

            differentUsers = random.sample(userList, 2)
            pair = [random.choice(userFlows[differentUsers[0]]), random.choice(userFlows[differentUsers[1]]), 0]
            pair[0] = differentUsers[0] + '/' + pair[0].split('/')[-1]
            pair[1] =  differentUsers[1] + '/' + pair[1].split('/')[-1]
            pairs.append(pair)
            combinedList[i] = pairs
        except Exception as e:
            print(e)
            print(f'Error for user {user} or {differentUsers}')

    with open(f'{outputFile}_Test' , "w") as jsonfile:
        json.dump(combinedList,jsonfile)



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


def parallelCombineUserFlows( usersToPair , fileName):
    file = open("groupings.json")
    userFlowsDic = json.load(file)
    combinedList = {}
    userList = list(userFlows.keys())
    weightList = [len(value) for value in userFlows.values()]
    cnt = 0
    for user in usersToPair:
        flows = userFlowsDic[user]
        try:
            if len(flows) < 2:
                continue
            if len(flows) <= 65:
                selected_flows = flows
            else:
                selected_flows = random.sample(flows, 65)
            pairs = list(combinations(selected_flows, 2))
            # Labeling pairs as 1
            pairs = [(p[0], p[1], 1) for p in pairs]
            # We use length of pairs because we want to have the same amount of records with 0 label
            for i in range(len(pairs)):
                ## Random choise based on the weight of each user, the weight is number of flows associated with each user
                otherUser = random.choices(userList, weights=weightList, k=1)[0]
                # Making shure about incorrect possible incorrect labelling
                while otherUser == user:
                    otherUser = random.choice(userList)
                newPair = (random.choice(selected_flows), random.choice(userFlows[otherUser]), 0)
                pairs.append(newPair)
            combinedList[user] = pairs
        except:
            print(f'error for user {user}')

    with open(f'/home/jaber/TrueDetective/CombinedPairsParallel/{fileName}.json', "w") as jsonfile:
        json.dump(combinedList, jsonfile)


def combineSameUserFlows(userFlows):
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

            if len(flows) <= 150:
                selected_flows = flows
            else:
                selected_flows = random.sample(flows, 150)
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



    with open("Combine150Flows.json", "w") as jsonfile:
        json.dump(combinedList,jsonfile)

def createDatasetFromFlows(flowPairsFile):
    file = open(flowPairsFile)
    flowPairs = json.load(file)
    file.close()
    # Read csv files and add it to the dataframe
    flag = True
    cnt = 0
    dataset = []
    cols = []
    for user, pairs in flowPairs.items():
        cnt +=1

        print(cnt)
        try:
            for pair in pairs:
                print(len(pair))
                firstFlow = pd.read_csv(pair[0])
                secFlow = pd.read_csv(pair[1])
                label = pair[2]
                joined = firstFlow.values.tolist()[0]
                joined.extend(secFlow.values.tolist()[0])
                joined.append(label)
                dataset.append(joined)
                if not cols:
                    cols = list(firstFlow.columns)
                    cols.extend(list(secFlow.columns))
                    cols.append('label')



        except:
            print(user)

    df = pd.DataFrame(dataset,columns=cols)
    fileName = flowPairsFile.split('/')[-1][:-5]
    location = f'/home/jaber/TrueDetective/dataset/{fileName}.pkl'
    df.to_pickle(location)

def feedParallelDataset(input_dir):
    arg_list = []
    for i in range(1,2):
        arg = (input_dir +str(i)+ ".json",)
        arg_list.append(arg)

    _paralell_process(createDatasetFromFlows, arg_list)

def analyseTopUsers(UsersFiles):

    users = pd.read_csv(UsersFiles)
    for i in range(4):
        print(i)


def findService(fileName):
    df = pd.read_csv(fileName)
    # Direction 1 means the src is internal
    if df['Direction'][0] == 1:
        return [df['Dst Port'][0], df['Outside IP'][0]]
    # Direction 0 means the dest is internal
    else:
        return [df['Src Port'][0], df['Outside IP'][0]]


def checkParisDistribution(groupingsJson):
    file = open(groupingsJson)
    userFlows = json.load(file)
    file.close()


    ServicesPerUser = {}
    cnt = 0
    # Iterate over the userFlows  
    for user, flows in userFlows.items():
        print(user)
        cnt+=1
        print(cnt)
        if cnt>1000:
            break
        services = []
        # Count the number of flows with label 1
        for item in flows:
            services.append(findService(item))
        unique_pairs = {(port, ip) for port, ip in services}
        unique_ports = {port for port, ip in services}
        ServicesPerUser[user] = (list(unique_ports),list(unique_pairs), len(userFlows[user]))
    with open("ServicesPerUser1000Users.json", "w") as jsonfile:
        json.dump(ServicesPerUser,jsonfile)

    portNum =[]
    serviceNum = []
    userFlowLength= []
    for user , stats in ServicesPerUser.items():
        portNum.append(len(ServicesPerUser[user][0]))
        serviceNum.append(len(ServicesPerUser[user][1]))
        userFlowLength.append(ServicesPerUser[user][2])
    import statistics
    print(f'average port {statistics.mean(portNum)}')
    print(max(portNum))
    print(min(portNum))
    print(statistics.median(portNum))
    print(f'average service {statistics.mean(serviceNum)}')
    print(max(serviceNum))
    print(min(serviceNum))
    print(statistics.median(serviceNum))
    print(f'average flows per user {statistics.mean(userFlowLength)}')
    print(max(userFlowLength))
    print(min(userFlowLength))
    print(statistics.median(userFlowLength))




if __name__ == "__main__":
    # df= pd.read_csv('TopUsers.csv')
    # print("here")
    file = open("groupings.json")
    userFlows = json.load(file)
    userFlowStats(userFlows)
    # combineSameUserFlows(userFlows)
    # file = open("CombinedPairs6Flows.json")
    # flowPairs = json.load(file)
    # createDatasetFromFlows(flowPairs)
    # feedFlows(userFlows)
    # soft_limit_bytes = 100 * 1024 * 1024 * 1024  # 2 GB
    # process = psutil.Process()
    # process.rlimit(psutil.RLIMIT_AS, (soft_limit_bytes, psutil.RLIM_INFINITY))
    # feedParallelDataset("/home/jaber/TrueDetective/CombinedPairsParallel/")
#    createDatasetFromFlows("/home/jaber/TrueDetective/CombinedPairsParallel/1.json")
    # checkParisDistribution("/home/jaber/TrueDetective/preprocess/groupings.json")
