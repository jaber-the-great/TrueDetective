import pandas as pd
from multiprocessing import Pool
import os
from collections import defaultdict
from subprocess import call
import json
from collections import Counter
from itertools import combinations
import random
import psutil


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

    with open("FreqOfNumFlowsPerUsers.json", "w") as jsonfile:
        json.dump(freq,jsonfile)

def feedFlows(UserFlowsDic):
    userList = list(UserFlowsDic.keys())
    arg_list = []
    size = 100
    for i in range(200,260):

        arg = ( userList[i*size : (i+1) * size ], str(i))

        if i == 259:
            arg = (userList[i * size:], str(i))


        arg_list.append(arg)

    print(arg_list)
    _paralell_process(parallelCombineUserFlows,arg_list)


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

            if len(flows) <= 65:
                selected_flows = flows
            else:
                selected_flows = random.sample(flows, 65)
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
        if len(flows) > 500:
            continue
        if cnt>3000:
            break
        services = []
        # Count the number of flows with label 1
        for item in flows:
            services.append(findService(item))
        unique_pairs = {(port, ip) for port, ip in services}
        unique_ports = {port for port, ip in services}
        ServicesPerUser[user] = (list(unique_ports),list(unique_pairs), len(flows))
    with open("ServicesPerUser3000Users.json", "w") as jsonfile:
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
    # file = open("groupings.json")
    # userFlows = json.load(file)
    # userFlowStats(userFlows)
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
    checkParisDistribution("/home/jaber/TrueDetective/preprocess/groupings.json") 
    file = open("ServicesPerUser3000Users.json")
    ServicesPerUser = json.load(file)
    portFreq = {}
    portNum= []
    serviceNum =[]
    userFlowLength= []
    for user , stats in ServicesPerUser.items():
        portNum.append(len(stats[0]))
        serviceNum.append(len(stats[1]))
        userFlowLength.append(stats[2])
        for port in stats[0]:
            if port in portFreq:
                portFreq[port] += 1
            else:
                portFreq[port] = 1

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
    print(portFreq)