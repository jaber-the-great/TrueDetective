import pandas as pd
from multiprocessing import Pool
import os
from collections import defaultdict
from subprocess import call
import json
from collections import Counter
from itertools import combinations
import random


# TODO: Don't forget to drop src and dest IP from headers dataset
# TODO: Look in each direcotry and find the number of items --> just use the dictionary
# TODO: How many pairs with True in each directory: n (n-1)/2. Create all those pairs
# TODO: Randmoly slec
def userFlowStats(userFlows):
    # Number of flows per user
    # Number of total users
    Lengths = []
    for key in userFlows:
        Lengths.append(len(userFlows[key]))
    Lengths.sort()
    print(Lengths)
    # Calculates how many users have n numbre of flows
    freq = Counter(Lengths)
    for item , frequency in freq.items():
        print(f"{item}: {frequency} times")
    with open("NumberofFlowsPerUser.txt", "w") as file:
        for item in Lengths:
            file.write(f'{item},')

    with open("FreqOfNumFlowsPerUsers.json", "w") as jsonfile:
        json.dump(freq,jsonfile)


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





if __name__ == "__main__":
    # inputFile = ("/mnt/md0/jaber/Combine20Flows.json")
    # feedPairs(inputFile)
    file = open("/mnt/md0/jaber/groupings.json")
    userFlows = json.load(file)
    userFlowStats(userFlows)
    # combineSameUserFlows(userFlows)
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



