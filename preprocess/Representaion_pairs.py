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
    cnt = 0
    # Skip the users with less than 6 and more than 300 flows
    # Then creating the same amount of combination with other users which are randomly chose
    # IMPORTANT: Be careful about the location of the files (it is before batching them together but not need to change it at this stage)
    for user, flows in userFlows.items():
        if len(flows) < 2:
            continue

        if len(flows) <= 200:
            selected_flows = flows
        else:
            selected_flows = random.sample(flows, 200)
        pairs = list(combinations(selected_flows, 2))
        # Labeling pairs as 1
        pairs = [(p[0], p[1], 1) for p in pairs]
        #print(pairs)

        # We use length of pairs because we want to have the same amount of records with 0 label
        for i in range(len(pairs)):
            otherUser = random.choice(userList)
            # Making shure about incorrect possible incorrect labelling
            while otherUser == user :
                otherUser = random.choice(userList)
            newPair = (random.choice(selected_flows), random.choice(userFlows[otherUser]), 0)
            pairs.append(newPair)
        combinedList[user] = pairs



    with open("CombinedPairs6Flows.json", "w") as jsonfile:
        json.dump(combinedList,jsonfile)

def createDatasetFromFlows(flowPairs):
    # TODO: create the panda dataframe
    # Read csv files and add it to the dataframe
    flag = True
    cnt = 0
    for user, pairs in flowPairs.items():
        cnt +=1
        print(cnt)
        if cnt % 1000 == 0:
            dataset.to_pickle(f'{cnt}.pkl')

        try:
            for pair in pairs:

                firstFlow = pd.read_csv(pair[0])
                secFlow = pd.read_csv(pair[1])
                label = pair[2]
                if flag:
                    flag = False
                    dataset = pd.concat([firstFlow, secFlow], axis =1)
                    dataset['Label'] = label
                else:
                    joined = pd.concat([firstFlow, secFlow], axis =1)
                    joined['Label'] = label
                    # list of pds and append f
                   # dataset = pd.concat([dataset, joined] , ignore_index=True)

        except:
            print(user)
            #print(pairs)

    dataset.to_pickle("dataset.pkl")





if __name__ == "__main__":
    file = open("groupings.json")
    userFlows = json.load(file)
    # userFlowStats(userFlows)
    # combineSameUserFlows(userFlows)
    #file = open("CombinedPairs6Flows.json")
    #flowPairs = json.load(file)
    #createDatasetFromFlows(flowPairs)


