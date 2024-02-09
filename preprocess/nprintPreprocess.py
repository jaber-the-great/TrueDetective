
import pandas as pd
import ipaddress
import os
import warnings
import numpy as np
import json
from collections import Counter
from itertools import combinations
import random
import statistics

# Suppress all FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def checkIP(src, dst):
    networks = ['128.111.0.0/16', '169.231.0.0/16', '192.150.216.0/23', '92.35.222.0/24', '199.120.153.0/24']
    for network in networks:
        if ipaddress.ip_address(src) in ipaddress.ip_network(network):
            return src

    dst_str = ".".join(
        str(int("".join(map(str, dst[i:i + 8])), 2))
        for i in range(0, 32, 8)
    )
    for network in networks:
        if ipaddress.ip_address(dst_str) in ipaddress.ip_network(network):
            return dst_str
    return None

    # Display the IP address
    print("IP Address:", ip_address_str)

def mergeNptsAndFindInternalIP(inpt_dir , output_dir):
    dataFiles = []
    cnt = 0
    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".npt"):
                cnt+=1
                print(cnt)
                df = pd.read_csv(subdir + "/" + file)
                if len(df) >= 5:
                    # create a new dataframe with the first 5 rows of the df but append thses rows to each other in horizontal way
                    # and then append the new dataframe to dataFiles
                    new_df = pd.concat([df.iloc[i] for i in range(5)],axis =0)
                    src =new_df['src_ip'][0]
                    dst = new_df.iloc[131:163].tolist()
                    src = checkIP(src,dst)
                    if src != None:
                        new_df['User Label'] = src
                        dataFiles.append(new_df)



    nprintData =  pd.concat(dataFiles, axis=1).transpose()
    nprintData.to_csv(output_dir + "nprint_data.csv",index=False)

    print("Here")

def createUsersDictionary(input_file, output_dir):
    df = pd.read_csv(input_file)
    # Iterate over the rows of the dataframe, and create a dictionary of users based on the user label and record the row number of that record
    users = {}
    for index, row in df.iterrows():
        if row['User Label'] in users:
            users[row['User Label']].append(index)
        else:
            users[row['User Label']] = [index]
    
    # Save the dictionary to a json file

    with open(output_dir + 'NprintUsersFlows.json', 'w') as fp:
        json.dump(users, fp)



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
    with open("/mnt/md0/jaber/npt/NumberofFlowsPerUser.txt", "w") as file:
        for item in Lengths:
            file.write(f'{item},')

    with open("/mnt/md0/jaber/npt/FreqOfNumFlowsPerUsers.json", "w") as jsonfile:
        json.dump(freq,jsonfile)

def combineSameUserFlows(userFlowsFile):
    file = open(userFlowsFile)
    userFlows = json.load(file)

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
            if len(flows) < 3:
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




    with open("/mnt/md0/jaber/npt/nptCombine65Flows.json", "w") as jsonfile:
        json.dump(combinedList,jsonfile)

def createDatasetFromFlows(flowPairs , nptFile ):
    index = 0
    dataset = []
    cols = []
    cnt = 0
    userFlows = pd.read_csv(nptFile)
    file = open(flowPairs)
    flowPairs = json.load(file)

    for user, pairs in flowPairs.items():
        cnt+=1
        print(cnt)
        if cnt % 10 == 0:
            df = pd.DataFrame(dataset, columns=cols)
            df.to_pickle(f'/mnt/md0/jaber/npt/nptdata/{index}.pkl')
            dataset = []
            index +=1
        for pair in pairs:
            try:

                firstFlow = userFlows.iloc[pair[0]]
                secFlow = userFlows.iloc[pair[1]]
                label = pair[2]
                joined = firstFlow.values.tolist() + secFlow.values.tolist() + [label]
                dataset.append(joined)
                if not cols:
                    cols = firstFlow.index.tolist() + secFlow.index.tolist() + ['label']
            except:
                print(user)
                #print(pairs)

    df = pd.DataFrame(dataset, columns=cols)
    df.to_pickle(f'/mnt/md0/jaber/dataset/{index}.pkl')


if __name__ == "__main__":
    # input_dir = "/mnt/md0/jaber/nprintFiles"
    # output_dir = '/mnt/md0/jaber/'
    # mergeNptsAndFindInternalIP(input_dir,output_dir)
    # createUsersDictionary('/mnt/md0/jaber/npt/nprint_data.csv', '/mnt/md0/jaber/npt/')
    # file = open("/mnt/md0/jaber/npt/NprintUsersFlows.json")
    # userFlows = json.load(file)
    # userFlowStats(userFlows)
    # combineSameUserFlows("/mnt/md0/jaber/npt/NprintUsersFlows.json")
    createDatasetFromFlows("/mnt/md0/jaber/npt/nptCombine65Flows.json" ,'/mnt/md0/jaber/npt/nprint_data.csv')

