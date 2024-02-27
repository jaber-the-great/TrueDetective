
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
                firstflowName = pair[0].split('/')[-1].replace(".pcap_Flow.csv", ".npt")
                secFlowName = pair[1].split('/')[-1].replace(".pcap_Flow.csv", ".npt")
                firstFlow = pd.read_csv(f'/mnt/md0/jaber/nprintPerDataset/5_1000.json_NoRepetiti/{firstflowName}')
                secFlow = pd.read_csv(f'/mnt/md0/jaber/nprintPerDataset/5_1000.json_NoRepetiti/{secFlowName}')

                if cols == []:
                    FirstCols = list(firstFlow.columns)
                    SecCols = list(secFlow.columns)
                    SecCols = ["x" + s for s in SecCols]
                    # Multiply by 5 for number of packets
                    cols = FirstCols*5 + ['User Label'] + SecCols*5 + ['xUser Label']  + ['label']

                # if len(firstFlow) >= 5 and len(secFlow) >=5:
                #     firstFlow = pd.concat([firstFlow.iloc[i] for i in range(5)],axis =0)
                #     secFlow = pd.concat([secFlow.iloc[i] for i in range(5)], axis=0)
                # else:
                #     continue
                    # creeat first flow and second flow from the first 5 rows of the npt file if there is less than
                    # 5 rows in the npt file, pad the rows with 0s

                # If less than 5 packets in the flow, use zero padding
                firstFlow = pd.concat([firstFlow.iloc[i] if i < len(firstFlow) else pd.Series([0]*len(firstFlow.columns)) for i in range(5)],axis =0)
                secFlow = pd.concat([secFlow.iloc[i] if i < len(secFlow) else pd.Series([0]*len(secFlow.columns)) for i in range(5)], axis=0)
                src =firstFlow['src_ip'][0]
                dst = firstFlow.iloc[131:163].tolist()
                src = checkIP(src,dst)
                firstFlow['User Label'] = src


                src =secFlow['src_ip'][0]
                dst = secFlow.iloc[131:163].tolist()
                src = checkIP(src,dst)
                secFlow['User Label'] = src

                pairlabel = pair[2]
                joined = firstFlow.values.tolist()  + secFlow.values.tolist()  + [pairlabel]

                dataset.append(joined)


            except Exception as e:
                print(user)
                print(e)

    df = pd.DataFrame(dataset , columns=cols)
    df.to_csv(OutputFile + ".csv", index = False)








if __name__ == "__main__":
    # input_dir = "/mnt/md0/jaber/nprintFiles"
    # output_dir = '/mnt/md0/jaber/'
    # mergeNptsAndFindInternalIP(input_dir,output_dir)
    # createUsersDictionary('/mnt/md0/jaber/npt/nprint_data.csv', '/mnt/md0/jaber/npt/')
    # file = open("/mnt/md0/jaber/npt/NprintUsersFlows.json")
    # userFlows = json.load(file)
    # userFlowStats(userFlows)
    # combineSameUserFlows("/mnt/md0/jaber/npt/NprintUsersFlows.json")
    # createDatasetFromFlows("/mnt/md0/jaber/npt/nptCombine65Flows.json" ,'/mnt/md0/jaber/npt/nprint_data.csv')
    # file = open('/mnt/md0/jaber/pairsDatasets/1_10.json')
    # UserPairs = json.load(file)
    readAndMergePairFiles('/mnt/md0/jaber/pairsDatasets/5_1000.json_NoRepetition', '/mnt/md0/jaber/mergedNprint/5_1000_NORepeat')

