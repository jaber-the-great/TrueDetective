#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import pandas as pd
from multiprocessing import Pool
import csv


# In[2]:


def changeFlowID(filename):
    df = pd.read_csv(filename)
    flowID = os.path.splitext(os.path.basename(filename))[0]
    flowID = flowID.split('.')[0]
    df.iloc[:, 0] = flowID
    df.to_csv(filename, index=False)


# In[3]:


def mergeCICs(input_dir):
    filesList = []
    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".csv"):
                filesList.append(subdir + "/" + file)
                changeFlowID(subdir + "/" + file)
                print(file)
    if filesList != []:
        df = pd.concat(map(pd.read_csv, filesList), ignore_index=True)
        print("here")
        df.to_csv("CIC_Merged.csv", index=False)


# In[4]:


def appendCICandheaders(meregedCIC, headersDir,  outputDir , numOfHeaderBytes ):
    cic = pd.read_csv(meregedCIC)

    counter = 0
    colNames = []
    for i in range(numOfHeaderBytes):
        name = "byte_" + str(i + 1)
        colNames.append(name)
    for subdir, dirs, files in os.walk(headersDir):
        for file in files:
            if file.endswith(".csv"):
                headerfile = pd.read_csv(subdir + "/" + file, header=None)
                headerFlowID = file.split('.')[0]
                if headerFlowID in cic["Flow ID"].values:
                    row_to_append = cic[cic['Flow ID'] == headerFlowID]
                    row_to_append = pd.concat([row_to_append, pd.DataFrame(columns=colNames)])
                    row_to_append.loc[cic["Flow ID"].str.contains(headerFlowID), colNames] = list(headerfile.iloc[0])
                    print(headerFlowID)
                    row_to_append.to_csv( outputDir + str(headerFlowID) +  ".csv", index=False)
                
    


# In[ ]:





# In[10]:


def feed_packets(input_dir, output_dir):
    arg_list = []
    for subdir, dirs, files in os.walk(input_dir):
        print(subdir)
        for file in files:
            if file.endswith(".pcap"):
                newName = file.split('.')[0] + ".csv"
                lst = ( input_dir + '/' + file, output_dir + '/' + newName)
                arg_list.append(lst)


# In[8]:


def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)
    


# In[22]:


if __name__ == "__main__":
    #mergeCICs("/home/jaber/TrueDetective/cic")
    appendCICandheaders("CIC_Merged.csv", "/home/jaber/TrueDetective/3packetHeaders/" ,"/home/jaber/TrueDetective/cicMerge3Packet/",  120)



# In[ ]:




