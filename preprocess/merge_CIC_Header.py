import os
import subprocess

import pandas as pd
from multiprocessing import Pool
import csv
from subprocess import call



def changeFlowID(filename):
    df = pd.read_csv(filename)
    flowID = os.path.splitext(os.path.basename(filename))[0]
    flowID = flowID.split('.')[0]
    df.iloc[:, 0] = flowID
    df.to_csv(filename, index=False)



def mergeCICs(input_dir, output_dir):
    filesList = []
    for subdir, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".csv"):
                filesList.append(subdir + "/" + file)
                changeFlowID(subdir + "/" + file)
                #print(file)

    if filesList != []:
        df = pd.concat(map(pd.read_csv, filesList), ignore_index=True)
        subprocess.getoutput(f'mkdir {output_dir}')
        df.to_csv( output_dir+ "CIC_Merged.csv", index=False)


def appendCICandheaders(cicFile, headersDir,  outputDir , numOfHeaderBytes ):
    cic = pd.read_csv(cicFile)

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
                    # print(headerFlowID)
                    row_to_append.to_csv( outputDir + str(headerFlowID) +  ".csv", index=False)
                



def appendHeaderCICFiles(input_dir ,output_dir, numOfHeaderBytes):
        
    colNames = []
    for i in range(numOfHeaderBytes):
        name = "byte_" + str(i + 1)
        colNames.append(name)
    cnt = 0

    for subdir, dirs, files in os.walk(input_dir):
        cnt +=1
        print(cnt)

        for file in files:
            try:
                if file.endswith("pcap_Flow.csv"):
                    cic = pd.read_csv(subdir + "/" + file)
                    headerName = file.split('.')[0] + ".csv"
                    header_file = pd.read_csv(subdir + "/" + headerName, header=None)
                    cic = pd.concat([cic, pd.DataFrame(columns=colNames)])
                    cic.loc[:, colNames] = list(header_file.iloc[0])
                    SaveLoc = output_dir + subdir.split('/')[-1] + '/'
                    os.makedirs(SaveLoc, exist_ok=True)

                    cic.to_csv( SaveLoc +  file.split('.')[0] + ".merged.csv", index=False)
            except:
                print(f'Issue with file {file}')


def feed_packets_appendCICandheaders(header_dir, cic_dir ,output_dir,numOfHeaderBytes):
    arg_list = []

    for counter in range (37):
        for i in range(20):
            index = counter * 20 + i
            print(index)
            cicFile = f'{cic_dir}/{str(index)}/CIC_Merged.csv'

            lst = ( cicFile,header_dir + "/" + str(index) + "/", output_dir + "/" + str(index) + "/" , numOfHeaderBytes)
            arg_list.append(lst)
            #print(arg_list)
        _paralell_process(appendCICandheaders, arg_list)




def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)
    


if __name__ == "__main__":
    #mergeCICs("/home/jaber/TrueDetective/cic")
    #appendCICandheaders("CIC_Merged.csv", "/home/jaber/TrueDetective/3packetHeaders/" ,"/home/jaber/TrueDetective/cicMerge3Packet/",  120)
    print("here")
    #feed_packets_appendCICandheaders("/home/jaber/TrueDetective/headers", "/home/jaber/TrueDetective/mergedCIC", "/home/jaber/TrueDetective/HeaderCIC", 120)
    # for i in range(1,745):
    #     print(i)
    #     # Mefging the edited cics of each subfolder
    #     # mergeCICs("/home/jaber/TrueDetective/editedCICFilteredPcaps/" + str(i) + "/" , "/home/jaber/TrueDetective/mergedCIC/"+ str(i) + "/" )
    #     #
    #     # Creating subdirectories before saving the output
    #     subprocess.getoutput(f'mkdir /home/jaber/TrueDetective/HeaderCIC/{str(i)}/')
    # df = pd.read_csv("/home/jaber/TrueDetective/cicFilteredPcaps/1/s3-2022-04-29-1215-ens4f1-2022-04-29-1215-360752.pcap_Flow.csv")
    #
    # timestamps = df['Timestamp']
    # print(df)





