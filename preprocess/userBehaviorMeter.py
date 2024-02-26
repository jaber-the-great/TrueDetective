import subprocess
import pandas as pd
from multiprocessing import Pool
import os
import datetime
import json
import time


# input = ['/mnt/md0/jaber/groupedcicflow/169.231.141.110/s2f3_00000_20221205201500-45086.pcap_Flow.csv',
#          '/mnt/md0/jaber/groupedcicflow/169.231.141.110/s3f0_00000_20221205201500-51228.pcap_Flow.csv',
#           '/mnt/md0/jaber/groupedcicflow/169.231.141.110/s3f2_00000_20221205201500-24138.pcap_Flow.csv' , 
#            '/mnt/md0/jaber/groupedcicflow/169.231.141.110/s3f3_00000_20221205201500-41813.pcap_Flow.csv']




def userTrafficPattern(userFlowsDir, outputDir,captureStartTime, captureDuration):
    windowStat = {}
    userFlows = []
    for filename in os.listdir(userFlowsDir):
        # Check if the item is a file (not a subdirectory)
        if os.path.isfile(os.path.join(userFlowsDir, filename)):
            if filename.endswith(".csv"):
                data = pd.read_csv(os.path.join(userFlowsDir, filename))
                userFlows.append(data)



    if userFlows == []:
        return
    userFlows = pd.concat(userFlows)
    
    # Flow Duration
    windowStat['TotalFlowDuration'] = userFlows['Flow Duration'].sum()
    windowStat['MinFlowDuration'] = userFlows['Flow Duration'].min()
    windowStat['MaxFlowDuration'] = userFlows['Flow Duration'].max()
    windowStat['AvgFlowDuration'] = userFlows['Flow Duration'].mean()
    windowStat['StdFlowDuration'] = userFlows['Flow Duration'].std() 

    # Flow Packets Number fwd and bwd
    windowStat['totalPackets'] = userFlows['Total Fwd Packet'].sum()
    windowStat['minPackets'] = userFlows['Total Fwd Packet'].min()
    windowStat['maxPackets'] = userFlows['Total Fwd Packet'].max()
    windowStat['avgPackets'] = userFlows['Total Fwd Packet'].mean()
    windowStat['stdPackets'] = userFlows['Total Fwd Packet'].std()
    windowStat['totalBwdPackets'] = userFlows['Total Bwd packets'].sum()    
    windowStat['minBwdPackets'] = userFlows['Total Bwd packets'].min()
    windowStat['maxBwdPackets'] = userFlows['Total Bwd packets'].max()
    windowStat['avgBwdPackets'] = userFlows['Total Bwd packets'].mean()    
    windowStat['stdBwdPackets'] = userFlows['Total Bwd packets'].std()

    # Flow Bytes fwd 
    windowStat['totalFwdBytes'] = userFlows['Total Length of Fwd Packet'].sum()
    windowStat['minFwdBytes'] = userFlows['Total Length of Fwd Packet'].min()
    windowStat['maxFwdBytes'] = userFlows['Total Length of Fwd Packet'].max()
    windowStat['avgFwdBytes'] = userFlows['Total Length of Fwd Packet'].mean()
    windowStat['stdFwdBytes'] = userFlows['Total Length of Fwd Packet'].std()
    windowStat['totalBwdBytes'] = userFlows['Total Length of Bwd Packet'].sum()
    windowStat['minBwdBytes'] = userFlows['Total Length of Bwd Packet'].min()
    windowStat['maxBwdBytes'] = userFlows['Total Length of Bwd Packet'].max()
    windowStat['avgBwdBytes'] = userFlows['Total Length of Bwd Packet'].mean()
    windowStat['stdBwdBytes'] = userFlows['Total Length of Bwd Packet'].std()
    windowStat['totalBytes'] = windowStat['totalFwdBytes'] + windowStat['totalBwdBytes']


    # Flows distinct src and dst ports
    windowStat['distinctSrcPorts'] = userFlows['Src Port'].nunique()
    windowStat['distinctDstPorts'] = userFlows['Dst Port'].nunique()

    # Flows distinct dst IP
    windowStat['distinctDstIP'] = userFlows['Outside IP'].nunique()

    # Total number of flows
    windowStat['totalFlows'] = len(windowStat)

    # Packet length fwd and bwd
    windowStat['minFwdPacketLen'] = userFlows['Fwd Packet Length Min'].min()
    windowStat['maxFwdPacketLen'] = userFlows['Fwd Packet Length Max'].max()
    windowStat['avgFwdPacketLen'] = userFlows['Fwd Packet Length Mean'].mean()
    windowStat['stdFwdPacketLen'] = userFlows['Fwd Packet Length Mean'].std()

    windowStat['minBwdPacketLen'] = userFlows['Bwd Packet Length Min'].min()
    windowStat['maxBwdPacketLen'] = userFlows['Bwd Packet Length Max'].max()    
    windowStat['avgBwdPacketLen'] = userFlows['Bwd Packet Length Mean'].mean()
    windowStat['stdBwdPacketLen'] = userFlows['Bwd Packet Length Mean'].std()

    # Down/Up Ratio
    windowStat['minDownUpRatio'] = userFlows['Down/Up Ratio'].min()
    windowStat['maxDownUpRatio'] = userFlows['Down/Up Ratio'].max()
    windowStat['avgDownUpRatio'] = userFlows['Down/Up Ratio'].mean()
    windowStat['stdDownUpRatio'] = userFlows['Down/Up Ratio'].std() 

    # Inte arrival time of flows

    intervals = flowInterArrivalTime(userFlows)
    windowStat['minFlowIAT'] = intervals.min()
    windowStat['maxFlowIAT'] = intervals.max()
    windowStat['avgFlowIAT'] = intervals.mean()
    windowStat['stdFlowIAT'] = intervals.std()

    
    # Calculate the stats of the idle times
    idleTimes = flowIdleTime(userFlows['Timestamp'], userFlows['Flow Duration'], captureStartTime, captureDuration)
    windowStat['minIdleTime'] = min(idleTimes)  
    windowStat['maxIdleTime'] = max(idleTimes)  
    windowStat['totalIdleTime'] = sum(idleTimes)
    windowStat['stdIleTime'] = idleTimes.std()
    windowStat['avgIdleTime'] = idleTimes.mean()  

    # Ratio of number of TCP flows to number of UDP flows
    windowStat['totalTCPFlows'] = userFlows[userFlows['Protocol'] == 6]['Protocol'].count()
    windowStat['totalUDPFlows'] = userFlows[userFlows['Protocol'] == 17]['Protocol'].count()

    # It resulted in a division by zero error
    # windowStat['TCPUDPFlowsRatio'] = windowStat['totalTCPFlows'] / windowStat['totalUDPFlows']

    # Saving to file
    IP = userFlows['User IP'].iloc[0]
    outFileName = f'{outputDir}{IP}.csv'
    #print(outFileName)
    df = pd.DataFrame(windowStat , index=[0])
    df.to_csv(outFileName, index=False)





def flowInterArrivalTime(userFlows):
    userFlows['Timestamp'] = pd.to_datetime(userFlows['Timestamp'], format='%m/%d/%Y %I:%M:%S %p')
    # Sort flows based on timestamp
    userFlows = userFlows.sort_values(by='Timestamp')

    # Calculate time intervals between flows
    intervals = []
    timestamps = userFlows['Timestamp'].tolist()
    
    if len(timestamps) >1:
        for i in range(len(timestamps) - 1):
            interval = timestamps[i+1] - timestamps[i]
            intervals.append(interval.total_seconds())
    else:
        intervals.append(0)
    
    # Calculate the stats of the intervals
    intervals = pd.Series(intervals)
    return intervals

def flowIdleTime(Start, Duration, captureTime, CaptureDuration ):
    # TODO: Edit this function and make it faster
    # Sample data: start times and durations of tasks
    
    data = {
        'Start': Start,
        'Duration': Duration
    }

    df = pd.DataFrame(data)

    # Convert start times and durations to datetime
    df['Start'] = pd.to_datetime(df['Start'])
    df['Duration'] = pd.to_timedelta(df['Duration'])

    # Create a list to store time brackets with no tasks
    free_time_brackets = []

    # Sort the DataFrame based on start times
    df = df.sort_values(by='Start')

    # Iterate through the sorted DataFrame to find free time brackets
    previous_end = pd.to_datetime(captureTime)

    captureTime = pd.to_datetime(captureTime)
    capDuration = pd.to_timedelta(CaptureDuration, unit='s')
    endTime = captureTime + capDuration

    for _, row in df.iterrows():
        start_time = row['Start']
        duration = row['Duration']
        
        end_time = start_time + duration

        if start_time > previous_end:
            free_time_brackets.append((previous_end, start_time))

        previous_end = max(previous_end, end_time)

    # Check if there is free time after the last task
   
    if previous_end < endTime:
        free_time_brackets.append((previous_end, endTime))

    # Display the result
    # print("Time brackets with no tasks:")
    # for start, end in free_time_brackets:
    #     print(f"{start.strftime('%m:%d:%Y %H:%M:%S')} - {end.strftime('%m:%d:%Y %H:%M:%S')}")
    
    idle = []
    for i in range(len(free_time_brackets)):
        interval = free_time_brackets[i][1] - free_time_brackets[i][0]
        idle.append(interval.total_seconds())
    idle = pd.Series(idle)
    return idle

def _paralell_process(func, input_args, cores=0):
    if cores == 0:
        cores = os.cpu_count()
    with Pool(cores) as p:
        return p.starmap(func, input_args)

def getCaptureStartEndTime(inputDir):
    # use the timestamp as seen in cicFl0owMeter cause we are directly working with cicS
    cnt = 0
    minTime = pd.to_datetime("05/12/2025 08:15:00 PM")
    maxTime=  pd.to_datetime("05/12/2000 08:15:00 PM")
    for root, dirs, files in os.walk(inputDir):
        for file in files:
            if file.endswith(".csv"):
                data = pd.read_csv(os.path.join(root, file))
                currentTime = pd.to_datetime(data['Timestamp'], format='%m/%d/%Y %I:%M:%S %p')[0]
                minTime = min(currentTime, minTime)
                maxTime = max(currentTime, maxTime)
        print(root)
        cnt += 1
        print(cnt)
        if cnt  > 100:
            break

    print(f'minimum time is {minTime}')
    print(f'maximum time is {maxTime}')
    return (minTime, maxTime)

def feedUsersForTrafficPatter(inputDir, outputDir):
    captureStartTime = "2022-05-12 8:15:00 PM"
    captureStartTime= pd.to_datetime(captureStartTime)

    captureDuration = 15 * 60 # in seconds
    # read all files in the directory and put them in a list
    inputDir = "/mnt/md0/jaber/groupedCIC/"
    outputDir = "/mnt/md0/jaber/groupedUserBehavior/"
    cnt = 0
    argList = []
    for root, dirs, files in os.walk(inputDir):
        print(root)
        cnt+=1
        print(cnt)
        argList.append((root, outputDir,captureStartTime,captureDuration))

    _paralell_process(userTrafficPattern, argList)

    argList = []




if __name__ == '__main__':
    # Get start time and capture duration from here and use it in feedUsersForT
    # mintime , maxtime = getCaptureStartEndTime( "/mnt/md0/jaber/groupedCIC/")
    # use the timestamp as seen in cicFl0owMeter cause we are directly working with CICs
    print("here")

    feedUsersForTrafficPatter("/mnt/md0/jaber/groupedCIC/", "/mnt/md0/jaber/groupedUserBehavior/")
    # userTrafficPattern(userFlows, outputDir )

