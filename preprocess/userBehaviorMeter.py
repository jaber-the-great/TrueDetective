import subprocess
import pandas as pd
from multiprocessing import Pool
import os
import datetime


#TODO: Open all cic files in each direcotry, put them in a dictionary,
#TODO: import and use python stat and collections modules for getting the stats 

input = ['/mnt/md0/jaber/groupedcicflow/169.231.141.110/s2f3_00000_20221205201500-45086.pcap_Flow.csv',
         '/mnt/md0/jaber/groupedcicflow/169.231.141.110/s3f0_00000_20221205201500-51228.pcap_Flow.csv',
          '/mnt/md0/jaber/groupedcicflow/169.231.141.110/s3f2_00000_20221205201500-24138.pcap_Flow.csv' , 
           '/mnt/md0/jaber/groupedcicflow/169.231.141.110/s3f3_00000_20221205201500-41813.pcap_Flow.csv']





def flowStat(userFlows):
    flows = []
    windowStat = {}
    for item in userFlows:
        flows.append( pd.read_csv(item))
    userFlows = pd.concat(flows)
    print(userFlows)
    
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
    # TODO: May need to change this to Outside IP
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
    # TODO: Sort the flows based on the timestamp, then calculate the inter arrival time
    # Convert timestamp to datetime object
    userFlows['Timestamp'] = pd.to_datetime(userFlows['Timestamp'], format='%m/%d/%Y %I:%M:%S %p')

    # Sort flows based on timestamp
    userFlows = userFlows.sort_values(by='Timestamp')
    print(windowStat)
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
    windowStat['minFlowIAT'] = intervals.min()
    windowStat['maxFlowIAT'] = intervals.max()
    windowStat['avgFlowIAT'] = intervals.mean()
    windowStat['stdFlowIAT'] = intervals.std()

    













    # Flow Packets

    # windowStat['totalPackets'] = windowStat['totalBwdBytes'] + windowStat['totalFwdBytes']  
    # windowStat['minPacketLen'] = userFlows['min_packet_length'].min()
    # windowStat['maxPacketLen'] = userFlows['max_packet_length'].max()
    # windowStat['avgPacketLen'] = userFlows['mean_packet_length'].mean()
    # windowStat['stdPacketLen'] = userFlows['mean_packet_length'].std()
    # windowStat['minIAT'] = userFlows['min_inter_arrival_time'].min()

    # # Inter Arrival Time
    # windowStat['maxIAT'] = userFlows['max_inter_arrival_time'].max()
    # windowStat['avgIAT'] = userFlows['mean_inter_arrival_time'].mean()
    # windowStat['stdIAT'] = userFlows['mean_inter_arrival_time'].std()

    # windowStat['minFwdIAT'] = userFlows['min_forward_inter_arrival_time'].min()
    # windowStat['maxFwdIAT'] = userFlows['max_forward_inter_arrival_time'].max()
    # windowStat['avgFwdIAT'] = userFlows['mean_forward_inter_arrival_time'].mean()
    # windowStat['stdFwdIAT'] = userFlows['mean_forward_inter_arrival_time'].std()


    # # flow idle time
    # windowStat['minIdleTime'] = userFlows['min_flow_idle_time'].min()
    # windowStat['maxIdleTime'] = userFlows['max_flow_idle_time'].max()
    # windowStat['avgIdleTime'] = userFlows['mean_flow_idle_time'].mean()
    # windowStat['stdIdleTime'] = userFlows['mean_flow_idle_time'].std()
    # i\
    # # flow active time
    # windowStat['minActiveTime'] = userFlows['min_flow_active_time'].min()



def flowIdleTime():
    # TODO: Edit this function and make it faster
    # Sample data: start times and durations of tasks
    data = {'Start': ['01:01:2022 00:00:15', '01:01:2022 00:00:30', '01:01:2022 00:00:10'],
            'Duration': ['00:00:10', '00:00:15', '00:00:05']}

    df = pd.DataFrame(data)

    # Convert start times and durations to datetime
    df['Start'] = pd.to_datetime(df['Start'])
    df['Duration'] = pd.to_timedelta(df['Duration'])

    # Create a list to store time brackets with no tasks
    free_time_brackets = []

    # Sort the DataFrame based on start times
    df = df.sort_values(by='Start')

    # Iterate through the sorted DataFrame to find free time brackets
    previous_end = pd.to_datetime('01:01:2022 00:00:00')

    for _, row in df.iterrows():
        start_time = row['Start']
        duration = row['Duration']
        
        end_time = start_time + duration

        if start_time > previous_end:
            free_time_brackets.append((previous_end, start_time))

        previous_end = max(previous_end, end_time)

    # Check if there is free time after the last task
    if previous_end < pd.to_datetime('01:01:2022 00:01:00'):
        free_time_brackets.append((previous_end, pd.to_datetime('01:01:2022 00:01:00')))

    # Display the result
    print("Time brackets with no tasks:")
    for start, end in free_time_brackets:
        print(f"{start.strftime('%m:%d:%Y %H:%M:%S')} - {end.strftime('%m:%d:%Y %H:%M:%S')}")




# flowStat(input)
flowIdleTime()