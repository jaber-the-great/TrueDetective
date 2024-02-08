import tensorflow as tf
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense

def renameColumns(numberOfPackets):
        IPheaderList = ['VersionIHL', 'TOS','Total_length1', 'Total_length1', 'Identification1','Identification2','FlagFrag1','FlagFrag2','TTL','Protocol','L2Checksum1','L2Checksum2','SrcIP1','SrcIP2','SrcIP3','SrcIP4','DstIP1','DstIP2','DstIP3','DstIP4']
        columnsToRename = {}
        for j in range(numberOfPackets):
            for i in range(len(IPheaderList)):
                name = f'byte_{str(j*40 + i+1)}'
                columnsToRename[name] = IPheaderList[i]

            TCPHeaderList = ['SrcPort1','SrcPort2','DstPort1','DstPort2','Seq1','Seq2','Seq3','Seq4','Ack1','Ack2','Ack3','Ack4','Flags1','Flags2','Window1','Window2','L3Checksum1','L3Checksum2','urgent1','urgent2']
            for i in range(len(TCPHeaderList)):
                name= f'byte_{str(j*40 + i + 21)}'
                columnsToRename[name] = TCPHeaderList[i]
        return columnsToRename



# Load the dataset, rename the columns and drop the unnecessary columns
dataset = pd.read_pickle('/mnt/md0/jaber/dataset.pkl')
columnsToRename = renameColumns(3)
dataset.rename(columns=columnsToRename, inplace=True)
columnsToDrop = ['Flow ID', 'Timestamp', 'User Label', 'Label','Outside IP','L2Checksum1','L2Checksum2', 'SrcIP1','SrcIP2','SrcIP3','SrcIP4','DstIP1','DstIP2','DstIP3','DstIP4','L3Checksum1','L3Checksum2']
dataset.drop(columns=columnsToDrop, inplace= True)

# Split the dataset into features and labels
X = dataset.iloc[:, :-1].values
y = dataset.iloc[:, -1].values

# Split the dataset into training and testing sets and scale the features
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 42)
sc = StandardScaler()
X_train = sc.fit_transform(X_train)
X_test = sc.transform(X_test)


# Define the model architecture
model = Sequential()
# TODO: Change the input shape to match the number of features
model.add(Dense(64, activation='relu', input_shape=(320,)))
model.add(Dense(64, activation='relu'))
model.add(Dense(1, activation='sigmoid'))

# Compile the model
model.compile(optimizer='adam', loss='binary_crossentropy', metrics=['accuracy'])


# Train the model
model.fit(train_X, train_y, epochs=10, batch_size=32)



# Evaluate the model on the testing set
loss, accuracy = model.evaluate(test_X, test_y)
print(f'Test loss: {loss}')
print(f'Test accuracy: {accuracy}')
