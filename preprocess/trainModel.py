import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import confusion_matrix, accuracy_score
from numba import jit, cuda
import torch
# use_cuda = torch.cuda.is_available()
# device = torch.device("cuda" if use_cuda else "cpu")
# print("Device: ",device)
# device = torch.device("cuda:2" if use_cuda else "cpu")
def renameColumns(numberOfPackets):
        # change column names:
        # TCP: 2 src port 2 dst port 4 seq 4 ack 2flags  2 windows 2 checksum, 2 urgent pointer
        # IP: 1 VersionIHL, 1 TOS, 2 total length, 2 identification, 2 flagsFragment, 1 TTL, 1 protocl , 2 checksum, 4 src, 4 dst
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
        # print(columnsToRename)
        return columnsToRename
df1 = pd.read_pickle('/home/jaber/TrueDetective/dataset/90.pkl')
df2 = pd.read_pickle('/home/jaber/TrueDetective/dataset/91.pkl')
df3 = pd.read_pickle('/home/jaber/TrueDetective/dataset/92.pkl')
dataset = pd.concat([df1,df2,df3])
dataset = pd.read_pickle('/home/jaber/TrueDetective/dataset/90.pkl')
columnsToRename = renameColumns(3)
dataset.rename(columns=columnsToRename, inplace=True)


# TODO: Drop irrelevant columns, maybe change some of them instead of dropping
columnsToDrop = ['Flow ID', 'Timestamp', 'User Label', 'Label','Outside IP','L2Checksum1','L2Checksum2', 'SrcIP1','SrcIP2','SrcIP3','SrcIP4','DstIP1','DstIP2','DstIP3','DstIP4','L3Checksum1','L3Checksum2']
dataset.drop(columns=columnsToDrop, inplace= True)
X = dataset.iloc[:, :-1].values
y = dataset.iloc[:, -1].values

# Automatically scrambles the data

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)


sc = StandardScaler()
X_train = sc.fit_transform(X_train)
X_test = sc.transform(X_test)

from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC

models = []
print("Decision Tree")
DT= DecisionTreeClassifier(criterion = 'entropy', random_state = 0)
models.append(DT)
# 91.5
#
# print("K Nearest Neighbor")
# KNN = KNeighborsClassifier(n_neighbors = 5, metric = 'minkowski', p = 2)
# models.append(KNN)
# 78

# print("Kernel SVM")
# Support = SVC(kernel = 'rbf', random_state = 0)
# models.append(Support)
# # 80
# print("Logistic Regression")
# LR = LogisticRegression(random_state = 0, max_iter=10000)
# models.append(LR)
# LR.fit(X_train, y_train)
#
# # 59
#
print("Naive Bayes")
NB = GaussianNB()
models.append(NB)
# # 51.4

print("Random Forest")
RF = RandomForestClassifier(n_estimators = 10, criterion = 'entropy', random_state = 0)
models.append(RF)
# # 96.6
#
# print("SVM")
# svm = SVC(kernel = 'linear', random_state = 0)
# models.append(svm)


for item in models:
    print(item)
    item.fit(X_train, y_train)
    y_pred = item.predict(X_test)
    # cm = confusion_matrix(y_test, y_pred)
    # print(cm)
    print(accuracy_score(y_test, y_pred))

