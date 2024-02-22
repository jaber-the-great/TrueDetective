import numpy as np
import matplotlib.pyplot as plt
import os
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import confusion_matrix, accuracy_score
from sklearn import datasets
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report


from sklearn.tree import DecisionTreeClassifier
from sklearn.neighbors import KNeighborsClassifier
from sklearn.svm import SVC
from sklearn.linear_model import LogisticRegression
from sklearn.naive_bayes import GaussianNB
from sklearn.ensemble import RandomForestClassifier
from sklearn.svm import SVC
# use_cuda = torch.cuda.is_available()
# device = torch.device("cuda" if use_cuda else "cpu")
# print("Device: ",device)
# device = torch.device("cuda:2" if use_cuda else "cpu")
def renameColumns(numberOfPackets):
    # change column names:
    # TCP: 2 src port 2 dst port 4 seq 4 ack 2flags  2 windows 2 checksum, 2 urgent pointer
    # IP: 1 VersionIHL, 1 TOS, 2 total length, 2 identification, 2 flagsFragment, 1 TTL, 1 protocl , 2 checksum, 4 src, 4 dst
    IPheaderList = ['VersionIHL', 'TOS', 'Total_length1', 'Total_length1', 'Identification1', 'Identification2', 'FlagFrag1',
                    'FlagFrag2', 'TTL', 'Protocol', 'L2Checksum1', 'L2Checksum2', 'SrcIP1', 'SrcIP2',
                    'SrcIP3', 'SrcIP4', 'DstIP1', 'DstIP2', 'DstIP3', 'DstIP4']
    TCPHeaderList = ['SrcPort1', 'SrcPort2', 'DstPort1', 'DstPort2', 'Seq1', 'Seq2', 'Seq3', 'Seq4', 'Ack1', 'Ack2',
                     'Ack3', 'Ack4', 'Flags1', 'Flags2', 'Window1', 'Window2', 'L3Checksum1', 'L3Checksum2',
                     'urgent1', 'urgent2']



    columnsToRename = {}
    # TODO: Name changes: Each packet header different name, each user pair have different name

    for packetIndex in range(numberOfPackets):
        for i in range(len(IPheaderList)):
            name = f'{str(packetIndex * 40 + i + 1)}'
            columnsToRename[name] = f'{IPheaderList[i]}_{packetIndex}'

        for i in range(len(TCPHeaderList)):
            name = f'{str(packetIndex * 40 + i + 21)}'
            columnsToRename[name] = f'{TCPHeaderList[i]}_{packetIndex}'

    for packetIndex in range(numberOfPackets):
        for i in range(len(IPheaderList)):
            name = f'xbyte_{str(packetIndex * 40 + i + 1)}'
            columnsToRename[name] = f'x{IPheaderList[i]}_{packetIndex}'

        for i in range(len(TCPHeaderList)):
            name = f'x{str(packetIndex * 40 + i + 21)}'
            columnsToRename[name] = f'x{TCPHeaderList[i]}_{packetIndex}'

    # print(columnsToRename)
    return columnsToRename


def trainTestDifferentModels(datasetFile):
    # df1 = pd.read_pickle('/home/jaber/TrueDetective/dataset/90.pkl')
    # df2 = pd.read_pickle('/home/jaber/TrueDetective/dataset/91.pkl')
    # df3 = pd.read_pickle('/home/jaber/TrueDetective/dataset/92.pkl')
    # dataset = pd.concat([df1,df2,df3])
    dataset = pd.read_csv(datasetFile)
    datasetTest = pd.read_csv(f'/mnt/md0/jaber/testSet/valid20users.json.csv')
    columnsToRename = renameColumns(5)
    dataset.rename(columns=columnsToRename, inplace=True)
    datasetTest.rename(columns=columnsToRename, inplace=True)
    # FieldsToDrop = ['Flow ID', 'Timestamp', 'User Label', 'Label' ,'Outside IP','L2Checksum1','L2Checksum2', 'SrcIP1','SrcIP2','SrcIP3','SrcIP4','DstIP1','DstIP2','DstIP3','DstIP4','L3Checksum1','L3Checksum2']
    FieldsToDrop = ['User IP', 'Flow ID', 'Timestamp', 'User Label', 'Label' ,'Outside IP','L2Checksum1','L2Checksum2', 'SrcIP1','SrcIP2','SrcIP3','SrcIP4','DstIP1','DstIP2','DstIP3','DstIP4','L3Checksum1','L3Checksum2']

    # TODO: Drop irrelevant columns, maybe change some of them instead of dropping, especially timestamp
    # FieldsToDrop = ['Flow ID', 'Timestamp', 'User IP', 'Label' ,'Outside IP','L2Checksum1','L2Checksum2', 'SrcIP1','SrcIP2','SrcIP3','SrcIP4','DstIP1','DstIP2','DstIP3','DstIP4','L3Checksum1','L3Checksum2']
    columns_to_drop = []
    for filed in FieldsToDrop:
        for col in dataset.columns:
            if filed in col:
                columns_to_drop.append(col)
    dataset.drop(columns=columns_to_drop, inplace= True)
    datasetTest.drop(columns=columns_to_drop, inplace=True)

    X = dataset.iloc[:, :-1].values
    y = dataset.iloc[:, -1].values

    # Automatically scrambles the data

    #X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)
    X_train = X
    y_train = y
    X_test = datasetTest.iloc[:, :-1].values
    y_test = datasetTest.iloc[:, -1].values


    sc = StandardScaler()
    X_train = sc.fit_transform(X_train)
    X_test = sc.transform(X_test)

    # print("Feature scaling done")
    # clf = RandomForestClassifier(n_estimators=100)
    # clf.fit(X_train, y_train)
    # y_pred = clf.predict(X_test)
    #
    # print("Classification done")

    # trustee = ClassificationTrustee(expert=clf)
    # trustee.fit(X_train, y_train, num_iter=50, num_stability_iter=10, samples_size=0.3, verbose=True)
    # dt, pruned_dt, agreement, reward = trustee.explain()
    # dt_y_pred = dt.predict(X_test)
    #
    # print("Model explanation global fidelity report:")
    # print(classification_report(y_pred, dt_y_pred))
    # print("Model explanation score report:")
    # print(classification_report(y_test, dt_y_pred))

    #
    # models = []
    # print("Decision Tree")
    # DT= DecisionTreeClassifier(criterion = 'entropy', random_state = 0)
    # models.append(DT)
    # # 91.5
    #
    # print("K Nearest Neighbor")
    # KNN = KNeighborsClassifier(n_neighbors = 5, metric = 'minkowski', p = 2)
    # models.append(KNN)
    # #78
    #
    # # print("Kernel SVM")
    # # Support = SVC(kernel = 'rbf', random_state = 0)
    # # models.append(Support)
    # # # 80
    # print("Logistic Regression")
    # LR = LogisticRegression(random_state = 0, max_iter=10000)
    # models.append(LR)
    # # 59
    #
    # print("Naive Bayes")
    # NB = GaussianNB()
    # models.append(NB)
    # # # 51.4
    #
    models = []
    print("Random Forest")
    RF = RandomForestClassifier(n_estimators = 10, criterion = 'entropy', random_state = 0)
    models.append(RF)
    # # 96.6
    # #
    # # print("SVM")
    # # svm = SVC(kernel = 'linear', random_state = 0)
    # # models.append(svm)
    #
    outputFilename = datasetFile.split('/')[-1]
    result = open(f'/mnt/md0/jaber/{outputFilename}.txt', "w")
    for item in models:
        print(item)
        result.write(f'{item}\n')
        item.fit(X_train, y_train)
        y_pred = item.predict(X_test)
        # cm = confusion_matrix(y_test, y_pred)
        # print(cm)
        feature_importances = item.feature_importances_

        # Get the indices of features sorted by importance
        sorted_indices = np.argsort(feature_importances)[::-1]

        # Print the feature ranking
        print("Feature ranking:")
        for i, feature_index in enumerate(sorted_indices):

            print(f"{i + 1}. Feature {feature_index} ({dataset.columns.tolist()[feature_index]}): {feature_importances[feature_index]}")
            result.write(f"{i + 1}. Feature {feature_index} ({dataset.columns.tolist()[feature_index]}): {feature_importances[feature_index]}\n")
            if i > 20:
                break

        cm = confusion_matrix(y_test, y_pred)
        print(f'confusion matrix:\n {cm}')
        result.write(f'confusion matrix:\n {cm}')
        print("Test accuracy")
        print(accuracy_score(y_test, y_pred))
        result.write(f'Test accuracy:\n{accuracy_score(y_test, y_pred)}\n')
        print("Train accuracy:")
        print(accuracy_score(y_train, item.predict(X_train)))
        result.write(f'Test accuracy:\n{accuracy_score(y_train, item.predict(X_train))}\n')
        result.close()


if __name__ == "__main__":
    for subdir, dirs, files in os.walk('/mnt/md0/jaber/MoreDatasets/'):
        for file in files:
            if file.endswith(".csv"):
                trainTestDifferentModels(subdir + file)
    # trainTestDifferentModels('/mnt/md0/jaber/datasets/5_1000.json_FullCombination.csv')