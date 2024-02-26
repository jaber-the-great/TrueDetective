# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.metrics import classification_report
# from sklearn.metrics import roc_auc_score
import pandas as pd
import numpy as np
import matplotlib as plt
from sklearn.model_selection import train_test_split
from sklearn import datasets
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import classification_report
from sklearn.metrics import confusion_matrix, accuracy_score

# TODO: Is number of packets 5?
# TODO: Match to the fields that I am using
# Fields I dropped in ucsb data: FieldsToDrop = ['Flow ID', 'Timestamp', 'User Label', 'Label' ,'Outside IP','L2Checksum1','L2Checksum2', '
# SrcIP1','SrcIP2','SrcIP3','SrcIP4','DstIP1','DstIP2','DstIP3','DstIP4','L3Checksum1','L3Checksum2']
dfs = []
for i in range(100,201):
    df = pd.read_pickle(f'/mnt/md0/jaber/nptdata/{str(i)}.pkl')
    dfs.append(df)
dataset =  pd.concat(dfs, ignore_index=True)

columns_to_drop = []
dropls = []
num_packets = 5
FieldsToDrop = ['ipv4_dst_', 'ipv4_src_', 'prt', 'port', 'cksum', 'proto' , 'src_ip', 'User Label', 'payload_bit']
for filed in FieldsToDrop:
    for col in dataset.columns:
        if filed in col:
            columns_to_drop.append(col)
dataset.drop(columns=columns_to_drop, inplace= True)

X = dataset.iloc[:, :-1].values
y = dataset.iloc[:, -1].values

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)

sc = StandardScaler()
X_train = sc.fit_transform(X_train)
X_test = sc.transform(X_test)

print("Random Forest")
RF = RandomForestClassifier(n_estimators = 10, criterion = 'entropy', random_state = 0)
RF.fit(X_train, y_train)
y_pred = RF.predict(X_test)
print(accuracy_score(y_test, y_pred))
feature_importances = RF.feature_importances_

# Get the indices of features sorted by importance
sorted_indices = np.argsort(feature_importances)[::-1]

# Print the feature ranking
print("Feature ranking:")
for i, feature_index in enumerate(sorted_indices):
    if i > 40:
        break
    print(
        f"{i + 1}. Feature {feature_index} ({dataset.columns.tolist()[feature_index]}): {feature_importances[feature_index]}")

cm = confusion_matrix(y_test, y_pred)
print(f'confusion matrix:\n {cm}')
print(accuracy_score(y_test, y_pred))