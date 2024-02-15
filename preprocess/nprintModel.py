# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestClassifier
# from sklearn.metrics import classification_report
# from sklearn.metrics import roc_auc_score
import pandas as pd
import numpy as np
import matplotlib as plt

# TODO: Is number of packets 5?
# TODO: Match to the fields that I am using
dropls = []
num_packets = 5
for i in range(num_packets):
    for j in range(32):
        dropls.append("ipv4_dst_" + str(j) + "_" + str(i))
        dropls.append("ipv4_src_" + str(j) + "_" + str(i))
    for j in X.columns.values.tolist():
        if (
            "flow" in j
            or "Timestamp" in j
            or "prt" in j
            or "port" in j
            or "chksum" in j
            or "proto" in j
        ):  # or "ttl" in j or "seq" in j or "ack" in j:
            dropls.append(j)
        if "payload" in j:
            if int(j.split("_")[2]) > 96:
                dropls.append(j)