# TODO list for the project:

- [ ] Compute the GINI index and analyse the confusion matrix 
- [ ] Ignore less than 5 flows (check the stats)
- [ ] Use Tranalyzer for the data and add the relevant fields to cicFlowMeter
- [ ] Talk to Snajay and Roman about removing the datacenter locations and IPs
- [ ] Separte the columns for the flags and fields
- [ ] Using calsses for outside port? (defining the type of service for well known services)
- [ ] Should we consider the outside IP address?
- [ ] I am just considering TCP/UDP flows, should I consider ICMP etc?
- [ ] Do not drop some of the columns and then test accuracy (the columns that I considered irrelevent)
- [x] Check whether IAT identifies the service or it identifies users? (run statistical analysis on the number of services per user in flag 0-belong to the same user but different srevice using outside port and outsid IP) with median of 200 flows per user 
    * Ran for 3000 users, exclueding the ones with more than 1000 flows: Average flows: 168 and median 144, Average unique port 5.1 and median 4, average services 49.7 and median 43. Frequently used ports: 993, 5222, 5223, 80, 443
- [ ] Create datasets of length K users




# Notes:
* It is not a good idea to do split-by-clinet-ip in pcapsplitter cause we may have different pcap files etc. First get all the flows and then find the clients. Also, because flows are in both direction, that would be confusing and need some other sorts of preprocessing.
* 
