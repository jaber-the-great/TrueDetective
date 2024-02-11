## Running CICflowMeter 
* java -Djava.library.path=/data/CICFlowMeter/jnetpcap/linux/jnetpcap-1.4.r1425/ -jar /data/CICFlowMeter/build/libs/CICFlowMeter-4.0.jar <source_folder> <dest_folder>

## Better way of running CIC
* docker run -v <filename.pcap>:/tmp/<filename.pcap> -v <output_folder>:/tmp/output --rm pinot.cs.ucsb.edu/cicflowmeter:latest /tmp/<filename.pcap> /tmp/output
* docker run -v /home/jaber/TrueDetective/first.pcap:/tmp/first.pcap -v /home/jaber/TrueDetective/preprocess/:/tmp/output --rm pinot.cs.ucsb.edu/cicflowmeter:latest /tmp/first.pcap /tmp/output


## CIC for multiple files
* docker run -v /home/jaber/TrueDetective/smallsplit/:/tmp/server_ndt -v /home/jaber/TrueDetective/smallcic/:/tmp/output/ --entrypoint /bin/bash --rm pinot.cs.ucsb.edu/cicflowmeter:latest -c "ls /tmp/server_ndt/*.pcap | parallel java -Djava.library.path=/CICFlowMeter/jnetpcap/linux/jnetpcap-1.4.r1425/ -jar build/libs/CICFlowMeter-4.0.jar {} /tmp/output/"

## CIC for mulitple file (no limit for argument length)
docker run -v /home/jaber/new15min/s2f1/:/tmp/server_ndt -v /home/jaber/cic:/tmp/output/ --entrypoint /bin/bash --rm pinot.cs.ucsb.edu/cicflowmeter:latest -c "find /tmp/server_ndt/ -type f -name "*.pcap" | parallel java -Djava.library.path=/CICFlowMeter/jnetpcap/linux/jnetpcap-1.4.r1425/ -jar build/libs/CICFlowMeter-4.0.jar {} /tmp/output/"

### Split large pcap:
tcpdump -r "input" -w "output" -C 10
