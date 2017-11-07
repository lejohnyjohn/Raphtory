#!/usr/bin/env bash 
 
IP="$(./getMyIP.sh)" 
 
ZooKeeper="192.168.1.245:2180" 
 
Image="quay.io/miratepuffin/cluster" #if you want to use prebuilt one on my quay.io 
 
NumberOfPartitions=5
 
JVM="-Dcom.sun.management.jmxremote.rmi.port=9090 -Dcom.sun.management.jmxremote=true -Dcom.sun.management.jmxremote.port=9090  -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.local.only=false -Djava.rmi.server.hostname=$IP" 
if [ ! -d logs ]; then mkdir logs; fi 
rm -r logs/machine1Setup
if [ ! -d logs/machine1Setup ]; then mkdir logs/machine1Setup; fi 
if [ ! -d logs/machine1Setup/entityLogs/ ]; then mkdir logs/machine1Setup/entityLogs/; fi 
entityLogs=$(pwd)"/logs/machine1Setup/entityLogs" 
 
chmod 777 logs 
chmod 777 logs/machine1Setup
chmod 777 logs/machine1Setup/entityLogs
 
SeedPort=9101 
RestPort=9102 
UpdatePort=9103 
BenchmarkPort=9104 
 
LiveAnalysisPort=9105 
 
ClusterUpPort=9106 
 
PM1Port=9201
PM3Port=9203
PM1ID=1
PM3ID=3
(docker run -p $PM1Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM1Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $PM1ID $NumberOfPartitions $ZooKeeper &) > logs/machine1Setup/partitionManager1.txt 
sleep 2 
echo "Partition Manager $PM1ID up and running at $IP:$PM1Port" 
 
(docker run -p $PM3Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$PM3Port" -v $entityLogs:/logs/entityLogs $Image partitionManager $PM3ID $NumberOfPartitions $ZooKeeper &) > logs/machine1Setup/partitionManager3.txt 
sleep 2 
echo "Partition Manager $PM3ID up and running at $IP:$PM3Port" 
 
Router1Port=9301
Router3Port=9303
(docker run -p $Router1Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$Router1Port" $Image router $NumberOfPartitions $ZooKeeper &) > logs/machine1Setup/router1.txt 
sleep 1 
echo "Router 1 up and running at $IP:$Router1Port" 
 
(docker run -p $Router3Port:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$Router3Port" $Image router $NumberOfPartitions $ZooKeeper &) > logs/machine1Setup/router3.txt 
sleep 1 
echo "Router 3 up and running at $IP:$Router3Port" 
 
(docker run -p $ClusterUpPort:2551  --rm -e "HOST_IP=$IP" -e "HOST_PORT=$ClusterUpPort" $Image ClusterUp $NumberOfPartitions $ZooKeeper &) > logs/machine1Setup/ClusterUp.txt 
sleep 1 
echo "CLUSTER UP"
 
