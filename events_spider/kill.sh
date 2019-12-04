#!/bin/bash
#获取XXX项目进程ID
SERVICE_NAME="statisticsservice-0.0.1-SNAPSHOT.jar"
service_pid=`ps -ef | grep rpc | grep -v grep | awk '{print $2}'`
 
 
echo "进程ID为：$service_pid"
 
 
 
 
#杀进程
echo "kill service..."
for id in $service_pid
do
	kill -9 $id 
done
 
 
echo "$service_pid已杀死..." 
 
 
echo "重启中..." 
./restart.sh
