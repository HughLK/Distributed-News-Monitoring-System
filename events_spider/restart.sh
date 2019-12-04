#!/bin/sh

ps -ef | grep "rpc" | grep -v "grep"

if [ "$?" -eq 1 ]

then

	/usr/bin/python2.7 /home/yuqing/spider/events_spider/rpc_server.py>/dev/null 2>&1 &

fi