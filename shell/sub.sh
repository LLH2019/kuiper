#! /bin/bash

mosquitto_sub -t devices/demo_001/messages/events2 |ts '[%Y-%m-%d %H:%M:%S]' | tee >>a.log | date >> a.log
aa=${PIPESTATUS[0]} 
if [ $aa -ne 0 ] 
then 
    echo "aaaaaaaaaaaaaaaa" 
fi
