#!/bin/bash

count=0
while [ $count -lt 20 ]
do
  echo "Starting Draw instance #$count"
  java -client -Xms4m -Xmx12m JGroups.Demos.Draw &
#  sleep 1
  count=$(($count+1))
done