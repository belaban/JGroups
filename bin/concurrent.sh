#!/bin/bash


count=0
while [ $count -lt 20 ]
do
  echo "Starting Draw instance #$count"
  # change the IP address to your system
  jgroups.sh org.jgroups.demos.Draw -props /home/bela/udp.xml -name $count  &
  #  sleep 1
  count=$(($count+1))
done