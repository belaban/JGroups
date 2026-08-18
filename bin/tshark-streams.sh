#!/bin/bash

# Prints the conversations for each individual TCP stream (assuming we have 10)
# To find out the streams: tshark -r <PCAPNG file> -Tfields -etcp.stream|sort|uniq

for stream in 0 1 2 3 4 5 6 7 8 9; do
  tshark -r 273.pcapng -qz "follow,tcp,raw,$stream" 2>/dev/null \
    | grep -v "^===\|^Follow\|^Filter\|^Node\|^$" | sed 's/^\t//' \
    | java org.jgroups.tests.ParseMessages -tcp -parse-discovery-responses true
done
