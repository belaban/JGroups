
#!/bin/bash

# Extracts a given stream from a capture and prints its JGroups messages with ParseMessages'
# To find out the streams: tshark -r <PCAPNG file> -Tfields -etcp.stream|sort|uniq

if [ $# -lt 2 ];
    then echo "parse-streams.sh <PCAP file> <stream>";
         exit 1
fi


file=$1
stream=$2

shift
shift

arguments=$*
echo file is $file, stream is $stream, args are $arguments

tshark -r $file -qz "follow,tcp,raw,$stream" 2>/dev/null \
    | grep -v "^===\|^Follow\|^Filter\|^Node\|^$" | sed 's/^\t//' \
    | java org.jgroups.tests.ParseMessages -tcp -parse-discovery-responses true -show-views true $arguments