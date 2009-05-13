#!/bin/bash 
#Script file used for automated performance tests. Prior to running this script 
#user should use org.jgroups.tests.perf.PerformanceTestGenerator to generate 
#performance tests input files.
#
#For a succesful performance test running user should ensure the following:
# - can log into all machines listed in CLUSTER_NODES variable
# - CLASSPATH variable points to existing lib files
# - CONFIG_FILES is initialized to proper performance tests input files
# - JGROUPS_CONFIG_FILE is initialized to an existing JGroups stack conf file   

#lists all the computer nodes used for performance tests
CLUSTER_NODES=( cluster01.qa.atl.jboss.com cluster02.qa.atl.jboss.com cluster03.qa.atl.jboss.com cluster04.qa.atl.jboss.com cluster05.qa.atl.jboss.com cluster06.qa.atl.jboss.com cluster07.qa.atl.jboss.com cluster08.qa.atl.jboss.com )

USERID=bela


#classpath for performance tests
CLASSPATH='jgroups-all.jar'

#finds test configuration files
#note the running directory of this script and make sure that find command can 
#actually find configuration files
CONFIG_FILES=`find . -name 'config_*.txt'`

#JGroups configuration stack used in performance tests
JGROUPS_CONFIG_FILE="/home/${USERID}/udp.xml"
#JGROUPS_CONFIG_FILE="/home/${USERID}/tcp-nio.xml"

#sleeptime between performance test rounds (should be big enough to prevent test
#overlapping)
SLEEP_TIME=30

LOGIN_COMMAND='ssh -i rgreathouse@jboss.com.id_dsa vblagojevic@'

JVM_COMMAND='/opt/jdk1.5.0_06/bin/java -Djava.net.preferIPv4Stack=true'

LOGIN_COMMAND="${USERID}@"

JVM_COMMAND='java -Djava.net.preferIPv4Stack=true'

JVM_PARAM="-Xmx500M -Xms500M -XX:NewRatio=1 -XX:+AggressiveHeap -XX:+DisableExplicitGC -XX:CompileThreshold=100 -Dbind.address=\${MYTESTIP_1}"

#verify that we found configuration files
config_file_count=${#CONFIG_FILES[*]}
if [ $config_file_count -le "0" ] ; then
	echo Did not find performance test configuration files!
	exit
fi	

echo "starting performance tests..."
node_count=${#CLUSTER_NODES[@]} 
for file in $CONFIG_FILES;
do
	num_senders_line=`grep num_senders $file`
	num_senders=${num_senders_line:12}

	num_members_line=`grep num_members $file`
	num_members=${num_members_line:12}

	sender_count=1
	sender_or_receiver=" -sender "
	echo "starting performance test round for $file..."
	for (( i = 0 ; i < node_count ; i++ ))
	do
    	node=${CLUSTER_NODES[$i]}
		if [ $sender_count -le $num_senders ] ; then
			let "sender_count++"
		else
			sender_or_receiver=" -receiver "
		fi
		let j=$i+1
		if [ $j -eq $node_count ] ; then
			SSH_CMD="ssh"
			output_file="-f result-${file#.*/}"
		else
			SSH_CMD="ssh -f"
			output_file=""
		fi
		args="-config $file -props $JGROUPS_CONFIG_FILE $sender_or_receiver $output_file"
		final_command="$SSH_CMD $LOGIN_COMMAND$node $JVM_COMMAND $JVM_PARAM  org.jgroups.tests.perf.Test $args > /dev/null"
		echo starting $final_command on $node
		$final_command
		sleep 5
	done
	echo "Tests round is now running, waiting $SLEEP_TIME seconds for this test round to finish..."
	sleep $SLEEP_TIME
done


