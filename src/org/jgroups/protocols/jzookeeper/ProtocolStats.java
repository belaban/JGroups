package org.jgroups.protocols.jzookeeper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;

/*
 * It uses to gathering protocol stats like throughput, latency and load.
 */
public class ProtocolStats {
	private List<Long> latencies;
	private List<Integer> fromfollowerToLeaderF;
	private List<Integer> fromLeaderToFollowerP;
	private List<Integer> latencyPropForward;
	private Map<MessageId, Long> latencyProposalForwardST;
	private List<Integer> latencyProp;
	private Map<MessageId, Long> latencyProposalST;
	//private List<Integer> throughputInRate;
	private Map<String, Double> throughputs;
	// private List<Integer> fromFollowerToLeaderA1;
	// private List<Integer> fromFollowerToLeaderA2;
	// private List<Integer> fromFollowerToLeaderF1;
	// private List<Integer> fromFollowerToLeaderF2;
	private int throughput;
	private long startThroughputTime;
	private long endThroughputTime;
	private long lastThroughputTime;
	private String protocolName;
	private int numberOfSenderInEachClient;
	private int numberOfClients;
	private AtomicInteger numRequest;
	private AtomicInteger numReqDelivered;
	private AtomicInteger lastNumReqDeliveredBefore;
	private AtomicInteger countMessageLeader;
	private AtomicInteger countTotalMessagesFollowers;
	private AtomicInteger countMessageFollower;
	private static PrintWriter outFile;
	private static PrintWriter outFileToWork;
	private static PrintWriter outFileToWorkLatency;

	private String outDir;
	private String outDirWork;

	private AtomicInteger countDummyCall;
	private boolean is_warmup = true;
	protected final Log log = LogFactory.getLog(this.getClass());

	public ProtocolStats() {

	}

	public ProtocolStats(String protocolName, int numberOfClients,
			int numberOfSenderInEachClient, String outDir, String outDirWork,
			boolean stopWarmup) {

		this.latencies = new ArrayList<Long>();
		this.fromfollowerToLeaderF = new ArrayList<Integer>();
		this.fromLeaderToFollowerP = new ArrayList<Integer>();
		this.latencyProp = new ArrayList<Integer>();
		;
		this.latencyProposalST = Collections
				.synchronizedMap(new HashMap<MessageId, Long>());
		this.latencyPropForward = new ArrayList<Integer>();
		;
		this.latencyProposalForwardST = Collections
				.synchronizedMap(new HashMap<MessageId, Long>());
		//this.throughputInRate = new ArrayList<Integer>();
		this.throughputs = new TreeMap<String, Double>();;
		// this.fromFollowerToLeaderA1 = new ArrayList<Integer>();
		// this.fromFollowerToLeaderA2 = new ArrayList<Integer>();
		// this.fromFollowerToLeaderF1 = new ArrayList<Integer>();;
		// this.fromFollowerToLeaderF2 = new ArrayList<Integer>();;
		this.throughput = 0;
		this.protocolName = protocolName;
		this.numberOfClients = numberOfClients;
		this.numberOfSenderInEachClient = numberOfSenderInEachClient;
		this.numRequest = new AtomicInteger(0);
		this.numReqDelivered = new AtomicInteger(0);
		this.lastNumReqDeliveredBefore = new AtomicInteger(0);
		countMessageLeader = new AtomicInteger(0);
		countMessageFollower = new AtomicInteger(0);
		;
		countTotalMessagesFollowers = new AtomicInteger(0);
		this.countDummyCall = new AtomicInteger(0);
		this.is_warmup = stopWarmup;
		this.outDir = outDir;
		this.outDirWork = outDirWork;
		try {
			this.outFile = new PrintWriter(new BufferedWriter(new FileWriter(
					outDir + InetAddress.getLocalHost().getHostName()
							+ protocolName + ".log", true)));
			this.outFileToWork = new PrintWriter(new BufferedWriter(
					new FileWriter(outDirWork + protocolName + ".log", true)));
			this.outFileToWorkLatency = new PrintWriter(new BufferedWriter(
					new FileWriter(outDirWork + "latency.log", true)));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public int getLastNumReqDeliveredBefore() {
		return lastNumReqDeliveredBefore.get();
	}

	public void setLastNumReqDeliveredBefore(int lastNumReqDeliveredBefore) {
		this.lastNumReqDeliveredBefore.set(lastNumReqDeliveredBefore);
	}

	public List<Long> getLatencies() {
		return latencies;
	}

	public void setLatencies(List<Long> latencies) {
		this.latencies = latencies;
	}

	public int getThroughput() {
		return throughput;
	}

	public void setThroughput(int throughput) {
		this.throughput = throughput;
	}

	public long getStartThroughputTime() {
		return startThroughputTime;
	}

	public void setStartThroughputTime(long startThroughputTime) {
		this.startThroughputTime = startThroughputTime;
	}

	public long getEndThroughputTime() {
		return endThroughputTime;
	}

	public void setEndThroughputTime(long endThroughputTime) {
		this.endThroughputTime = endThroughputTime;
	}

	public String getProtocolName() {
		return protocolName;
	}

	public void setProtocolName(String protocolName) {
		this.protocolName = protocolName;
	}

	public int getNumberOfSenderInEachClient() {
		return numberOfSenderInEachClient;
	}

	public void setNumberOfSenderInEachClient(int numberOfSenderInEachClient) {
		this.numberOfSenderInEachClient = numberOfSenderInEachClient;
	}

	public int getNumberOfClients() {
		return numberOfClients;
	}

	public void setNumberOfClients(int numberOfClients) {
		this.numberOfClients = numberOfClients;
	}

	public AtomicInteger getNumRequest() {
		return numRequest;
	}

	public void setNumRequest(AtomicInteger numRequest) {
		this.numRequest = numRequest;
	}

	public int getnumReqDelivered() {
		return numReqDelivered.get();
	}

	public AtomicInteger getcountDummyCall() {
		return countDummyCall;
	}

	public void setnumReqDelivered(AtomicInteger numReqDelivered) {
		this.numReqDelivered = numReqDelivered;
	}

	public AtomicInteger getCountMessageLeader() {
		return countMessageLeader;
	}

	public void setCountMessageLeader(AtomicInteger countMessageLeader) {
		this.countMessageLeader = countMessageLeader;
	}

	public AtomicInteger getCountTotalMessagesFollowers() {
		return countTotalMessagesFollowers;
	}

	public void setCountTotalMessagesFollowers(int countTotalMessagesFollowers) {
		this.countTotalMessagesFollowers.set(countTotalMessagesFollowers);
	}

	public String getOutdir() {
		return outDir;
	}

	public AtomicInteger getCountMessageFollower() {
		return countMessageFollower;
	}

	public void setCountMessageFollower(int countMessageFollower) {
		this.countMessageFollower.addAndGet(countMessageFollower);
	}

	public void incNumRequest() {
		numRequest.incrementAndGet();
	}

	public void incnumReqDelivered() {
		numReqDelivered.incrementAndGet();
	}

	public void addCountTotalMessagesFollowers(int countTotalMessages) {
		this.countTotalMessagesFollowers.addAndGet(countTotalMessages);
	}

	public void InCountDummyCall() {
		countDummyCall.incrementAndGet();
	}

	public void addLatency(long latency) {
		latencies.add(latency);
	}

	public void addLatencyFToLF(int latency) {
		fromfollowerToLeaderF.add(latency);
	}

	public void addLatencyLToFP(int latency) {
		fromLeaderToFollowerP.add(latency);
	}

	public void addLatencyProp(int latency) {
		latencyProp.add(latency);
	}

	public void addLatencyProposalST(MessageId mid, Long st) {
		latencyProposalST.put(mid, st);
	}

	public Long getLatencyProposalST(MessageId mid) {
		return latencyProposalST.get(mid);
	}

	public void removeLatencyProposalST(MessageId mid) {
		latencyProposalST.remove(mid);
	}

	public void addLatencyPropForward(int latency) {
		latencyPropForward.add(latency);
	}

	public void addLatencyProposalForwardST(MessageId mid, Long st) {
		latencyProposalForwardST.put(mid, st);
	}

	public Long getLatencyProposalForwardST(MessageId mid) {
		return latencyProposalForwardST.get(mid);
	}

	public void removeLatencyProposalForwardST(MessageId mid) {
		latencyProposalForwardST.remove(mid);
	}
	
	//Add Throughput using rate
//	public void addThroughput(int thr) {
//		throughputInRate.add(thr);
//	}
	
	//Add Throughput using rate
	public void addThroughput(String time, double thr){
		throughputs.put(time, thr);
	}

	public long getLastThroughputTime() {
		return lastThroughputTime;
	}

	public void setLastThroughputTime(long lastThroughputTime) {
		this.lastThroughputTime = lastThroughputTime;
	}
	public void incCountMessageFollower() {
		this.countMessageFollower.incrementAndGet();
	}

	public void incCountMessageLeader() {
		this.countMessageLeader.incrementAndGet();
	}

	public boolean isWarmup() {
		return is_warmup;
	}

	public void setWarmup(boolean is_warmup) {
		this.is_warmup = is_warmup;
	}

	public void printProtocolStats(boolean isLeader) {

		// print Min, Avg, and Max latency
		List<Long> latAvg = new ArrayList<Long>();
		int count = 0;
		long avgTemp = 0;
		long min = Long.MAX_VALUE, avg = 0, max = Long.MIN_VALUE, FToLFAvg = 0, LToFPAvg = 0, avgAll = 0, latProp = 0, latLeader = 0;
		double latPropD = 0, FToLFAvgD = 0, LToFPAvgD = 0, avgAllD = 0, latLeaderD = 0, throughputRate =0, tempSumThr=0;

		printLatencyToFile(latencies);
		for (long lat : latencies) {
			if (lat < min) {
				min = lat;
			}
			if (lat > max) {
				max = lat;
			}
			avg += lat;
			avgTemp += lat;
			count++;
			if (count > 10000) {
				latAvg.add(avgTemp / count);
				count = 0;
				avgTemp = 0;
			}

		}
		// avgAllD = (double) avg/latencies.size();
		// if(!protocolName.equals("ZabCoinTossing")){
		// if (isLeader){
		// for (long lat : fromLeaderToFollowerP) {
		// LToFPAvg += lat;
		// }
		// LToFPAvgD = (double) LToFPAvg/fromLeaderToFollowerP.size();
		//
		// for (long lat : latencyProp) {
		// latProp += lat;
		// }
		// latPropD = (double) latProp/latencyProp.size();
		//
		// for (long lat : latencyPropForward) {
		// latLeader += lat;
		// }
		// latLeaderD = (double) latLeader/latencyPropForward.size();
		// }
		// else{
		// for (long lat : fromfollowerToLeaderF) {
		// lat += lat;
		// }
		// FToLFAvgD = (double) FToLFAvg/fromfollowerToLeaderF.size();

		// }
		// }

		outFile.println("Start");
		// outFileToWork.println("Start");
		outFile.println(protocolName + "/" + numberOfClients + "/"
				+ numberOfSenderInEachClient);
		// outFileToWork.println(protocolName + "/" + numberOfClients + "/" +
		// numberOfSenderInEachClient);
		outFile.println("Number of Request Recieved = " + (numRequest));
		// outFileToWork.println("Number of Request Recieved = " +
		// (numRequest));
		outFile.println("Number of Request Deliever = " + numReqDelivered);
		// outFileToWork.println("Number of Request Deliever = " +
		// numReqDelivered);
		outFile.println("Total Load generates by Atomic broadcast = "
				+ (countMessageLeader.get() + countTotalMessagesFollowers.get()));
		// outFileToWork.println("Total Load generates by Atomic broadcast = "
		// + (countMessageLeader.get() + countTotalMessagesFollowers.get()));
		outFile.println("Throughput = "
				+ (numReqDelivered.get() / (TimeUnit.MILLISECONDS
						.toSeconds(endThroughputTime - startThroughputTime))));
		
		for (double th:throughputs.values()){
			tempSumThr+=th;
		}
		throughputRate = tempSumThr/throughputs.size();
		outFile.println("Throughput Rates = " + throughputs);
		outFile.println("Throughput Rate average = " + throughputRate);
		// outFileToWork.println("Throughput = "
		// + (numReqDelivered.get() / (TimeUnit.MILLISECONDS
		// .toSeconds(endThroughputTime - startThroughputTime))));

		// outFile.println("Latency average" + latAvg
		// + " numbers avg = " + latAvg.size());
		// outFileToWork.println("Latency average" + latAvg
		// + " numbers avg = " + latAvg.size());
		latnecyDistribution(latencies);
		avgAllD = average(latencies);
		outFile.println("All Latency average " + (avgAllD) / 1000000);
		// outFileToWork.println("All Latency average " + (avgAllD)/1000000);
		// if(!protocolName.equals("ZabCoinTossing")){
		// if (isLeader){
		// outFile.println("Latency From Leader to Follower (round-trip) (Proposal) "
		// + (double)(LToFPAvgD)/1000000);
		// outFile.println("Latency From FORWARD case to Proposal sent (Proposal) "
		// + "latencyProp Size " +
		// latencyProp.size() + " " +(double)(latPropD)/1000000);
		// outFile.println("Latency From leader to Proposal sent (Proposal) " +
		// "latencyPropForward Size " +
		// latencyPropForward.size() + " " +(double)(latLeaderD)/1000000);
		// }
		// else{
		// outFile.println("Latency From Folower to Leader (round-trip) (Forward) "
		// + (double)(FToLFAvgD)/1000000);
		//
		// }
		// }
		// else{
		// outFile.println("Number of call Dummy " + countDummyCall.get());
		//
		// }

		outFile.println("Test Generated at "
				+ new Date()
				+ " /Lasted for = "
				+ TimeUnit.MILLISECONDS
						.toSeconds((endThroughputTime - startThroughputTime)));
		//outFileToWork.println("Test Generated at "
				//+ new Date()
				//+ " /Lasted for = "
				//+ TimeUnit.MILLISECONDS
						//.toSeconds((endThroughputTime - startThroughputTime)));
		outFile.println("End");
		//outFileToWork.println("End");
		outFile.println();
		//outFileToWork.println();

		outFile.close();
		System.out.println("Finished");
		//outFileToWork.close();
		// printLatencyToFile(latencies);

	}

	public double average(List<Long> data) {
		long sum = 0;
		double avg = 0;
		for (int i = 0; i < data.size(); i++) {
			if (data.get(i) <= 0.0) {
				System.out.println("negative Index is " + data.get(i));
				data.remove(i);
			}
		}
		for (int i = 0; i < data.size(); i++) {
			sum += data.get(i);
		}
		avg = (double) sum / data.size();
		return avg;

	}

	public void printLatencyToFile(List<Long> data) {
		for (int i = 0; i < data.size(); i++) {
			outFileToWorkLatency.println(data.get(i));
		}
		outFileToWorkLatency.println("End");
		outFileToWorkLatency.println();
		outFileToWorkLatency.close();
	}

	public void latnecyDistribution(List<Long> data) {
		List<Double> latencies = new ArrayList<Double>();
		int partitionSize = 20;

		for (int i = 0; i < data.size(); i++) {
			latencies.add((double) (((double) data.get(i))/1000000));
		}
//		for (int i = 0; i < 5; i++) {
//			System.out.println(data.get(i));
//			System.out.println((double) (((double) data.get(i))/1000000));
//		}
		outFile.println("Latency Size: "+latencies.size());

		final Multiset<Double> multiset = TreeMultiset.create(latencies);

		// create 10 partitions of entries
		// (each element value may appear multiple times in the multiset
		// but only once per partition)
		final Iterable<List<Double>> partitions = Iterables.partition(
				multiset.elementSet(),
				// other than aioobe, I create the partition size from
				// the number of unique entries, accounting for gaps in the list
				multiset.elementSet().size() / partitionSize);

		int partitionIndex = 0;
		List<Double> larageLatencies = new ArrayList<Double>();
		for (final List<Double> partition : partitions) {

			// count the items in this partition
			int count = 0;
			if(partitionIndex==partitionSize-1){
				int index = 0;
				while(index<partition.size()){
					larageLatencies.add(partition.get(index));
					index = index+(partition.size()/30);
				}
				for (int i=partition.size()-10;i<partition.size();i++){
					larageLatencies.add(partition.get(i));
				}
			}
			if(partitionIndex==partitionSize){
				for (int i=0;i<partition.size();i++){
					larageLatencies.add(partition.get(i));
				}
			}
			
			for (final Double item : partition) {
				count += multiset.count(item);
			}

//			System.out.println("Partition " + ++partitionIndex + " contains "
//					+ count + " items (" + partition.size() + " unique) from "
//					+ partition.get(0) + " to "
//					+ partition.get(partition.size() - 1));
			outFile.println("Partition " + ++partitionIndex + " contains "
					+ count + " latencies from "
					+ partition.get(0) + " to "
					+ partition.get(partition.size() - 1));
		}
		outFile.println("Check Large Latencies "+ larageLatencies);

	}

}