package org.jgroups.protocols.jzookeeperBackup;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Message;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.protocols.jzookeeper.MessageId;

/*
 * It uses to gathering protocol stats like throughput, latency and load.
 */
public class ProtocolStats {
	private List<Integer> latencies;
	private List<Integer> fromfollowerToLeaderF;
	private List<Integer> fromLeaderToFollowerP;
	private List<Integer> latencyPropForward;
	private Map<MessageId, Long> latencyProposalForwardST;
	private List<Integer> latencyProp;
	private Map<MessageId, Long> latencyProposalST;
//	private List<Integer> fromFollowerToLeaderA1;
//	private List<Integer> fromFollowerToLeaderA2;
//	private List<Integer> fromFollowerToLeaderF1;
//	private List<Integer> fromFollowerToLeaderF2;
    private int throughput;
    private long startThroughputTime;
    private long endThroughputTime;
    private String protocolName;
    private int numberOfSenderInEachClient;
    private int numberOfClients;
	private AtomicInteger numRequest;
    private AtomicInteger numReqDelivered;
    private AtomicInteger countMessageLeader;
    private AtomicInteger countTotalMessagesFollowers;
    private AtomicInteger countMessageFollower;
    private static PrintWriter outFile;
    private String outDir;
    private AtomicInteger countDummyCall;
    private boolean is_warmup = true;
    protected final Log        log=LogFactory.getLog(this.getClass());
   
    public ProtocolStats(){
    	
    }
  
	public ProtocolStats(String protocolName, int numberOfClients, int numberOfSenderInEachClient,
			String outDir, boolean stopWarmup) {

		this.latencies = new ArrayList<Integer>();
		this.fromfollowerToLeaderF = new ArrayList<Integer>();
		this.fromLeaderToFollowerP = new ArrayList<Integer>();
		this.latencyProp = new ArrayList<Integer>();;
		this.latencyProposalST= Collections
				.synchronizedMap(new HashMap<MessageId, Long>());
		this.latencyPropForward = new ArrayList<Integer>();;
		this.latencyProposalForwardST= Collections
				.synchronizedMap(new HashMap<MessageId, Long>());
//		this.fromFollowerToLeaderA1 = new ArrayList<Integer>();
//		this.fromFollowerToLeaderA2 = new ArrayList<Integer>();
//		this.fromFollowerToLeaderF1 = new ArrayList<Integer>();;
//		this.fromFollowerToLeaderF2 = new ArrayList<Integer>();;
		this.throughput = 0;
		this.protocolName = protocolName;
		this.numberOfClients = numberOfClients;
		this.numberOfSenderInEachClient = numberOfSenderInEachClient;
		this.numRequest = new AtomicInteger(0);
		this.numReqDelivered = new AtomicInteger(0);
		countMessageLeader = new AtomicInteger(0);
		countMessageFollower =  new AtomicInteger(0);;
	    countTotalMessagesFollowers = new AtomicInteger(0);
	    this.countDummyCall =  new AtomicInteger(0);
	    this.is_warmup = stopWarmup;
	    this.outDir = outDir;
		try {
			this.outFile = new PrintWriter(new BufferedWriter(new FileWriter(outDir
					+ InetAddress.getLocalHost().getHostName() + protocolName +".log", true)));
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	
	
	public List<Integer> getLatencies() {
		return latencies;
	}




	public void setLatencies(List<Integer> latencies) {
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




	public AtomicInteger getnumReqDelivered() {
		return numReqDelivered;
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



	public void incNumRequest(){
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

	public void addLatency(int latency){	
		latencies.add(latency);
	}
	
	public void addLatencyFToLF(int latency){	
		fromfollowerToLeaderF.add(latency);
	}
	
	public void addLatencyLToFP(int latency){	
		fromLeaderToFollowerP.add(latency);
	}
	
	public void addLatencyProp(int latency){	
		latencyProp.add(latency);
	}
	public void addLatencyProposalST(MessageId mid, Long st){	
		latencyProposalST.put(mid,st);
	}
	public Long getLatencyProposalST(MessageId mid){	
		return latencyProposalST.get(mid);
	}
	public void removeLatencyProposalST(MessageId mid){	
		latencyProposalST.remove(mid);
	}
	
	public void addLatencyPropForward(int latency){	
		latencyPropForward.add(latency);
	}
	public void addLatencyProposalForwardST(MessageId mid, Long st){	
		latencyProposalForwardST.put(mid,st);
	}
	public Long getLatencyProposalForwardST(MessageId mid){	
		return latencyProposalForwardST.get(mid);
	}
	public void removeLatencyProposalForwardST(MessageId mid){	
		latencyProposalForwardST.remove(mid);
	}
	
//	public void addLatencyFToLA2(int latency){	
//		fromFollowerToLeaderA2.add(latency);
//	}
//	
//	public void addLatencyFToLF1(int latency){	
//		fromFollowerToLeaderF1.add(latency);
//	}
//	public void addLatencyFToLF2(int latency){	
//		fromFollowerToLeaderF2.add(latency);
//	}
	
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
		long min = Long.MAX_VALUE, avg = 0, max = Long.MIN_VALUE, FToLFAvg=0, LToFPAvg=0, avgAll=0, latProp=0, latLeader=0;
		double latPropD=0, FToLFAvgD=0, LToFPAvgD=0, avgAllD=0, latLeaderD=0;
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
		avgAllD = (double) avg/latencies.size();
		
		if(!protocolName.equals("ZabCoinTossing")){
			if (isLeader){
				for (long lat : fromLeaderToFollowerP) {
					   LToFPAvg += lat;
				}
				LToFPAvgD = (double) LToFPAvg/fromLeaderToFollowerP.size();
				   
				 for (long lat : latencyProp) {
					   latProp += lat;
				 }
				   latPropD = (double) latProp/latencyProp.size();
				   
				 for (long lat : latencyPropForward) {
					   latLeader += lat;
				 }
				   latLeaderD = (double) latLeader/latencyPropForward.size();
			}
		   else{
			   for (long lat : fromfollowerToLeaderF) {
					lat += lat;
				}
			   FToLFAvgD = (double) FToLFAvg/fromfollowerToLeaderF.size();
			   	
		   }
		}
		
		outFile.println("Start");
		outFile.println(protocolName + "/" + numberOfClients + "/" + numberOfSenderInEachClient);
		outFile.println("Number of Request Recieved = " + (numRequest));
		outFile.println("Number of Request Deliever = " + numReqDelivered);
		outFile.println("Total Load generates by Atomic broadcast = "
				+ (countMessageLeader.get() + countTotalMessagesFollowers.get()));
		outFile.println("Throughput = "
				+ (numReqDelivered.get() / (TimeUnit.MILLISECONDS
						.toSeconds(endThroughputTime - startThroughputTime))));
		outFile.println("Latency average" + latAvg
				+ " numbers avg = " + latAvg.size());
		outFile.println("All Latency average " +  (avgAllD)/1000000);
		if(!protocolName.equals("ZabCoinTossing")){
			if (isLeader){
				outFile.println("Latency From Leader to Follower (round-trip) (Proposal) " + (double)(LToFPAvgD)/1000000);
				outFile.println("Latency From FORWARD case to Proposal sent (Proposal) " + "latencyProp Size " + 
						latencyProp.size() + " " +(double)(latPropD)/1000000);
				outFile.println("Latency From leader to Proposal sent (Proposal) " + "latencyPropForward Size " + 
						latencyPropForward.size() + " " +(double)(latLeaderD)/1000000);
			}
			else{
				outFile.println("Latency From Folower to Leader (round-trip) (Forward) " + (double)(FToLFAvgD)/1000000);
	
			}
		}
		else{
			outFile.println("Number of call Dummy " + countDummyCall.get());

		}
		
		outFile.println("Test Generated at "
				+ new Date()
				+ " /Lasted for = "
				+ TimeUnit.MILLISECONDS
						.toSeconds((endThroughputTime - startThroughputTime)));
		outFile.println("End");
		outFile.println();

		outFile.close();

	}
    
    

}
