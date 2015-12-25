package org.jgroups.protocols.jzookeeper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;

/*
 * It uses to gathering protocol stats like throughput, latency and load.
 */
public class ProtocolStatsForTest {
	private List<Integer> latencies;
	private List<Integer> fromfollowerToLeaderF;
	private List<Integer> fromLeaderToFollowerP;
	private List<Integer> fromFollowerToLeaderA1;
	private List<Integer> fromFollowerToLeaderA2;
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
    protected final Log        log=LogFactory.getLog(this.getClass());

  
	public ProtocolStatsForTest(String protocolName, int numberOfClients, int numberOfSenderInEachClient, String outDir) {

		this.latencies = new ArrayList<Integer>();
		this.fromfollowerToLeaderF = new ArrayList<Integer>();
		this.fromLeaderToFollowerP = new ArrayList<Integer>();;
		this.fromFollowerToLeaderA1 = new ArrayList<Integer>();;
		this.fromFollowerToLeaderA2 = new ArrayList<Integer>();;
		this.throughput = 0;
		this.protocolName = protocolName;
		this.numberOfClients = numberOfClients;
		this.numberOfSenderInEachClient = numberOfSenderInEachClient;
		this.numRequest = new AtomicInteger(0);
		this.numReqDelivered = new AtomicInteger(0);
		countMessageLeader = new AtomicInteger(0);
		countMessageFollower =  new AtomicInteger(0);;
	    countTotalMessagesFollowers = new AtomicInteger(0);
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

	public void addLatency(int latency){	
		latencies.add(latency);
	}
	
	public void addLatencyFToLF(int latency){	
		fromfollowerToLeaderF.add(latency);
	}
	
	public void addLatencyLToFP(int latency){	
		fromLeaderToFollowerP.add(latency);
	}
	
	public void addLatencyFToLA1(int latency){	
		fromFollowerToLeaderA1.add(latency);
	}
	public void addLatencyFToLA2(int latency){	
		fromFollowerToLeaderA2.add(latency);
	}
	
	public void incCountMessageFollower() {
		this.countMessageFollower.incrementAndGet();
	}
	
	public void incCountMessageLeader() {
		this.countMessageLeader.incrementAndGet();
	}


	public void printProtocolStats() {
		
		// print Min, Avg, and Max latency
		List<Long> latAvg = new ArrayList<Long>();
		int count = 0;
		long avgTemp = 0;
		long min = Long.MAX_VALUE, avg = 0, max = Long.MIN_VALUE;
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
