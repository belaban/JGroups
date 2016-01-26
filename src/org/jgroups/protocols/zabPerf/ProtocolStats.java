package org.jgroups.protocols.zabPerf;

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

/*
 * It uses to gathering protocol stats like throughput, latency and load.
 */
public class ProtocolStats {
	private List<Integer> latencies;
	private List<Integer> oneRoundFToL;
	private List<Integer> fromfollowerToLeaderF;
	private List<Integer> fromLeaderToFollowerP;
	private Map<MessageId, Long> tempPPTL;
	private Map<MessageId, Long> tempPPTFForward;
	private List<Integer> PPTL;
	private List<Integer> PPTFF;
	private List<Integer> ackProcessTime;
	private  Map<MessageId, Long>  tempackProcessTime;
	private List<Integer> fromFToLOneR; //point1 (Follower) catch st and point2 (Leader) catch et not reliable
	private  Map<MessageId, Long>  tempCommitPTL;
	private List<Integer>  commitPTL;
	private  Map<MessageId, Long>  deliveryProcessTimeFL;
	private List<Integer>  deliveryPTime;
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
		this.oneRoundFToL = new ArrayList<Integer>();
		this.fromfollowerToLeaderF = new ArrayList<Integer>();
		this.fromLeaderToFollowerP = new ArrayList<Integer>();
		this.PPTL = new ArrayList<Integer>();
		this.PPTFF = new ArrayList<Integer>();
		this.ackProcessTime = new ArrayList<Integer>();
		this.fromFToLOneR = new ArrayList<Integer>();
		this.tempPPTL= Collections
				.synchronizedMap(new HashMap<MessageId, Long>());
		this.tempPPTFForward= Collections
				.synchronizedMap(new HashMap<MessageId, Long>());
		this.tempackProcessTime =  Collections.synchronizedMap(new HashMap<MessageId, Long>());
		this.ackProcessTime = new ArrayList<Integer>();
		this.tempCommitPTL =  Collections.synchronizedMap(new HashMap<MessageId, Long>());
		this.commitPTL = new ArrayList<Integer>();
		this.deliveryProcessTimeFL =  Collections.synchronizedMap(new HashMap<MessageId, Long>());
		this.deliveryPTime = new ArrayList<Integer>();
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
	
	public void addOneRoundFToL(int latency){	
		oneRoundFToL.add(latency);
	}
	
	public void addLatencyFToLFOneRound(int latency){	
		fromFToLOneR.add(latency);
	}
	public List<Integer> getListFToLFOneRound(){	
		return fromFToLOneR;
	}
	
	public void addLatencyFToLF(int latency){	
		fromfollowerToLeaderF.add(latency);
	}
	
	public void addAckPT(int latency){	
		ackProcessTime.add(latency);
	} 
	
	public Long getTempAckProcessTime(MessageId mid){	
		return tempackProcessTime.get(mid);
	}
	public void addTempAckProcessTime(MessageId mid, long latency){	
		tempackProcessTime.put(mid, latency);
	}
	
	public void removeTempAckProcessTime(MessageId mid){	
		tempackProcessTime.remove(mid);
	}
	
	public void addLatencyLToFP(int latency){	
		fromLeaderToFollowerP.add(latency);
	}
	
	public void addPPTL(int latency){	
		PPTL.add(latency);
	}
	public void addtempPPTL(MessageId mid, Long st){	
		tempPPTL.put(mid,st);
	}
	public Long getTempPPTL(MessageId mid){	
		return tempPPTL.get(mid);
	}
	public void removeTempPPTL(MessageId mid){	
		tempPPTL.remove(mid);
	}
	
	public void addPPTFF(int latency){	
		PPTFF.add(latency);
	}
	public void addTempPPTForward(MessageId mid, Long st){	
		tempPPTFForward.put(mid,st);
	}
	public Long getTempPPTForward(MessageId mid){	
		return tempPPTFForward.get(mid);
	}
	public void removeTempPPTForward(MessageId mid){	
		tempPPTFForward.remove(mid);
	}
	
	public void addCommitProcessTime(MessageId mid, long startTime){	
		tempCommitPTL.put(mid, startTime);
	}
	public Long getCommitProcessTime(MessageId mid){	
		return tempCommitPTL.get(mid);
	}
	public void removeCommitProcessTime(MessageId mid){	
		tempCommitPTL.remove(mid);
	}
	
	public void addCommitPTL(int commitTime){	
		commitPTL.add(commitTime);
	}

	public void addDeliveryProcessTime(MessageId mid, Long st){	
		deliveryProcessTimeFL.put(mid,st);
	}
	public Long getDeliveryProcessTime(MessageId mid){	
		return deliveryProcessTimeFL.get(mid);
	}
	public void removeDeliveryProcessTime(MessageId mid){	
		deliveryProcessTimeFL.remove(mid);
	}
	
	public void addDeliveryPT(int deliveryTime){	
		deliveryPTime.add(deliveryTime);
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
		long min = Long.MAX_VALUE, avg = 0, max = Long.MIN_VALUE, FToLFAvg=0, LToFPAvg=0, avgAll=0, latProp=0, PPTTemp = 0, FToL1RoundF=0, ackPTAvg=0;
		double latPropD=0, FToLFAvgD=0, LToFPAvgD=0, avgAllD=0, latLeaderD=0, PPTLAvg=0, FToLOneRoundF=0, commitPT=0, deliveryPT=0, ackPTAvgD=0, FtOLOneR=0;
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
		
		if (!protocolName.equals("ZabCoinTossing")) {
			if (isLeader) {
//				for (long lat : fromLeaderToFollowerP) {
//					LToFPAvg += lat;
//				}
//				LToFPAvgD = (double) LToFPAvg / fromLeaderToFollowerP.size();
				LToFPAvgD = average(fromLeaderToFollowerP);
				
				log.info(" oneRoundFToL size = "+ oneRoundFToL.size());
				List<Integer> noneRoundFToL = new ArrayList<Integer>();
				noneRoundFToL.addAll(oneRoundFToL);
				FtOLOneR = average(noneRoundFToL);
				PPTFF.addAll(PPTL);
//				for (long lat : PPTFF) {
//					PPTTemp += lat;
//				}
				//PPTLAvg = (double) PPTTemp / PPTFF.size();
				PPTLAvg = average(PPTFF);
				log.info(" fromFToLOneR size = "+ fromFToLOneR.size());
				//log.info(" Show all numbers "+ fromFToLOneR);
//				for (long lat : fromFToLOneR) {
//					FToL1RoundF += lat;
//				}
 				//FToLOneRoundF = (double) FToL1RoundF/fromFToLOneR.size();
				//FToLOneRoundF = average(fromFToLOneR);
//				for (int i=0; i<fromFToLOneR.size();i++){
//					FToL1RoundF += fromFToLOneR.get(i);
//				}
//				FToLOneRoundF = (double) FToL1RoundF
//						/ fromFToLOneR.size();
				
//				for (long lat :commitPTL) {
//					commitPT += lat;
//				}
//				commitPT = (double) commitPT/commitPTL.size();
				commitPT = average(commitPTL);

				
			} else {
//				for (long lat : fromfollowerToLeaderF) {
//					lat += lat;
//				}
//				FToLFAvgD = (double) FToLFAvg / fromfollowerToLeaderF.size();
				FToLFAvgD=average(fromfollowerToLeaderF);
//				for (long lat : ackProcessTime) {
//					   ackPTAvg += lat;
//					}
//				ackPTAvgD = (double) ackPTAvg/ackProcessTime.size();
				ackPTAvgD = average(ackProcessTime);

			}
//			for (long lat :deliveryPTime) {
//				deliveryPT += lat;
//			}
//			deliveryPT = (double) deliveryPT/deliveryPTime.size();
			deliveryPT = average(deliveryPTime);
		}
		
		outFile.println("Start");
		outFile.println("Test For Saed");
		if(isLeader)
			outFile.println("Leader");
		else
			outFile.println("Follower");
		
		outFile.println("protocolName/numberOfClients/numberOfSenderInEachClient: "+protocolName + "/" + numberOfClients + "/" + numberOfSenderInEachClient);
		outFile.println("NRR: " + (numRequest));
		outFile.println("NRD: " + numReqDelivered);
		if(isLeader){
			outFile.println("TLAB: "
				+ (countMessageLeader.get() + countTotalMessagesFollowers.get()));
		}
		outFile.println("Throughput: "
				+ (numReqDelivered.get() / (TimeUnit.MILLISECONDS
						.toSeconds(endThroughputTime - startThroughputTime))));
		outFile.println("Latency Sample: " + latAvg);
		outFile.println("Latency Average: " +  (avgAllD)/1000000);
		if(!protocolName.equals("ZabCoinTossing")){
			if (isLeader){
				//outFile.println("Latency From Leader to Follower (round-trip) (Proposal) " + (double)(LToFPAvgD)/1000000);
				outFile.println("PPT: "+(double)(PPTLAvg)/1000000);
				outFile.println("Latency From FToL: " +(double)(FtOLOneR)/1000000);
				outFile.println("CPTL: " +(double)(commitPT)/1000000);
			}
			else{
				outFile.println("Latency From Folower to Leader (round-trip) (Forward) " + (double)(FToLFAvgD)/1000000);
				outFile.println("APT: " + (double)(ackPTAvgD)/1000000);
			}
			outFile.println("DPT: " +(double)(deliveryPT)/1000000);

		}
		else{
			outFile.println("Number of call Dummy " + countDummyCall.get());

		}
		
		outFile.println("Test Generated at "
				+ new Date());
		outFile.println("End");
		outFile.println();

		outFile.close();

	}
	
	public double average(List<Integer> data){
		log.info("Data size "+ data.size());
		long sum=0;
		double avg=0;
		for (long lat :data) {
			sum += lat;
		}
		avg = (double) sum/data.size();
		return avg;
		
	}
    
    

}
