package org.jgroups.protocols.jzookeeper;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.Address;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.annotations.ManagedAttribute;
import org.jgroups.annotations.ManagedOperation;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;

public class CopyZab extends Protocol {
	
	private final AtomicLong                  zxid=new AtomicLong(0);
    private ExecutorService                     executor;
    private Address                           local_addr;
    private volatile Address                  leader;
    private int QUEUE_CAPACITY = 500;
    private volatile View                     view;
    private volatile boolean                  is_leader=false;
    private List<Address>                       zabMembers = Collections.synchronizedList(new ArrayList<Address>());
	private AtomicLong lastZxidProposed=new AtomicLong(0);
	private AtomicLong lastZxidCommitted=new AtomicLong(0);
    private final Set<MessageId>                requestQueue =Collections.synchronizedSet(new HashSet<MessageId>());
	private Map<Long, ZabHeader> queuedCommitMessage = new HashMap<Long, ZabHeader>();
	private List<Integer> latencies = new ArrayList<Integer>();
	private List<Integer> avgLatencies = new ArrayList<Integer>();
	private List<String> avgLatenciesTimer = new ArrayList<String>();
	private List<String> largeLatencies = new ArrayList<String>();

    private final Map<Long, ZabHeader> queuedProposalMessage = Collections.synchronizedMap(new HashMap<Long, ZabHeader>());
    private final LinkedBlockingQueue<ZabHeader> queuedMessages =
	        new LinkedBlockingQueue<ZabHeader>();
	private ConcurrentMap<Long, Proposal> outstandingProposals = new ConcurrentHashMap<Long, Proposal>();
    private final Map<MessageId, Message> messageStore = Collections.synchronizedMap(new HashMap<MessageId, Message>());
    private Calendar cal = Calendar.getInstance();
    private int index=-1;
    private volatile boolean                  running=true;
    private long startThroughputTime = 0;
    private long endThroughputTime = 0;
    private volatile boolean startThroughput = false;
    private static PrintWriter outFile;
    private final static String outDir="/home/pg/p13/a6915654/ZAB/";
	private  AtomicInteger                  numReqDeviverd=new AtomicInteger(0);
	private  AtomicInteger                  numRequest=new AtomicInteger(0);
	private long rateInterval = 10000;
	private long rateCount = 0;
	private long currentCpuTime = 0, rateCountTime = 0, lastTimer = 0, lastCpuTime = 0;
	private int lastArrayIndex = 0, lastArrayIndexUsingTime = 0 ;
	private long timeInterval = 200;
	private int lastFinished = 0;
	private Timer timer;
	private AtomicLong countMessageLeader = new AtomicLong(0);
	private long countMessageFollower = 0;
	private long countTotalMessagesFollowers = 0;
	private AtomicLong warmUpRequest = new AtomicLong(0);
	private static long warmUp = 10000;
    private int largeLatCount = 0;

    public CopyZab(){
    	
    }
    
    @ManagedAttribute
    public boolean isleaderinator() {return is_leader;}
    public Address getleaderinator() {return leader;}
    public Address getLocalAddress() {return local_addr;}

    @ManagedOperation
    public String printStats() {
        return dumpStats().toString();
    }

    @Override
    public void start() throws Exception {
        super.start();
        running=true;    
	    executor = Executors.newSingleThreadExecutor();
	    executor.execute(new FollowerMessageHandler(this.id));
	    log.setLevel("trace");
	    this.outFile = new PrintWriter(new BufferedWriter(new FileWriter
				(outDir+InetAddress.getLocalHost().getHostName()+"Zab.log",true)));
	    
    }


    public void reset() {
    	zxid.set(0);
    	lastZxidProposed=new AtomicLong(0);
    	lastZxidCommitted=new AtomicLong(0);
        requestQueue.clear();
    	queuedCommitMessage.clear();
        queuedProposalMessage.clear(); 
        queuedMessages.clear();
        outstandingProposals.clear();
        messageStore.clear();
        latencies.clear();
        avgLatencies.clear();
        avgLatenciesTimer.clear();
        numReqDeviverd= new AtomicInteger(0);
        numRequest= new AtomicInteger(0);
        startThroughputTime = 0;
        endThroughputTime = 0;
        startThroughput = false;
        currentCpuTime = 0;
        rateCountTime = 0;
        lastTimer = 0;
        lastCpuTime = 0;
    	lastArrayIndex = 0;
    	lastArrayIndexUsingTime = 0 ;
    	lastFinished = 0;
    	countMessageLeader = new AtomicLong(0);
	    
    }
    @Override
    public void stop() {
        running=false;
        executor.shutdown();
        super.stop();
    }

    public Object down(Event evt) {
        switch(evt.getType()) {
            case Event.MSG:
            	Message m = (Message) evt.getArg();
                handleClientRequest(m);
                return null; // don't pass down
            case Event.SET_LOCAL_ADDRESS:
                local_addr=(Address)evt.getArg();
                break;
        }
        return down_prot.down(evt);
    }

    public Object up(Event evt) {
        Message msg = null;
        ZabHeader hdr;

        switch(evt.getType()) {
            case Event.MSG:
                msg=(Message)evt.getArg();
                hdr=(ZabHeader)msg.getHeader(this.id);
                if(hdr == null){
                    break; // pass up
                }
                switch(hdr.getType()) {                
                   case ZabHeader.START_SENDING:
	                    return up_prot.up(new Event(Event.MSG, msg));
                	case ZabHeader.REQUEST:        
//                		
	                		forwardToLeader(msg);                		
                		break;
                	case ZabHeader.RESET:
  	                    reset();
  	                	break;
                    case ZabHeader.FORWARD:   
//                    	
                   		queuedMessages.add(hdr);

                		
                		break;
                    case ZabHeader.PROPOSAL:
                    	//warmUpRequest.incrementAndGet();
                   	 	if(lastZxidCommitted.get() >= warmUp && !startThroughput && !is_leader){
        	            	startThroughput = true;
        	            	startThroughputTime = System.currentTimeMillis();
        	            	timer = new Timer();
        	            	timer.schedule(new ResubmitTimer(), timeInterval, timeInterval);
                   	 	}
                   	 
	                   	if (!is_leader){	                   		
		                   	hdr.getMessageId().setStartTime(System.currentTimeMillis());
		            		sendACK(msg);
	            		}
	                   	break;           		
                    case ZabHeader.ACK:
                		if (is_leader){
                			processACK(msg, msg.getSrc());
                		}
                		break;
                    case ZabHeader.COMMIT:
                		 deliver(msg);
                		 break;
                    case ZabHeader.STATS:
                    	//if(is_leader)
                    		printMZabStats();
                    	break;
                    case ZabHeader.COUNTMESSAGE:
                    		sendTotalABMessages(hdr);  
                    		log.info("Yes, I recieved count request");
                    	break;
                    case ZabHeader.RESPONSE:
                    	handleOrderingResponse(hdr);
                    	
            }                
                return null;
              case Event.VIEW_CHANGE:
              handleViewChange((View)evt.getArg());
              break;

        }

        return up_prot.up(evt);
    }

    public void up(MessageBatch batch) {
        for(Message msg: batch) {
            if(msg.isFlagSet(Message.Flag.NO_TOTAL_ORDER) || msg.isFlagSet(Message.Flag.OOB) || msg.getHeader(id) == null)
                continue;
            batch.remove(msg);

            try {
                up(new Event(Event.MSG, msg));
            }
            catch(Throwable t) {
                log.error("failed passing up message", t);
            }
        }

        if(!batch.isEmpty())
            up_prot.up(batch);
    }

    /* --------------------------------- Private Methods ----------------------------------- */

    
	private void handleClientRequest(Message message) {
		ZabHeader clientHeader = ((ZabHeader) message.getHeader(this.id));

		if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.START_SENDING) {
			for (Address client : view.getMembers()) {
				if (log.isDebugEnabled())
					log.info("Address to check " + client);
				if (!zabMembers.contains(client)) {
					if (log.isDebugEnabled())
						log.info("Address to check is not zab Members, will send start request to"
								+ client + " " + getCurrentTimeStamp());
					message.setDest(client);
					down_prot.down(new Event(Event.MSG, message));
				}
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.RESET) {

			for (Address server : zabMembers) {
				// message.setDest(server);
				Message resetMessage = new Message(server).putHeader(this.id,
						clientHeader);
				down_prot.down(new Event(Event.MSG, resetMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.STATS) {

			for (Address server : zabMembers) {
				// message.setDest(server);
				Message statsMessage = new Message(server).putHeader(this.id,
						clientHeader);
				down_prot.down(new Event(Event.MSG, statsMessage));
			}
		}

		else if (clientHeader != null
				&& clientHeader.getType() == ZabHeader.COUNTMESSAGE) {
			for (Address server : zabMembers) {
				if(!server.equals(zabMembers.get(0))){
					Message countMessages = new Message(server).putHeader(this.id,
							clientHeader);
					down_prot.down(new Event(Event.MSG, countMessages));
				}
			}
			
		}

		else if (!clientHeader.getMessageId().equals(null)
				&& clientHeader.getType() == ZabHeader.REQUEST) {
			Address destination = null;
			messageStore.put(clientHeader.getMessageId(), message);
			ZabHeader hdrReq = new ZabHeader(ZabHeader.REQUEST,
					clientHeader.getMessageId());
			++index;
			if (index > 2)
				index = 0;
			destination = zabMembers.get(index);
			Message requestMessage = new Message(destination).putHeader(
					this.id, hdrReq);
			down_prot.down(new Event(Event.MSG, requestMessage));
		}

	}
    
    private void handleViewChange(View v) {
        List<Address> mbrs=v.getMembers();
        leader=mbrs.get(0);
        if (leader.equals(local_addr)){
        	is_leader = true;
        }
        if (mbrs.size() == 3){
        	zabMembers.addAll(v.getMembers());        	
        }
        if (mbrs.size() > 3 && zabMembers.isEmpty()){
        	for (int i = 0; i < 3; i++) {
        		zabMembers.add(mbrs.get(i));
			}

        }
        if(mbrs.isEmpty()) return;

        if(view == null || view.compareTo(v) < 0)
            view=v;
        else
            return;
    }

    private long getNewZxid(){
    	return zxid.incrementAndGet();
    }

    private void forwardToLeader(Message msg) {
    	 if(is_leader && lastZxidCommitted.get() > warmUp && !startThroughput){
         	startThroughput = true;
         	startThroughputTime = System.currentTimeMillis();
         	timer = new Timer();
         	timer.schedule(new ResubmitTimer(), timeInterval, timeInterval);
         }
	   ZabHeader hdrReq = (ZabHeader) msg.getHeader(this.id);
	   requestQueue.add(hdrReq.getMessageId());
	   if (is_leader){
		  queuedMessages.add((ZabHeader)msg.getHeader(this.id));
       }	   
	   else{
		   forward(msg);
	   }
            
           
   }

    private void forward(Message msg) {
        Address target=leader;
 	    ZabHeader hdrReq = (ZabHeader) msg.getHeader(this.id);
        if(target == null)
            return;
 
	    try {
	        ZabHeader hdr=new ZabHeader(ZabHeader.FORWARD, hdrReq.getMessageId());
	        Message forward_msg=new Message(target).putHeader(this.id,hdr);
	        down_prot.down(new Event(Event.MSG, forward_msg));
	     }
	    catch(Exception ex) {
	      log.error("failed forwarding message to " + msg, ex);
	    }
      
    }
    
    private void sendACK(Message msg){
   		numRequest.incrementAndGet();

    	if (msg == null )
    		return;
    	
    	ZabHeader hdr = (ZabHeader) msg.getHeader(this.id);
    	if (hdr == null)
    		return;
    	
    	//if (hdr.getZxid() != lastZxidProposed + 1){
//            log.info("Got zxid 0x"
//                    + Long.toHexString(hdr.getZxid())
//                    + " expected 0x"
//                    + Long.toHexString(lastZxidProposed + 1));
        //}
    	
    	lastZxidProposed.set(hdr.getZxid());
    	queuedProposalMessage.put(hdr.getZxid(), hdr);
		ZabHeader hdrACK = new ZabHeader(ZabHeader.ACK, hdr.getZxid(), hdr.getMessageId());
		Message ACKMessage = new Message(leader).putHeader(this.id, hdrACK);
		if(lastZxidCommitted.get() > warmUp)
			countMessageFollower++;
		try{
    		down_prot.down(new Event(Event.MSG, ACKMessage));   
         }catch(Exception ex) {
    		log.error("failed sending ACK message to Leader");
    	} 
		
		
    }
    
    
    private synchronized void processACK(Message msgACK, Address sender){
	   
    	ZabHeader hdr = (ZabHeader) msgACK.getHeader(this.id);	
    	long ackZxid = hdr.getZxid();
		if (lastZxidCommitted.get() >= ackZxid) {
            return;
        }
        Proposal p = outstandingProposals.get(ackZxid);
        if (p == null) {
            return;
        }		
		p.AckCount++;
		//if (log.isDebugEnabled()) {
//            log.debug("Count for zxid: " +
//                    Long.toHexString(ackZxid)+" = "+ p.getAckCount());
        //}
		
		if(isQuorum(p.getAckCount())){ 	
            outstandingProposals.remove(ackZxid);
            commit(ackZxid);	
		}
			
			
		}
		
    private void commit(long zxidd){
	    ZabHeader hdrOrg = queuedProposalMessage.get(zxidd);
   	    //log.info("Czxid = "+ hdrOrg.getZxid() + " " + getCurrentTimeStamp());
	       

		   if (hdrOrg == null){
			   log.info("??????????????????????????? Header is null (commit)"+ hdrOrg + " for zxid "+zxidd);
			   return;
		   }

	       ZabHeader hdrCommit = new ZabHeader(ZabHeader.COMMIT, zxidd);
	       Message commitMessage = new Message().putHeader(this.id, hdrCommit);	       
           for (Address address : zabMembers) {
        	   if(lastZxidCommitted.get()>warmUp)
              	  countMessageLeader.incrementAndGet();
        	  //if(!address.equals(leader))
        		  
              Message cpy = commitMessage.copy();
              cpy.setDest(address);
   		      //log.info("[" + local_addr + "] "+ "YYYYYYYY sending comit message zxid to = "+zxidd+":"+address);
              down_prot.down(new Event(Event.MSG, cpy));   
              
           }

	    }
		
    private void deliver(Message toDeliver){
		//ZABHeader hdrOrginal = null;
    	
		ZabHeader hdr = (ZabHeader) toDeliver.getHeader(this.id);
		long zxid = hdr.getZxid();
		synchronized(this){
	    	   lastZxidCommitted.set(zxid);
	       }
//		log.info("[" + local_addr + "] "
//				+ " recieved commit message (deliver) for zxid="
//				+ hdr.getZxid() + " " + getCurrentTimeStamp());
		//log.info("Dzxid = "+ hdr.getZxid() + " " + getCurrentTimeStamp());
		ZabHeader hdrOrginal = queuedProposalMessage.remove(zxid);
		if (hdrOrginal == null) {
			 if (log.isInfoEnabled())
				 log.info("$$$$$$$$$$$$$$$$$$$$$ Header is null (deliver)"
					+ hdrOrginal + " for zxid " + hdr.getZxid());
			return;
		}
//		log.info("!!!!!!!!!!!! check if (requestQueue.contains(hdrOrginal.getMessageId() (deliver)"
//				+ requestQueue.contains(hdrOrginal.getMessageId()));

		queuedCommitMessage.put(zxid, hdrOrginal);
		//synchronized(latencies){
			if(lastZxidCommitted.get()>warmUp){
				
				numReqDeviverd.incrementAndGet();
				endThroughputTime = System.currentTimeMillis();
				long startTime  = hdrOrginal.getMessageId().getStartTime();
				latencies.add((int)(System.currentTimeMillis() - startTime));
				rateCount++;
	//		if (rateCount == rateInterval){
	//			StatsThread st = new StatsThread();
	//			st.start();
	//			rateCount=0;
	//		}
				if (numReqDeviverd.get()>=1000000)
					timer.cancel();
			}
		//}
		
		if (log.isInfoEnabled())
			log.info("queuedCommitMessage size = " + queuedCommitMessage.size() + " zxid "+zxid);
		if (requestQueue.contains(hdrOrginal.getMessageId())) {
			
			//log.info("I am the zab request receiver, "+ hdr.getZxid());
					//+ hdrOrginal.getMessageId().getAddress());
			ZabHeader hdrResponse = new ZabHeader(ZabHeader.RESPONSE, hdr.getZxid(),
					hdrOrginal.getMessageId());
			Message msgResponse = new Message(hdrOrginal.getMessageId()
					.getOriginator()).putHeader(this.id, hdrResponse);
			down_prot.down(new Event(Event.MSG, msgResponse));

		}

	}
		
		
    private void handleOrderingResponse(ZabHeader hdrResponse) {
	        Message message = messageStore.get(hdrResponse.getMessageId());
	        message.putHeader(this.id, hdrResponse);
	        up_prot.up(new Event(Event.MSG, message));

	    }
	    	
		private boolean isQuorum(int majority){
	    	return majority >= ((zabMembers.size()/2) + 1)? true : false;
	    }
		
		private String getCurrentTimeStamp(){
			long timestamp = new Date().getTime();  
			cal.setTimeInMillis(timestamp);
			String timeString =
				   new SimpleDateFormat("HH:mm:ss:SSS").format(cal.getTime());

			return timeString;
		}
		
		private void sendTotalABMessages(ZabHeader CarryCountMessageLeader){
			if(!is_leader){
			    ZabHeader followerMsgCount = new ZabHeader(ZabHeader.COUNTMESSAGE, countMessageFollower);
		   	    Message requestMessage = new Message(leader).putHeader(this.id, followerMsgCount);
		        down_prot.down(new Event(Event.MSG, requestMessage)); 
			}
			else{
				long followerMsg = CarryCountMessageLeader.getZxid();
				countTotalMessagesFollowers += followerMsg;
			}
				
		}
		
		
		private void printMZabStats(){	
			// print Min, Avg, and Max latency
			long min = Long.MAX_VALUE, avg =0, max = Long.MIN_VALUE;
			for (long lat : latencies){
				if (lat < min){
					min = lat;
				}
				if (lat > max){
					max = lat;
				}
				avg+=lat;	
				
			}

			outFile.println("Number of Request Recieved = "+(numRequest.get()-10000));
			outFile.println("Number of Request Deliever = " + numReqDeviverd);
			outFile.println("Total ZAB Messages = " + (countMessageLeader.get() + countTotalMessagesFollowers));
			outFile.println("Throughput = " + (numReqDeviverd.get()/(TimeUnit.MILLISECONDS.toSeconds(endThroughputTime-startThroughputTime))));
			outFile.println("Large Latencies count " + largeLatCount);	
			outFile.println("Large Latencies " + largeLatencies);	
			outFile.println("Latency /Min= " + min + " /Avg= "+ (avg/latencies.size())+
			        " /Max= " +max);	
			outFile.println("Latency average rate with interval 100000 = " + 
			        avgLatencies);
			outFile.println("Latency average rate with interval 200 MillSec = " + 
			        avgLatenciesTimer);
			
					
				
			 
			Collections.sort(latencies);
			
			int x50=0, x100=0, x150=0, x200=0, x250=0, x300=0, x350=0,x400=0, x450=0, x500=0, x550=0, x600=0, x650=0, x700=0, xLager700=0;
			
			for (Integer l:latencies){
				if (l<51)
					x50++;
				else if(l>50 && l<101)
					x100++;
				else if(l>100 && l<151)
					x150++;
				else if(l>150 && l<201)
					x200++;
				else if(l>200 && l<251)
					x250++;
				else if(l>250 && l<301)
					x300++;
				else if(l>300 && l<351)
					x350++;
				else if(l>350 && l<401)
					x400++;
				else if(l>400 && l<451)
					x450++;
				else if(l>450 && l<501)
					x500++;
				else if(l>500 && l<551)
					x550++;
				else if(l>560 && l<601)
					x600++;
				else if(l>600 && l<651)
					x650++;
				else if(l>650 && l<701)
					x700++;
				else
					xLager700++;
			}
				outFile.println("Distribution contains latencies form (0-50) " + x50);
			    outFile.println("Distribution contains latencies form (51-100) " + x100);
			    outFile.println("Distribution contains latencies form (101-150) " + x150);
			    outFile.println("Distribution contains latencies form (151-200) " + x200);
			    outFile.println("Distribution contains latencies form (201-250) " + x250);
			    outFile.println("Distribution contains latencies form (251-300) " + x300);
			    outFile.println("Distribution contains latencies form (301-350) " + x350);
			    outFile.println("Distribution contains latencies form (351-400) " + x400);
			    outFile.println("Distribution contains latencies form (401-450) " + x450);
			    outFile.println("Distribution contains latencies form (451-500) " + x500);
			    outFile.println("Distribution contains latencies form (501-550) " + x550);
			    outFile.println("Distribution contains latencies form (551-600) " + x600);
			    outFile.println("Distribution contains latencies form (601-650) " + x650);
			    outFile.println("Distribution contains latencies form (651-700) " + x700);
			    outFile.println("Distribution contains latencies form (  > 700) " + xLager700);
		
			
//			final int groupRange = 50;
//
//			ListMultimap<Integer, Integer> map = Multimaps.index(latencies, new Function<Integer, Integer>() {
//			    public Integer apply(Integer i) {
//			        //work out which group the value belongs in
//			        return (i / groupRange) + (i % groupRange == 0 ? 0 : 1);
//			    }
//			});
//			for (Integer key : map.keySet()) {
//			    List<Integer> value = map.get(key);
//			    outFile.println("Distribution contains " + value.size() + " items from " + value.get(0) + " to " + value.get(value.size() - 1));
//			}
			
			    outFile.println("Test Generated at "+ new Date() + " /Lasted for = "
					 	+ TimeUnit.MILLISECONDS.toSeconds((endThroughputTime-startThroughputTime)));	
			 outFile.println();	

			outFile.close();	
		
		}
		
/* ----------------------------- End of Private Methods -------------------------------- */
   

 final class FollowerMessageHandler implements Runnable {
    	
    	private short id;
    	public FollowerMessageHandler(short id){
    		this.id = id;
    	}
    	
    	/**
         * create a proposal and send it out to all the members
         * 
         * @param message
         */
    	@Override
        public void run() {
                handleRequests();         
        }
  
        public void handleRequests() {
        	ZabHeader hdrReq = null;
            while (running) {
            	
                	 try {
                		 hdrReq = queuedMessages.take();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
	            numRequest.incrementAndGet();
	           
            	long new_zxid = getNewZxid();
            	ZabHeader hdrProposal = new ZabHeader(ZabHeader.PROPOSAL, new_zxid, hdrReq.getMessageId());                
                Message ProposalMessage=new Message().putHeader(this.id, hdrProposal);

                ProposalMessage.setSrc(local_addr);
            	Proposal p = new Proposal();
            	p.setMessageId(hdrReq.getMessageId());
            	p.AckCount++;            	            	
            	outstandingProposals.put(new_zxid, p);
            	hdrProposal.getMessageId().setStartTime(System.currentTimeMillis());
            	queuedProposalMessage.put(new_zxid, hdrProposal);
            	
            	          	
            	try{
       
                 	for (Address address : zabMembers) {
                        if(address.equals(leader))
                        	continue;
                        if(lastZxidCommitted.get()>warmUp)
                        	countMessageLeader.incrementAndGet();
                        Message cpy = ProposalMessage.copy();
                        cpy.setDest(address);
                		down_prot.down(new Event(Event.MSG, cpy));     
                    }
                 }catch(Exception ex) {
            		log.error("failed proposing message to members");
            	}    
            	
            }
            
        }

       
    }


	 public class StatsThread extends Thread{
		 public void run(){
			 int avg = 0, elementCount = 0;
			 for (int i =  lastArrayIndex; i < latencies.size(); i++){
				 avg+=latencies.get(i);
				 elementCount++;
			 }
			 
			 lastArrayIndex = latencies.size() - 1;
			 avg= avg/elementCount;
			 avgLatencies.add(avg);
		 }
	 }
	 
	 
	 class ResubmitTimer extends TimerTask{

		@Override
		public void run() {
			int finished = numReqDeviverd.get();
			currentCpuTime = System.currentTimeMillis();			
			//Find average latency
			 int avg = 0, elementCount = 0;			 
			 //List<Integer> latCopy = new ArrayList<Integer>(latencies);
			 for (int i =  lastArrayIndexUsingTime; i < latencies.size(); i++){
				 if (latencies.get(i)>199){
					 largeLatCount++;
					 largeLatencies.add((currentCpuTime - startThroughputTime) + "/" + latencies.get(i));
				 }
				 avg+=latencies.get(i);
				 elementCount++;
			 }
			 
			 lastArrayIndexUsingTime = latencies.size();
			 avg= avg/elementCount;
		 
			String mgsLat = (currentCpuTime - startThroughputTime) + "/" +
			                ((finished - lastFinished) + "/" + avg);// (TimeUnit.MILLISECONDS.toSeconds(currentCpuTime - lastCpuTime)));
			avgLatenciesTimer.add(mgsLat);
			lastFinished = finished;
			lastCpuTime = currentCpuTime;
		}
		 
	 }
}
