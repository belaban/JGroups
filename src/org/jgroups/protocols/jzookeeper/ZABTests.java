package org.jgroups.protocols.jzookeeper;
import org.jgroups.*;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.protocols.jzookeeper.Zab2PhasesWithCommit;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.Version;
import org.jgroups.View;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Util;

public class ZABTests extends ReceiverAdapter{
	
    String propsFile = "conf/sequencer.xml";
    String initiator = "";
    protected String                props;
    protected JChannel              channel;
    protected Address               local_addr=null;
    protected String                name;
    protected long                  num_msgs=1000;
    protected int                   msg_size=1000;
    protected int                   num_threads=32;
    protected long                  log_interval=num_msgs / 10; // log every 10%
    protected long                  receive_log_interval=Math.max(1, num_msgs / 10);
    protected View 					view;
    protected List<Address>			zabBox = new ArrayList<Address>();
    final AtomicLong    seqno=new AtomicLong(-1); // monotonically increasing seqno, to be used by all threads
    private final Map<MessageId,Stats> latencies = Collections.synchronizedMap(new HashMap<MessageId,Stats>());
    //private final Map<MessageId,Stats> latencies = new HashMap<MessageId,Stats>();

    protected static final short                  ID=ClassConfigurator.getProtocolId(MMZAB.class);
    private AtomicLong localSequence = new AtomicLong(); // This nodes sequence number

    long start, end;
    int msgReceived =0;
    Scanner read = new Scanner(System.in);
    Calendar cal = Calendar.getInstance();
    
    public void start(String props, String name) throws Exception {
    this.props=props;
    this.name=null;
    StringBuilder sb=new StringBuilder();
    sb.append("\n\n----------------------- ZABPerf -----------------------\n");
    sb.append("Date: ").append(new Date()).append('\n');
    sb.append("Run by: ").append(System.getProperty("user.name")).append("\n");
    sb.append("JGroups version: ").append(Version.description).append('\n');
    System.out.println(sb);
    channel=new JChannel("conf/sequencer.xml");
    if(this.name != null)
        channel.name(name);
    channel.setReceiver(this);
    channel.connect("ChatCluster");
    local_addr=channel.getAddress();
    JmxConfigurator.registerChannel(channel, Util.getMBeanServer(), "jgroups", "ChatCluster", true);

    Address coord=channel.getView().getMembers().get(0);
    
}

public void viewAccepted(View new_view) {
    System.out.println("** view: " + new_view);
	 view = new_view;
	 List<Address> mbrs=new_view.getMembers();
     if (mbrs.size() == 3){
     	zabBox.addAll(new_view.getMembers());     
    	System.out.println("["+ local_addr+ "]"+ "Zab box view = " + zabBox);

     }
     
     if (mbrs.size() > 3 && zabBox.isEmpty()){
     	for (int i = 0; i < 3; i++) {
     		zabBox.add(mbrs.get(i));
			}
    	System.out.println("["+ local_addr+ "]"+ "Zab box view = " + zabBox);
     }
    System.out.println("** view: " + new_view);
}

public void sendMessages(long mesNums, int mesSize, int num_threads) {
    final AtomicInteger actually_sent=new AtomicInteger(0); // incremented *after* sending a message
    final AtomicLong    seqno=new AtomicLong(1); // monotonically increasing seqno, to be used by all threads
    final Sender[]      senders=new Sender[num_threads];
    final CyclicBarrier barrier=new CyclicBarrier(num_threads +1);
    final byte[]        payload=new byte[mesSize];

    for(int i=0; i < num_threads; i++) {
        senders[i]=new Sender(barrier, mesNums, localSequence, actually_sent, seqno, payload);
        senders[i].setName("sender-" + i);
        senders[i].start();
    }
    try {
        System.out.println("-- sending " + mesNums);
        barrier.await();
    }
    catch(Exception e) {
        System.err.println("failed triggering send threads: " + e);
    }
}

public void receive(Message msg) {
    final ZabHeader testHeader = (ZabHeader) msg.getHeader(ID);
    synchronized(this){
	    if (testHeader.getType()!=ZabHeader.START_SENDING){
	    	
	    	//synchronized(latencies){
		    	Stats stat = latencies.get(testHeader.getMessageId());
		    	if(!stat.equals(null)){
		    		stat.end();
		    		Sender sender = stat.getSender();
		    		System.out.println("Notify name "+ stat.getSender().getName());
		    		sender.setSendAllow(true);
		    		System.out.println("latency= "+stat.toString());
		    	}
	    	//}
	    	
			msgReceived++;
		    String line="[" + msg.getSrc() + "]: " + msg.getObject();
		    System.out.println(line);
		    end = System.nanoTime();	    
		    System.out.println("message received = zxid  is = " + testHeader.getZxid()+" "+ msgReceived + " at "+ getCurrentTimeStamp());
		    System.out.println("Throughput = " + (end - start)/1000000);
		    //System.out.println("Test Done ");
    }
    
    else {
    	//System.out.println("[" + local_addr + "] "+ "Received START_SENDING "+ getCurrentTimeStamp());
    	msgReceived=0;
    	sendMessages(4500, 1000,32);
    }
    }
}

private String getCurrentTimeStamp(){
	long timestamp = new Date().getTime();  
	cal.setTimeInMillis(timestamp);
	String timeString =
		   new SimpleDateFormat("HH:mm:ss:SSS").format(cal.getTime());

	return timeString;
}

public static void main(String[] args) {
    String props="conf/sequencer.xml", name="ZAB";
     final ZABTests test=new ZABTests();
    try {
        test.start(props, name);
        test.loop();
        
    }
    catch(Exception e) {
        e.printStackTrace();
    }
 }
public void loop() {
    int c;
    
    final String INPUT="[1] Send start request to all clients \n[2] View \n[3] Change number  of message\n"
    		+ "[4] Change size of request \n[5] Change number of threads \n" +"";

    while(true) {
        try {
            c=Util.keyPress(String.format(INPUT));
            switch(c) {
                case '1':
                	msgReceived=0;
                	latencies.clear();
                	MessageId mid = new MessageId(local_addr, seqno.incrementAndGet());
                	ZabHeader startHeader = new ZabHeader(ZabHeader.START_SENDING,1, mid);
                	Message msg = new Message(null).putHeader(ID, startHeader);
                	msg.setObject("req");
         			channel.send(msg);
                    break;
                case '2':
                    System.out.println("view: " + channel.getView() + " (local address=" + channel.getAddress() + ")");
                    break;
                case '3':
                	System.out.println("Enter number of message");
                	num_msgs = read.nextLong();
                	break;
                case '4':
                	System.out.println("Enter Size of message");
                	msg_size = read.nextInt();
                	break;
                case '5':
                	System.out.println("Enter number of threads");
                	num_threads = read.nextInt();
                	break;
                case '6':
                    ProtocolStack stack=channel.getProtocolStack();
                    String cluster_name=channel.getClusterName();
                    try {
                        JmxConfigurator.unregisterChannel(channel, Util.getMBeanServer(), "jgroups", "ChatCluster");
                    }
                    catch(Exception e) {
                    }
                    stack.stopStack(cluster_name);
                    stack.destroy();
                    break;               
        }
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }

}

public class Sender extends Thread {
    private final CyclicBarrier barrier;
    private  AtomicLong local = new AtomicLong(0);//, actually_sent;
    private final AtomicLong    seqno;
    private final byte[]        payload;
    private final long    numsMsg;
    private volatile boolean sendAllow = false;



    protected Sender(CyclicBarrier barrier, long numsMsg,  AtomicLong local, AtomicInteger actually_sent,
                     AtomicLong seqno, byte[] payload) {
        this.barrier=barrier;
        this.seqno=seqno;
        this.payload=payload;
        this.numsMsg=numsMsg;
        this.local=local;
    }
    
    public boolean isSendAllow(){
    	return sendAllow;
    }
    
    public void setSendAllow(boolean sendAllow){
    	this.sendAllow = sendAllow;
    }

    public void run() {
    	System.out.println("Thread start"+ getName());
    	Stats stat;
        try {
            barrier.await();
        }
        catch(Exception e) {
            e.printStackTrace();
            return;
        }

        Address target;
        start = System.nanoTime();

        for  (int i = 0; i < numsMsg; i++) {
			
            try {
   	    	    MessageId messageId = new MessageId(local_addr, local.getAndIncrement()); // Increment localSequence
   	    	    
   	    	    //synchronized(latencies){
   	    	    	stat = new Stats();
   	   	    	    stat.addMessage();
   	   	    	    stat.setSender(this);
   	   	    	    latencies.put(messageId, stat);
   	    	   // }
   	    	    ZabHeader hdrReq=new ZabHeader(ZabHeader.REQUEST, messageId);  
        		target = Util.pickRandomElement(zabBox);
                Message msg=new Message(target, payload);
                msg.putHeader(ID, hdrReq);
                System.out.println("Sending "+i+" out of "+numsMsg);
                channel.send(msg);
                setSendAllow(false);
	    		System.out.println("Waiting name "+ getName());
	    		//long p =0;
	    		while(!sendAllow){
	    			// doing no things
    			}
	    		
	    	}
            catch(Exception e) {
            }
        }
    }
}
    

}
