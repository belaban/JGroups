package org.jgroups.protocols.jzookeeper;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.logging.Log;
import org.jgroups.logging.LogFactory;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;
import org.jgroups.util.Bits;

import java.io.*;
import java.util.*;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Scanner;

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.Version;
import org.jgroups.View;

import org.jgroups.util.Util;

public class ZABBenchmarkThreads extends ReceiverAdapter{
    String propsFile = "conf/sequencer.xml";
    String initiator = "";
    protected String                props;
    protected JChannel              channel;
    protected Address               local_addr=null;
    protected String                name;
    protected long                   num_msgs=1000;
    protected int                   msg_size=1000;
    protected int                   num_threads=3;
    protected long                   log_interval=num_msgs / 10; // log every 10%
    protected long                   receive_log_interval=Math.max(1, num_msgs / 10);
    protected View 					view;
    protected List<Address>			zabBox = new ArrayList<Address>();
   
    long start, end;
    int msgReceived =0;
    Scanner read = new Scanner(System.in);
    Calendar cal = Calendar.getInstance();
    
public void start(String props, String name) throws Exception {
    this.props=props;
    this.name=name;
    StringBuilder sb=new StringBuilder();
    sb.append("\n\n----------------------- ZABPerf -----------------------\n");
    sb.append("Date: ").append(new Date()).append('\n');
    sb.append("Run by: ").append(System.getProperty("user.name")).append("\n");
    sb.append("JGroups version: ").append(Version.description).append('\n');
    System.out.println(sb);
    System.out.println("End of start method>>>>>>>>");

    channel=new JChannel(props);
    channel.setName(name);
    channel.setReceiver(this);
    channel.connect("ZABPerf");
    local_addr=channel.getAddress();
    JmxConfigurator.registerChannel(channel, Util.getMBeanServer(), "jgroups", "ZABPerf", true);

    // send a CONFIG_REQ to the current coordinator, so we can get the current config
    Address coord=channel.getView().getMembers().get(0);
    System.out.println("End of start method>>>>>>>>");

    
}


public void viewAccepted(View new_view) {
	 List<Address> mbrs=new_view.getMembers();
     if (mbrs.size() == 3){
     	zabBox.addAll(new_view.getMembers());     	
     }
     
    System.out.println("** view: " + new_view);
}

public void sendMessages(long mesNums, int mesSize, int num_threads) {
    final AtomicInteger num_msgs_sent=new AtomicInteger(0); // all threads will increment this
    final AtomicInteger actually_sent=new AtomicInteger(0); // incremented *after* sending a message
    final AtomicLong    seqno=new AtomicLong(1); // monotonically increasing seqno, to be used by all threads
    final Sender[]      senders=new Sender[num_threads];
    final CyclicBarrier barrier=new CyclicBarrier(num_threads +1);
    final byte[]        payload=new byte[mesSize];

    for(int i=0; i < num_threads; i++) {
        senders[i]=new Sender(barrier, mesNums, num_msgs_sent, actually_sent, seqno, payload);
        senders[i].setName("sender-" + i);
        senders[i].start();
    }
    try {
        System.out.println("-- sending " + num_msgs);
        barrier.await();
    }
    catch(Exception e) {
        System.err.println("failed triggering send threads: " + e);
    }
}

public void receive(Message msg) {
	
	msgReceived++;
    String line="[" + msg.getSrc() + "]: " + msg.getObject();
    System.out.println(line);
    end = System.nanoTime();
    
    System.out.println("messgages received is = " + msgReceived + " at "+ getCurrentTimeStamp());
    System.out.println("Throughput = " + (end - start)/1000000);
    System.out.println("Test Done ");
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

   

    ZABBenchmarkThreads test=new ZABBenchmarkThreads();
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

    final String INPUT="[1] Send [2] View\n" +"";

    while(true) {
        try {
            c=Util.keyPress(String.format(INPUT));
            switch(c) {
                case '1':
                	start=0;
                	end=0;
                    msgReceived =0;
                	System.out.println("Enter number of messages");
                	num_msgs = read.nextLong();
                	System.out.println("Enter message size");
                	msg_size = read.nextInt();
                	System.out.println("Enter number of threads");
                	num_threads = read.nextInt();
                	sendMessages(num_msgs, msg_size, num_threads);
                    break;
                case '2':
                    System.out.println("view: " + channel.getView() + " (local address=" + channel.getAddress() + ")");
                    break;
                
        }
        }
        catch(Throwable t) {
            t.printStackTrace();
        }
    }
}


protected class Sender extends Thread {
    protected final CyclicBarrier barrier;
    protected final AtomicInteger num_msgs_sent = new AtomicInteger(0);//, actually_sent;
    protected final AtomicLong    seqno;
    protected final byte[]        payload;
    protected final long    numsMsg;


    protected Sender(CyclicBarrier barrier, long numsMsg,  AtomicInteger num_msgs_sent, AtomicInteger actually_sent,
                     AtomicLong seqno, byte[] payload) {
        this.barrier=barrier;
       //this.num_msgs_sent=num_msgs_sent;
       // this.actually_sent=actually_sent;
        this.seqno=seqno;
        this.payload=payload;
        this.numsMsg=numsMsg;
    }

    public void run() {
    	System.out.println("Theard start"+ getName());
        try {
            barrier.await();
        }
        catch(Exception e) {
            e.printStackTrace();
            return;
        }

        Address target;
        //Util.sleep(30000);
        if (zabBox.contains(local_addr)){
        	int wait = read.nextInt();
        }
        start = System.nanoTime();
    	System.out.println("Theard start"+ getName());

        for  (int i = 0; i < numsMsg; i++) {
			
            try {
                int tmp=num_msgs_sent.incrementAndGet();
//                if(tmp > numsMsg)
//                    break;
                long new_seqno=seqno.getAndIncrement();
        		target = Util.pickRandomElement(channel.getView().getMembers());

                Message msg=new Message(target, payload);
                channel.send(msg);
                // if we used num_msgs_sent, we might have thread T3 which reaches the condition below, but
                // actually didn't send the *last* message !
                //tmp=actually_sent.incrementAndGet(); // reuse tmp
//                if(tmp >= numsMsg){ // last message, send SENDING_DONE message
//                	System.out.println("Test Done, sent nums message ");
//                	running = false;      
//                	break;
//                }
               }
            catch(Exception e) {
            }
        }
    }
}


}
