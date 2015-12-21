package org.jgroups.protocols.jzookeeper;
import org.jgroups.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jgroups.protocols.jzookeeper.Zab;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.Version;
import org.jgroups.View;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

public class ZABBenchmarkMultiThreads extends ReceiverAdapter{
	
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
    final AtomicLong    seqno=new AtomicLong(-1); // monotonically increasing seqno, to be used by all threads

    protected static final short                  ID=ClassConfigurator.getProtocolId(Zab.class);

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

    channel=new JChannel(props);
    channel.setName(name);
    channel.setReceiver(this);
    channel.connect("ZABPerf");
    local_addr=channel.getAddress();
    JmxConfigurator.registerChannel(channel, Util.getMBeanServer(), "jgroups", "ZABPerf", true);

    // send a CONFIG_REQ to the current coordinator, so we can get the current config
    Address coord=channel.getView().getMembers().get(0);
    
}


public void viewAccepted(View new_view) {
	 List<Address> mbrs=new_view.getMembers();
     if (mbrs.size() == 3){
     	zabBox.addAll(new_view.getMembers());     	
     }
     
     if (mbrs.size() > 3 && zabBox.isEmpty()){
     	for (int i = 0; i < 3; i++) {
     		zabBox.add(mbrs.get(i));
			}

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

    final ZabHeader testHeader = (ZabHeader) msg.getHeader(ID);
    if (testHeader != null && testHeader.getType()==ZabHeader.START_SENDING)
        sendMessages(1000, 1000,3);
    else{
		msgReceived++;
	    String line="[" + msg.getSrc() + "]: " + msg.getObject();
	    System.out.println(line);
	    end = System.nanoTime();
	    
	    System.out.println("messgages received is = " + msgReceived + " at "+ getCurrentTimeStamp());
	    System.out.println("Throughput = " + (end - start)/1000000);
	    System.out.println("Test Done ");
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
     final ZABBenchmarkMultiThreads test=new ZABBenchmarkMultiThreads();
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

    final String INPUT="[1] Send start request to all clients [2] View\n" +"";

    while(true) {
        try {
            c=Util.keyPress(String.format(INPUT));
            switch(c) {
                case '1':
                	
                	MessageId mid = new MessageId(local_addr, seqno.incrementAndGet());
                	ZabHeader startHeader = new ZabHeader(ZabHeader.START_SENDING, mid);
                	Message msg = new Message();
                	msg.putHeader(ID, startHeader);
                	
                	for (Address client : zabBox){
                		if (!zabBox.contains(local_addr)){
                			msg.setDest(client);
                			channel.send(msg);			
                		}
                	}
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
                long new_seqno=seqno.getAndIncrement();
        		target = Util.pickRandomElement(channel.getView().getMembers());

                Message msg=new Message(target, payload);
                channel.send(msg);
               }
            catch(Exception e) {
            }
        }
    }
}
    

}
