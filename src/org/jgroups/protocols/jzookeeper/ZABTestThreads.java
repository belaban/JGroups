package org.jgroups.protocols.jzookeeper;

import org.jgroups.*;

import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jgroups.protocols.jzookeeper.ZAB;
import org.jgroups.protocols.jzookeeper.ClientThread.Sender;
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

public class ZABTestThreads extends ReceiverAdapter {
	private List<String> zabboxInit = new ArrayList<String>();
	private String propsFile = "conf/ZAB.xml";
	private static String ProtocotName = "ZAB";
	private JChannel channel;
	private Address local_addr = null;
	private List<Address> zabBox = new ArrayList<Address>();
	private View view;
	private static long num_msgs = 100000;
	private static long num_msgsPerThreads = 10000;
	private static int msg_size = 20000;
	private static int numOfClients = 5;
	private static int num_threads = 10;
	private long log_interval = num_msgs / 10; 
	private long receive_log_interval = Math.max(1, num_msgs / 10);
	private ClientThread[] clientThreads = new ClientThread[num_threads];
	private final byte[] payload = new byte[msg_size];
	private AtomicLong localSequence = new AtomicLong(); 
	private short ID;
	
    private String outputDir;
    private static PrintWriter outFile;
    private static int numsThreadFinished = 0;
    private static int avgTimeElpased = 0;
    private static long avgRecievedOps = 0;



	public ZABTestThreads(String [] zabHosts, String protocolName, String props,
						 int totalNum_msgs, int totalPerThreads, int num_threads,
						 int msg_size, String outputDir, int numOfClients){
		this.zabboxInit =  Arrays.asList(zabHosts);
		this.ProtocotName = protocolName;
		this.propsFile = props;
		this.num_msgs = totalNum_msgs;
		this.num_msgsPerThreads = totalPerThreads;
		this.num_threads = num_threads; 
		this.msg_size = msg_size;
		this.outputDir = outputDir;
		this.numOfClients = numOfClients;
		this.ID = ClassConfigurator
					.getProtocolId((this.ProtocotName.equals("ZAB"))?ZAB.class:MMZAB.class);
		
	}

	public void viewAccepted(View new_view) {
		System.out.println("** view: " + new_view);
		view = new_view;
		List<Address> mbrs = new_view.getMembers();
		if (mbrs.size() == 3) {
			zabBox.addAll(new_view.getMembers());
		}

		if (mbrs.size() > 3 && zabBox.isEmpty()) {
			for (int i = 0; i < 3; i++) {
				zabBox.add(mbrs.get(i));
			}
		}
		local_addr=channel.getAddress();
		
	}

	public void setup() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("\n\n----------------------- ZABPerf -----------------------\n");
		sb.append("Date: ").append(new Date()).append('\n');
		sb.append("Run by: ").append(System.getProperty("user.name"))
				.append("\n");
		sb.append("JGroups version: ").append(Version.description).append('\n');
		System.out.println(sb);
		channel = new JChannel(propsFile);
		channel.setReceiver(this);
		channel.connect("ZABCluster");
		JmxConfigurator.registerChannel(channel, Util.getMBeanServer(),
				"jgroups", "ZABCluster", true);
		Address coord = channel.getView().getMembers().get(0);

	}
	
	public void setupClientThreads(){

		final CyclicBarrier barrier = new CyclicBarrier(num_threads + 1);
		System.out.println("Host name for client"+ local_addr.toString().split("-")[0]);
		
		if (!zabboxInit.contains(local_addr.toString().split("-")[0])) {
			for (int i = 0; i < clientThreads.length; i++) {
				clientThreads[i] = new ClientThread(zabBox, barrier, num_msgs,
						localSequence, payload, ProtocotName, num_msgsPerThreads, propsFile);
			}
		}

		if ((view != null) && zabBox.size() != 0
				&& view.getMembers().size() > 3 && !zabBox.contains(local_addr)) {
			for (int i = 0; i < clientThreads.length; i++) {
				try {
					clientThreads[i].start();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}

		}
		
		
	}

	public void sendStartSign() throws Exception {
		System.out.println("inside sendStartSign");
		MessageId mid = new MessageId(local_addr,
				localSequence.incrementAndGet());
		ZABHeader startHeader = new ZABHeader(ZABHeader.START_SENDING, 1, mid);
		Message msg = new Message(null).putHeader(ID, startHeader);
		msg.setObject("req");
		channel.send(msg);
	}

	
	public void receive(Message msg) {
		final ZABHeader testHeader = (ZABHeader) msg.getHeader(ID);
		
			if (testHeader.getType() == ZABHeader.START_SENDING) {
      	numsThreadFinished=0;
	    	avgTimeElpased =0;
		    avgRecievedOps=0;
				System.out.println("I recieved START_SENDING");

				for (int i = 0; i < clientThreads.length; i++) {
					System.out.println("inside for i = "+i);
					clientThreads[i].sendMessages();				
			    }
				try {
					this.outFile = new PrintWriter(new BufferedWriter(new FileWriter
							(outputDir+InetAddress.getLocalHost().getHostName()+".log",true)));
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
	        }
	}
	
	public synchronized static void finishedopsSoFar(long opNums, Sender sender){
		//System.out.println("senter " + sender.getName()+ " has finished "+opNums+" ops");
	}
	
	public synchronized static void result(long numOpsRecieved, Sender sender,
			                              long timeElapsed, List<Long> latencies){
		numsThreadFinished++;
		avgTimeElpased +=timeElapsed;
		avgRecievedOps+=numOpsRecieved;
		if (numsThreadFinished==1){
			outFile.println();
			outFile.println();
			outFile.println("Test "+ProtocotName + " /numMsgs For Each Thread " + num_msgsPerThreads+
					" /numMsgs For Each Client " + num_msgs+ " /numsThreads "+num_threads+" /msg size " + msg_size+
					" /numOfClients All Cluster " + numOfClients+" /numMsgs For All Cluster "+ num_msgs*numOfClients);
			outFile.println("------------------------ Result --------------------------");
		}
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
		outFile.println("Sender " + sender.getName()+ " Finished "+
				numOpsRecieved + " Throughput per sender "+(numOpsRecieved/TimeUnit.MILLISECONDS.toSeconds(timeElapsed))+" ops/sec"
				+" /Latency-----> Min = " + min + " /Avg = "+ (avg/latencies.size())+
		        " /Max = " +max);
		if (numsThreadFinished >= num_threads){
			avgTimeElpased/=numsThreadFinished;
			outFile.println("Throughput Per Client " +(avgRecievedOps/TimeUnit.MILLISECONDS.toSeconds(avgTimeElpased))+" ops/sec");
			outFile.println("Throughput All Cluster " +((avgRecievedOps*numOfClients)/TimeUnit.MILLISECONDS.toSeconds(avgTimeElpased))
					+" ops/sec");
		    outFile.println("Test Generated at "+ new Date()+ " Lasted for " + TimeUnit.MILLISECONDS.toSeconds(avgTimeElpased)); 
			System.out.println("File closed" +"############################################"); 
			outFile.close();	
		}

}

	public static void main(String[] args) {
		String propsFile = "conf/sequencer.xml";
		String name ="ZAB";
		String outputDir= "/home/pg/p13/a6915654/"+name+"/";
		String [] zabboxInits= new String[4];
        int msgSize = 1000;
        int numsThreads= 10;
        int numberOfMessages= 100000; // #Msgs to be executed by this node
        int totalMessages= 1000000; // #Msgs to be sent by the whole cluster
        int numOfClients= 10; 

        for (int i = 0; i < args.length; i++) {

        	if("-config".equals(args[i])) {
                propsFile = args[++i];
                continue;
            }
        	if ("-hosts".equals(args[i])){
				zabboxInits = args[++i].split(","); 
		    }
        	 if("-name".equals(args[i])) {
                name = args[++i];
                continue;
            }
        	 if("-tmessages".equals(args[i])){
                totalMessages = Integer.parseInt(args[++i]);
                continue;
            }
        	 if("-nmessages".equals(args[i])) {
                numberOfMessages = Integer.parseInt(args[++i]);
                continue;
            }
        	 if("-threads".equals(args[i])) {
            	numsThreads = Integer.parseInt(args[++i]);
                continue;
            }
        	 if("-msgSize".equals(args[i])) {
            	msgSize = Integer.parseInt(args[++i]);
                continue;
            }
        	 if("-outputDir".equals(args[i])) {
            	outputDir = args[++i];
                continue;
            }
        	 if("-numClients".equals(args[i])) {
        		 numOfClients = Integer.parseInt(args[++i]);
                 continue;
             }
        	 
        }
      
		try {
			final ZABTestThreads test = new ZABTestThreads(zabboxInits, name, propsFile, totalMessages,
															numberOfMessages, numsThreads, msgSize, 
															outputDir, numOfClients);
			
			test.setup();
			test.setupClientThreads();
			test.loop();

	} catch (Exception e) {
		e.printStackTrace();
	}

	}
	
	public void loop() {
		int c;

		final String INPUT = "[1] Send start request to all clients \n[2] View \n[3] Change number  of message\n"
				+ "[4] Change size of request \n[5] Change number of threads \n"
				+ "";

		while (true) {
			try {
				c = Util.keyPress(String.format(INPUT));
				switch (c) {
				case '1':
					for (int i = 0; i < clientThreads.length; i++) {
						clientThreads[i].init();
					}
					System.out.println("Start Test ----->");
					sendStartSign();
					break;
				case '2':
					// System.out.println("view: " + channel.getView() +
					// " (local address=" + channel.getAddress() + ")");
					break;
				case '3':
					System.out.println("Enter number of message");
					// num_msgs = read.nextLong();
					break;
				case '4':
					System.out.println("Enter Size of message");
					//msg_size = read.nextInt();
					break;
				case '5':
					System.out.println("Enter number of threads");
					// num_threads = read.nextInt();
					break;
				case '6':
					// ProtocolStack stack=channel.getProtocolStack();
					// String cluster_name=channel.getClusterName();
					try {
						// JmxConfigurator.unregisterChannel(channel,
						// Util.getMBeanServer(), "jgroups", "ChatCluster");
					} catch (Exception e) {
					}
					// stack.stopStack(cluster_name);
					// stack.destroy();
					break;
				}
			} catch (Throwable t) {
				t.printStackTrace();
			}
		}

	}

}
