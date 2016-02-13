package org.jgroups.protocols.jzookeeper;

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

import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.Version;
import org.jgroups.View;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.jmx.JmxConfigurator;
import org.jgroups.util.Util;

public class ZabCoinTossingClient extends ReceiverAdapter {
	private String props;
	private static String ProtocotName = "ZabCoinTossing";
	private JChannel channel;
	private Address local_addr = null;
	final AtomicInteger actually_sent = new AtomicInteger(0); 
	private CyclicBarrier barrier;
	private AtomicLong local = new AtomicLong(0);
	private byte[] payload;
	private long numsMsg = 0;
	private long num_msgsPerThreads;
	private boolean startReset = true;
	private Sender sender;
	private long start, end, startTh, st=0;
	private volatile long  msgReceived = 0;
	private List<Address> zabBox = new ArrayList<Address>();
	private List<Long> latencies = new ArrayList<Long>();
	private View view;
	private static Scanner read = new Scanner(System.in);
	private static Calendar cal = Calendar.getInstance();
	private  short ID = ClassConfigurator
			.getProtocolId(ZabCoinTossing.class);
	private static int load = 1;
	private static int count = 0;
	private long numSendMsg=0;
	private volatile  boolean isSend = false;
    private static boolean is_warmUp = false;
    private int msgReceivedWarmUp = 0;
    private long warmUpRequests = 0;
    private long currentLoad = 0;
    private ZabCoinTossingTest zabCoinTossingTest= new ZabCoinTossingTest();


	public ZabCoinTossingClient(List<Address> zabbox, CyclicBarrier barrier, long numsMsg, AtomicLong local,
			byte[] payload, String ProtocotName, long num_msgsPerThreads, String propsFile, int load, long warmUpRequests, ZabCoinTossingTest zabCoinTossingTest ) {
		this.barrier = barrier;
		this.local = local;
		this.payload = payload;
		this.numsMsg = numsMsg;
		this.zabBox =zabbox;
		this.ProtocotName = ProtocotName;
		this.num_msgsPerThreads = num_msgsPerThreads;
		this.warmUpRequests = warmUpRequests;
		this.props = propsFile;
		this.load = load;
		this.zabCoinTossingTest = zabCoinTossingTest;
		this.ID = ClassConfigurator
			.getProtocolId(ZabCoinTossing.class);
	}

	public void init() {
		 startTh=0;
		 st=0;
		 msgReceived = 0;
		 startReset = true;
		 numSendMsg=0;
		 msgReceivedWarmUp=0;
		 latencies.clear();
	}
	
	public void setWarmUp(boolean warmUp){
		is_warmUp= warmUp;
	}

	public void viewAccepted(View new_view) {
		System.out.println("** view: " + new_view);
		view = new_view;
		System.out.println("** view: " + new_view);
	}

	public void start() throws Exception {
		StringBuilder sb = new StringBuilder();
		sb.append("\n\n----------------------- ZABPerf -----------------------\n");
		sb.append("Date: ").append(new Date()).append('\n');
		sb.append("Run by: ").append(System.getProperty("user.name"))
				.append("\n");
		sb.append("JGroups version: ").append(Version.description).append('\n');
		System.out.println(sb);
		channel = new JChannel(props);
		channel.setReceiver(this);
		channel.connect("ZABCluster");
		local_addr = channel.getAddress();
		//JmxConfigurator.registerChannel(channel, Util.getMBeanServer(),
				//"jgroups", "ZABCluster", true);
		Address coord = channel.getView().getMembers().get(0);

	}

	public void sendMessages(long numMsgs) {
		msgReceived=0;
		this.currentLoad = numMsgs;
		this.sender = new Sender(this.barrier, this.local,
				this.payload, numMsgs, load);
		System.out.println("Start sending "+ sender.getName());
		sender.start();
	}

	private String getCurrentTimeStamp() {
		long timestamp = new Date().getTime();
		cal.setTimeInMillis(timestamp);
		String timeString = new SimpleDateFormat("HH:mm:ss:SSS").format(cal
				.getTime());

		return timeString;
	}

	public void receive(Message msg) {
		synchronized (this) {
			final ZabCoinTossingHeader testHeader = (ZabCoinTossingHeader) msg.getHeader(ID);
			MessageId message = testHeader.getMessageId();
			if (testHeader.getType() != ZabCoinTossingHeader.START_SENDING) {
				if (is_warmUp){
					msgReceived++;
					if(msgReceived>=warmUpRequests){
						try {
							zabCoinTossingTest.finishedSend();
						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
				else if(testHeader.getType()==ZabCoinTossingHeader.RESPONSE){
					msgReceived++;
					if(msgReceived>=num_msgsPerThreads)
						zabCoinTossingTest.finishedTest();
				}

			}
		}

	}

	public class Sender extends Thread {
		private final CyclicBarrier barrier;
		private AtomicLong local = new AtomicLong(0);// , actually_sent;
		private final byte[] payload;
		private long num_msgsPerThreads;
		private int load=1;


		protected Sender(CyclicBarrier barrier, AtomicLong local,
				byte[] payload, long num_msgsPerThreads, int load) {
			super("" + (count++));
			this.barrier = barrier;
			this.payload = payload;
			this.local = local;
			this.num_msgsPerThreads = num_msgsPerThreads;
			this.load = load;

		}

		public void run() {
			System.out.println("Thread start " + getName());
//			try {
//	            barrier.await();
//	        }
//	        catch(Exception e) {
//	            e.printStackTrace();
//	            return;
//	        }
			Address target;
			st = System.currentTimeMillis();
			startTh = System.currentTimeMillis();
			numSendMsg =0;
			for (int i = 0; i < num_msgsPerThreads; i++) {
				numSendMsg = i;
				while ((numSendMsg - msgReceived) > load){
					//System.out.println("Outstanding is ----> "+(numSendMsg - msgReceived));
					try {
						this.sleep(0,1);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				}
				try {
					MessageId messageId = new MessageId(local_addr,
							local.getAndIncrement(), System.currentTimeMillis());

					ZabCoinTossingHeader hdrReq = new ZabCoinTossingHeader(ZabCoinTossingHeader.REQUEST,
							messageId);
					target = Util.pickRandomElement(zabBox);
					Message msg = new Message(target, payload);
					msg.putHeader(ID, hdrReq);
				//	System.out.println("sender " + this.getName()+ " Sending " + i + " out of " + num_msgsPerThreads);
					channel.send(msg);
					//isSend = true;
					//while (isSend){
						//wait until notify
					//}

				} catch (Exception e) {
				}
			}
		}
	}
}
