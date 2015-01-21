package org.jgroups.protocols.jzookeeper;


import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;
import org.jgroups.util.Util;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Scanner;

public class ZABBenchmark extends ReceiverAdapter {
	long start, end;
    int msgReceived =0;
    int numMsg = 0;
    JChannel channel;
    Scanner read = new Scanner(System.in);
    Calendar cal = Calendar.getInstance();


    public void viewAccepted(View new_view) {
        System.out.println("** view: " + new_view);
    }

    public void receive(Message msg) {
    	long timestamp = new Date().getTime();  
		cal.setTimeInMillis(timestamp);
		String timeString =
			   new SimpleDateFormat("HH:mm:ss:SSS").format(cal.getTime());
    	msgReceived++;
        String line="[" + msg.getSrc() + "]: " + msg.getObject();
        System.out.println(line);
        end = System.nanoTime();
        
        System.out.println("messgages received is = " + msgReceived + " at "+ timeString);
        System.out.println("Throughput = " + (end - start)/1000000);
        System.out.println("Test Done ");
//        if (msgReceived >=numMsg){
//        	System.out.println("messgages received is = " + msgReceived);
//            System.out.println("Throughput = " + (end - start)/1000000);
//            System.out.println("Test Done ");
//
//        }
    }

    /** Method called from other app, injecting channel */
    public void start(JChannel ch) throws Exception {
        channel=ch;
        channel.setReceiver(this);
        channel.connect("ChatCluster");
        eventLoop();
        channel.close();
    }

    private void start(String props, String name) throws Exception {
        channel=new JChannel(props);
        if(name != null)
            channel.name(name);
        channel.setReceiver(this);
        channel.connect("ChatCluster");
        eventLoop();
        channel.close();
    }

    private void eventLoop() throws Exception {
    	 int type;
    	 Address target = null;
     	Message msg = null;
    	 Data msgData= new Data();
    	 String da = msgData.createBytes(100);
    	 boolean exit = false;
//    	 System.out.println("Enter number of messages");
//     	numMsg= read.nextInt();
         //start = System.nanoTime();
     	//System.out.println("Message will send to "+target);
//         Thread.sleep(millis);
//     	for (int i = 0; i < 100; i++) {
//     		target = Util.pickRandomElement(channel.getView().getMembers());
//             msg=new Message(target, da);
//     		channel.send(msg);
//     	}
        while(true) {
            
                System.out.println("Select one of the following");
                System.out.println("(1) send to a coordinator , send one message ");
                System.out.println("(2) send to member L(1), one message");
                System.out.println("(3) send to member L(2), one message");
                System.out.println("(4) pick a random member L(1), send one message");
                System.out.println("(5) select number of messages, to a random member");
                System.out.println("(6) select number of messages, each to a random member");
                System.out.println("(7) exit");


               type= read.nextInt();
               msgReceived=0;
                switch (type){
                case 1:
                		start = System.nanoTime();
                        msg=new Message(channel.getView().getMembers().get(0),da);
                		channel.send(msg);				
                	break;
                case 2:
                    	start = System.nanoTime();
                        msg=new Message(channel.getView().getMembers().get(1), da);
                		channel.send(msg);				
                	break;
                case 3:
                        start = System.nanoTime();
                        msg=new Message(channel.getView().getMembers().get(2), da);
                		channel.send(msg);				
                	break;
                case 4:
                       start = System.nanoTime();
                		target = Util.pickRandomElement(channel.getView().getMembers());
                    	System.out.println("The destination follower Addtess is "+target);
                        msg=new Message(target, da);
                		channel.send(msg);
                    break;
                case 5:
                	System.out.println("Enter number of messages");
                	numMsg= read.nextInt();
                    start = System.nanoTime();
                	System.out.println("Message will send to "+target);
                	for (int i = 0; i < numMsg; i++) {
                		target = Util.pickRandomElement(channel.getView().getMembers());
                        msg=new Message(target, da);
                		channel.send(msg);
                	}
                    break;
                case 6:
                	System.out.println("Enter number of messages");
                	numMsg= read.nextInt();
                	System.out.println("Message will send to "+target);
                    start = System.nanoTime();
                	for (int i = 0; i < numMsg; i++) {
                		target = Util.pickRandomElement(channel.getView().getMembers());
                        msg=new Message(target, da);
                		channel.send(msg);
                	}
                    break;
                case 7:
                	exit = true;
                	break;
                }
                if (exit)
                	break;
                
        }
    }


    public static void main(String[] args) throws Exception {
        String props="conf/sequencer.xml";
        String name=null;

        for(int i=0; i < args.length; i++) {
            if(args[i].equals("-props")) {
                props=args[++i];
                continue;
            }
            if(args[i].equals("-name")) {
                name=args[++i];
                continue;
            }
            help();
            return;
        }

        new ZABBenchmark().start(props, name);
    }

    protected static void help() {
        System.out.println("Chat [-props XML config] [-name name]");
    }
}
