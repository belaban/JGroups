package org.jgroups.demos;

import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.View;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Scanner;

public class Chat extends ReceiverAdapter {
    JChannel channel;
    Scanner read = new Scanner(System.in);


    public void viewAccepted(View new_view) {
        System.out.println("** view: " + new_view);
    }

    public void receive(Message msg) {
        String line="[" + msg.getSrc() + "]: " + msg.getObject();
        System.out.println(line);
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
    	 boolean exit = false;
        while(true) {
            
                System.out.print("Select one of the following");
                System.out.print("(1) send 10 messages to leader");
                System.out.print("(2) send 10 messages to follower");
                System.out.print("(3) send 1 message to leader");
                System.out.print("(4) send 1 messages to follower");
                System.out.print("(5) exit");

               type= read.nextInt();
                switch (type){
                case 1:
                	Message msgl = null;
                	for (int i = 0; i < 10; i++) {
                        msgl=new Message(channel.getView().getMembers().get(0),"msg"+i);
                		channel.send(msgl);				
					}
                	break;
                case 2:
                	Message msgf = null;
                	for (int i = 0; i < 10; i++) {
                        msgf=new Message(channel.getView().getMembers().get(1), "msg"+i);
                		channel.send(msgf);				
					}
                	break;
                case 3:
                	System.out.println("The view is "+channel.getView());
                	System.out.println("Message will send to "+channel.getView().getMembers().get(0));
                	Message msg1l = null;
                        msg1l=new Message(channel.getView().getMembers().get(0), "msg1l");
                		channel.send(msg1l);
                    break;
                case 4:
                	System.out.println("The view is "+channel.getView());
                	System.out.println("Message will send to "+channel.getView().getMembers().get(1));
                	Message msg1f = null;
                        msg1f=new Message(channel.getView().getMembers().get(1), "msg1f");
                		channel.send(msg1f);
                    break;
                case 5:
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

        new Chat().start(props, name);
    }

    protected static void help() {
        System.out.println("Chat [-props XML config] [-name name]");
    }
}
