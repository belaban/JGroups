// $Id: PullPushTest.java,v 1.1 2003/09/09 01:24:13 belaban Exp $


package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.*;
import org.jgroups.log.Trace;




/**
 * Uses PullPush building block to send/receive messages. Reception is passive, e.g. the receiver's
 * receive() method is invoked whenever a message is received. The receiver has to register a callback method
 * when creating the channel.
 * @author Bela Ban
 */
public class PullPushTest implements MessageListener {
    private Channel          channel;
    private PullPushAdapter  adapter;

    public void receive(Message msg) {
	System.out.println("Received msg: " + msg);
    }

    public byte[] getState() {  // only called if channel option GET_STATE_EVENTS is set to true
	return null;
    }

    public void setState(byte[] state) {
	
    }


    public void start() throws Exception {
	Trace.init();
	channel=new JChannel();
	channel.connect("PullPushTest");
	adapter=new PullPushAdapter(channel);
	adapter.setListener(this);
	
	for(int i=0; i < 10; i++) {
	    System.out.println("Sending msg #" + i);
	    adapter.send(new Message(null, null, new String("Hello world").getBytes()));
	    Thread.sleep(1000);
	}
	channel.close();
	System.exit(0);
    }


    public static void main(String args[]) {
	PullPushTest t=new PullPushTest();
	try {
	    t.start();
	}
	catch(Exception e) {
	    System.err.println(e);
	}
    }

}
