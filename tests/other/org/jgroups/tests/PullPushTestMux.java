// $Id: PullPushTestMux.java,v 1.1.1.1 2003/09/09 01:24:13 belaban Exp $


package org.jgroups.tests;


import org.jgroups.*;
import org.jgroups.blocks.*;




/**
 * Uses PullPush building block to send/receive messages. Reception is passive, e.g. the receiver's
 * receive() method is invoked whenever a message is received. The receiver has to register a callback method
 * when creating the channel. Uses multiple MessageListeners
 * @author Bela Ban
 */
public class PullPushTestMux implements MessageListener {
    private Channel          channel;
    private PullPushAdapter  adapter;
    MyListener[]             listeners=null;


    public PullPushTestMux() {
	;
    }


    public void receive(Message msg) {
	System.out.println("Main receiver: received msg: " + msg);
    }

    public byte[] getState() {  // only called if channel option GET_STATE_EVENTS is set to true
	return null;
    }

    public void setState(byte[] state) {
	
    }


    public void start() throws Exception {
	int c;

	channel=new JChannel();
	channel.connect("PullPushTestMux");
	adapter=new PullPushAdapter(channel);
	adapter.setListener(this);

	listeners=new MyListener[10];
	for(int i=0; i < listeners.length; i++) {
	    listeners[i]=new MyListener(i, adapter);
	}

	while((c=choice()) != 'q') {
	    c-=48;
	    if(c < 0 || c > 9) {
		System.err.println("Choose between 0 and 9");
		continue;
	    }
	    if(c == 0)
		adapter.send(new Message(null, null, "Message from default message listener"));
	    else
		listeners[c].sendMessage();
	}

	channel.close();
	System.exit(0);
    }


    int choice() {
	int c;
	System.out.println("\n[q]uit [0]: send message on default channel [1-9] send message on other channels:");
	System.out.flush();
	try { 
	    c=System.in.read();
	}
	catch(Exception ex) {
	    return -1;
	}
	finally {
	    try {
		System.in.skip(System.in.available());
	    }
	    catch(Exception ex) {
	    }
	}
	return c;
    }


    public static void main(String args[]) {
	PullPushTestMux t=new PullPushTestMux();
	try {
	    t.start();
	}
	catch(Exception e) {
	    System.err.println(e);
	}
    }


    public class MyListener implements MessageListener {
	Integer         id=null;
	PullPushAdapter ad=null;

	MyListener(int id, PullPushAdapter ad) {
	    this.id=new Integer(id);
	    this.ad=ad;
	    ad.registerListener(this.id, this);
	}
	
	public void receive(Message msg) {
	    System.out.println("MyListener #" + id + ": received message from " + 
			       msg.getSrc() + ": " + msg.getObject());
	}

	public byte[] getState() {
	    return null;
	}

	public void setState(byte[] state) {
	    ;
	}

	void sendMessage() throws Exception {
	    Message msg=new Message(null, null, "Message from " + id);
	    ad.send(id, msg);
	}
    }

}
