// $Id: UnicastTest2.java,v 1.2 2004/02/26 19:14:15 belaban Exp $


package org.jgroups.tests;


import java.io.Serializable;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;
import org.jgroups.*;
import org.jgroups.util.*;
import org.jgroups.log.Trace;




/**
 * Demos the UNICAST protocol. As soon as we have 2 members in a group, a destination member is randomly
 * chosen (not self !). Then, as long as that destination member is member of the group, we send NUM_MSGS
 * unicast messages to it. The receiver checks that it receives messages in order (monotonically increasing).<p>
 * The sample protocol stack below has a DISCARD protocol in it, which randomly discards
 * both unicast and multicast messages (in the example below, down messages are discarded with a probability
 * of 10%, i.e. 1 out of 10 messages is discarded)).<p>
 * If you want to see the informational messages for DISCARD and UNICAST, you have to enable them in trace, e.g.
 * by adding the following statements to your jgroups.properties file (in your home directory):
 * <pre>
 * trace1=DISCARD DEBUG STDOUT
 * trace2=UNICAST DEBUG STDOUT
 * </pre>
 * @author Bela Ban
 */
public class UnicastTest2 implements Runnable {
    Channel   channel;
    String    groupname="UnicastTest2Group";
    String    props="UDP:PING:FD:DISCARD(down=0.1):NAKACK(retransmit_timeout=1000):"+
	            "UNICAST:FRAG:FLUSH:GMS:VIEW_ENFORCER:QUEUE";
    Thread    writer=null;
    Vector    mbrs=new Vector();
    Hashtable senders=new Hashtable();
    boolean   running=true;
    final int NUM_MSGS=100;




    public void start() throws Exception {
	Object            obj;
	Message           msg;
	View              view;
	Vector            tmp;
	UnicastTest2Info  info, myinfo;
	Object            sender;

	channel=new JChannel(props);
	channel.connect(groupname);
	System.out.println("[ready]");

	while(true) {
	    try {
		obj=channel.receive(0);
		if(obj instanceof View) {
		    view=(View)obj;
		    tmp=view.getMembers();
		    mbrs.removeAllElements();
		    for(int i=0; i < tmp.size(); i++)
			mbrs.addElement(tmp.elementAt(i));
		    
		    for(Enumeration e=senders.keys(); e.hasMoreElements();) {
			sender=e.nextElement();
			if(!mbrs.contains(sender)) {
			    mbrs.removeElement(sender);
			}
		    }
		    
		    if(mbrs.size() > 1) {
			if(writer == null) {
			    writer=new Thread(this, "WriterThread");
			    writer.start();
			}
		    }
		    else {
			if(writer != null) {
			    running=false;
			    writer.interrupt();
			}
			writer=null;
		    }
		}
		else if(obj instanceof Message) {
		    msg=(Message)obj;
		    info=(UnicastTest2Info)msg.getObject();
		    System.out.println("Received msg: " + info);

		    myinfo=(UnicastTest2Info)senders.get(info.sender);
		    if(myinfo == null) { // first msg
			if(info.msgno == 1) {
			    // must be 1
			    senders.put(info.sender, info);
			}
			else {
			    // error
			    System.err.println("UnicastTest2.start(): first seqno must be 1");
			}

		    }
		    else {
			if(info.msgno -1 != myinfo.msgno) {
			    System.err.println("UnicastTest2.start(): received msg " + info.sender + ":" +
					       info.msgno + ", but last received was " +
					       myinfo.sender + ":" + myinfo.msgno);
			}
			else {
			    System.out.println("UnicastTest2.start(): OK received " + info.sender + ":" + 
					       info.msgno + ", prev seqno=" + myinfo.sender + ":" + myinfo.msgno);
			    myinfo.msgno++;
			}
		    }

		}
		else
		    ;
	    }
	    catch(ChannelClosedException closed) {
		System.err.println("Channel closed");
		break;
	    }
	    catch(ChannelNotConnectedException not_conn) {
		System.err.println("Channel not connected");
		break;
	    }
	    catch(Exception e) {
		System.err.println(e);
	    }
	}




    }


    Address selectTarget() {
	Vector  tmp=new Vector();
	Address ret;
	int     t;

	if(mbrs == null || mbrs.size() < 2)
	    return null;

	for(int i=0; i < mbrs.size(); i++) {
	    if(!(mbrs.elementAt(i).equals(channel.getLocalAddress())))
		tmp.addElement(mbrs.elementAt(i));
	}
	t=(int)((Math.random() * 100));
	ret=(Address)tmp.elementAt(t % tmp.size());
	return ret;
    }



    public void run() {
	Address           target=selectTarget();
	UnicastTest2Info  info=null;
	int               msgno=1;
	
	if(target == null)
	    return;

	while(running && msgno <= NUM_MSGS) {
	    try {
		info=new UnicastTest2Info(msgno++, channel.getLocalAddress());
		System.out.println("Sending message #" + (msgno-1) + " to " + target);
		channel.send(new Message(target, null, info));
		Thread.sleep(500);
	    }
	    catch(ChannelClosedException closed) {
		System.err.println(closed);
		break;
	    }
	    catch(ChannelNotConnectedException not_conn) {
		System.err.println(not_conn);
	    }
	    catch(Exception e) {
		System.err.println(e);
	    }
	}
	System.out.println("UnicastTest2Info.run(): writer thread terminated");
    }




    public static void main(String[] args) {
	try {
	    Trace.init();
	    new UnicastTest2().start();
	}
	catch(Exception e) {
	    System.err.println(e);
	}
    }


    private static class UnicastTest2Info implements Serializable {
	int    msgno=0;
	Object sender=null;
	
	public UnicastTest2Info() {
	    
	}
	
	public UnicastTest2Info(int msgno, Object sender) {
	    this.msgno=msgno;
	    this.sender=sender;
	}
	
	
	public String toString() {
	    return "#" + msgno + " (sender=" + sender + ")";
	}
    }




}



