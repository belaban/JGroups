// $Id: NakReceiverWindowStressTest.java,v 1.3 2004/01/16 16:47:52 belaban Exp $

package org.jgroups.tests;



import org.jgroups.*;
import org.jgroups.stack.*;
import org.jgroups.util.Util;

import java.io.IOException;




/**
 * Adds a large number of messages (with gaps to simulate message loss) into NakReceiverWindow. Receives messages
 * on another thread. When the receiver thread has received all messages it prints out the time taken and terminates.
 * @author Bela Ban
 */
public class NakReceiverWindowStressTest implements Retransmitter.RetransmitCommand {
    NakReceiverWindow win=null;
    Address           sender=null;
    int               num_msgs=1000, prev_value=0;
    double            discard_prob=0.1; // discard 10% of all insertions
    long              start, stop;
    boolean           trace=false;
    boolean           debug=false;
    

    public NakReceiverWindowStressTest(int num_msgs, double discard_prob, boolean trace, boolean debug) {
	this.num_msgs=num_msgs;
	this.discard_prob=discard_prob;
	this.trace=trace;
    }


    public void retransmit(long first_seqno, long last_seqno, Address sender) {
        for(long i=first_seqno; i <= last_seqno; i++) {
            if(debug)
                out("-- xmit: " + i);
            Message m=new Message(null, sender, new Long(i));
            win.add(i, m);
        }
    }


    public void start() throws IOException {
	System.out.println("num_msgs=" + num_msgs + "\ndiscard_prob=" + discard_prob);

	sender=new IpAddress("localhost", 5555);
	win=new NakReceiverWindow(sender, this, 1);
	start=System.currentTimeMillis();
        sendMessages(num_msgs);
    }


    void sendMessages(int num_msgs) {
        Message msg;

        for(long i=1; i <= num_msgs; i++) {
            if(Util.tossWeightedCoin(discard_prob) && i <= num_msgs) {
                if(debug) out("-- discarding " + i);
            }
            else {
                if(debug) out("-- adding " + i);
                win.add(i, new Message(null, null, new Long(i)));
                if(trace && i % 100 == 0)
                    System.out.println("-- added " + i);
                while((msg=win.remove()) != null)
                    processMessage(msg);
            }
        }
        while(true) {
            while((msg=win.remove()) != null)
                processMessage(msg);
            Util.sleep(50);
        }
    }



    void processMessage(Message msg) {
        long i;

        i=((Long)msg.getObject()).longValue();
        if(prev_value + 1 != i) {
            System.err.println("** processMessage(): removed seqno (" + i + ") is not 1 greater than " +
                    "previous value (" + prev_value + ")");
            System.exit(0);
        }
        prev_value++;
        if(trace && i % 100 == 0)
            System.out.println("Removed " + i);
        if(i == num_msgs) {
            stop=System.currentTimeMillis();
            System.out.println("Inserting and removing " + num_msgs +
                    " messages into NakReceiverWindow took " + (stop - start) + "ms");
            System.out.println("<enter> to terminate");
            try {
                System.in.read();
            }
            catch(Exception ex) {
                System.err.println(ex);
            }
            System.exit(0);
        }
    }

    




    void out(String msg) {
	System.out.println(msg);
    }


    
    public static void main(String[] args) {
	NakReceiverWindowStressTest test;
	int                         num_msgs=1000;
	double                      discard_prob=0.1;
	boolean                     trace=false;
	boolean                     debug=false;

	
	for(int i=0; i < args.length; i++) {
	    if(args[i].equals("-help")) {
		help();
		return;
	    }
	    if(args[i].equals("-num_msgs")) {
		num_msgs=new Integer(args[++i]).intValue();
		continue;
	    }
	    if(args[i].equals("-discard")) {
		discard_prob=new Double(args[++i]).doubleValue();
		continue;
	    }
	    if(args[i].equals("-trace")) {
		trace=true;
		continue;
	    }
	    if(args[i].equals("-debug")) {
		debug=true;
		continue;
	    }
	}


	test=new NakReceiverWindowStressTest(num_msgs, discard_prob, trace, debug);
        try {
            test.start();
        }
        catch(IOException e) {
            e.printStackTrace();
        }
    }


    static void help() {
	System.out.println("NakReceiverWindowStressTest [-help] [-num_msgs <number>] [-discard <probability>] " +
			   "[-trace]");
    }
    
}
