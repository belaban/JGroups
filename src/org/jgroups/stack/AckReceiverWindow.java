// $Id: AckReceiverWindow.java,v 1.1.1.1 2003/09/09 01:24:12 belaban Exp $

package org.jgroups.stack;


import org.jgroups.Message;
import org.jgroups.log.Trace;

import java.util.HashMap;



/**
 * Counterpart of AckSenderWindow. Every message received is ACK'ed (even duplicates) and added to a hashmap
 * keyed by seqno. The next seqno to be received is stored in <code>next_to_remove</code>. When a message with
 * a seqno less than next_to_remove is received, it will be discarded. The <code>remove()</code> method removes
 * and returns a message whose seqno is equal to next_to_remove, or null if not found.<br>
 * Change May 28 2002 (bela): replaced TreeSet with HashMap. Keys do not need to be sorted, and adding a key to
 * a sorted set incurs overhead.
 * @author Bela Ban
 */
public class AckReceiverWindow {
    long      initial_seqno=0, next_to_remove=0;
    HashMap   msgs=new HashMap();  // keys: seqnos (Long), values: Messages


    public AckReceiverWindow(long initial_seqno) {
	this.initial_seqno=initial_seqno;
	next_to_remove=initial_seqno;
    }


    public void add(long seqno, Message msg) {
	if(seqno < next_to_remove) {
	    if(Trace.trace)
		Trace.warn("AckReceiverWindow.add()", "discarded msg with seqno=" + seqno +
			   " (next msg to receive is " + next_to_remove + ")");
	    return;
	}
	msgs.put(new Long(seqno), msg);
    }



    /**
     * Removes a message whose seqno is equal to <code>next_to_remove</code>, increments the latter.
     * Returns message that was removed, or null, if no message can be removed. Messages are thus
     * removed in order.
     */
    public Message remove() {
	Message retval=(Message)msgs.remove(new Long(next_to_remove));
	if(retval != null)
	    next_to_remove++;
	return retval;
    }



    public void reset() {
	msgs.clear();
	next_to_remove=initial_seqno;
    }


    public String toString() {
	return msgs.keySet().toString();
    }




    public static void main(String[] args) {
	AckReceiverWindow win=new AckReceiverWindow(33);
	Message           m=new Message();

	win.add(37, m);
	System.out.println(win);

	while((win.remove()) != null) {
	    System.out.println("Removed message, win is " + win);
	}


	win.add(35, m);
	System.out.println(win);

	win.add(36, m);
	System.out.println(win);

	while((win.remove()) != null) {
	    System.out.println("Removed message, win is " + win);
	}



	win.add(33, m);
	System.out.println(win);

	win.add(34, m);
	System.out.println(win);


	win.add(38, m);
	System.out.println(win);


	while((win.remove()) != null) {
	    System.out.println("Removed message, win is " + win);
	}


	win.add(35, m);
	System.out.println(win);

	win.add(332, m);
	System.out.println(win);


    }

}
