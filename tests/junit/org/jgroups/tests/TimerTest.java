// $Id: TimerTest.java,v 1.1 2003/09/09 01:24:13 belaban Exp $
package org.jgroups.tests;



import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;
import junit.framework.*;
import org.jgroups.stack.*;
import org.jgroups.util.Util;




/**
 * Test cases for Timer
 * @author Bela Ban
 */
public class TimerTest extends TestCase {
    Timer           win=null;
    final int       NUM_MSGS=1000;
    long[]          xmit_timeouts={1000, 2000, 4000, 8000};
    double          PERCENTAGE_OFF=0.3; // how much can expected xmit_timeout and real timeout differ to still be okay ?
    HashMap         msgs=new HashMap(); // keys=seqnos (Long), values=Entries

    

    class MyTask extends TimerTask {
	Entry       entry;

	MyTask(Entry entry) {
	    this.entry=entry;
	}

	public void run() {
	    entry.retransmit();
	}

    }


    class Entry {
	long     start_time=0;  // time message was added
	long     first_xmit=0;  // time between start_time and first_xmit should be ca. 1000ms
	long     second_xmit=0; // time between first_xmit and second_xmit should be ca. 2000ms
	long     third_xmit=0;  // time between third_xmit and second_xmit should be ca. 4000ms
	long     fourth_xmit=0; // time between third_xmit and second_xmit should be ca. 8000ms
	Interval interval=new Interval(xmit_timeouts);
	long     seqno=0;

	Entry(long seqno) {
	    this.seqno=seqno;
	    start_time=System.currentTimeMillis();
	}

	
	public void retransmit() {
	    // System.out.println(" -- retransmit(" + seqno + ")");
	    
	    if(first_xmit == 0)
		first_xmit=System.currentTimeMillis();
	    else if(second_xmit == 0)
		second_xmit=System.currentTimeMillis();
	    else if(third_xmit == 0)
		third_xmit=System.currentTimeMillis();
	    else if(fourth_xmit == 0)
		fourth_xmit=System.currentTimeMillis();

	    win.schedule(new MyTask(this), interval.next());
	}


	public long next() {
	    return interval.next();
	}


	/** Entry is correct if xmit timeouts are not more than 30% off the mark */
	boolean isCorrect() {
	    long    t;
	    long    expected;
	    long    diff, delta;
	    boolean off=false;

	    t=first_xmit - start_time;
	    expected=xmit_timeouts[0];
	    diff=Math.abs(expected - t);
	    delta=(long)(expected * PERCENTAGE_OFF);
	    if(diff >= delta) off=true;

	    t=second_xmit - first_xmit;
	    expected=xmit_timeouts[1];
	    diff=Math.abs(expected - t);
	    delta=(long)(expected * PERCENTAGE_OFF);
	    if(diff >= delta) off=true;

	    t=third_xmit - second_xmit;
	    expected=xmit_timeouts[2];
	    diff=Math.abs(expected - t);
	    delta=(long)(expected * PERCENTAGE_OFF);
	    if(diff >= delta) off=true;

	    t=fourth_xmit - third_xmit;
	    expected=xmit_timeouts[3];
	    diff=Math.abs(expected - t);
	    delta=(long)(expected * PERCENTAGE_OFF);
	    if(diff >= delta) off=true;

	    if(off) {
		System.err.println("#" + seqno + ": " + this + ": (" + "entry is more than " + 
				   PERCENTAGE_OFF + " percentage off ");
		return false;
	    }
	    return true;
	}


	public String toString() {
	    StringBuffer sb=new StringBuffer();
	    sb.append(first_xmit - start_time).append(", ").append(second_xmit-first_xmit).append(", ");
	    sb.append(third_xmit-second_xmit).append(", ").append(fourth_xmit-third_xmit);
	    return sb.toString();
	}
    }


    public TimerTest(String name) { super(name); }
    
    
    public void setUp() {
	
    }
    
    public void tearDown() {
	
    }
    
    
    /**
     * Tests whether retransmits are called at correct times for 1000 messages. A retransmit should not be
     * more than 30% earlier or later than the scheduled retransmission time
     */
    public void testRetransmits() {
	Entry  entry;
	MyTask task;
	int    num_non_correct_entries=0;

	win=new Timer();
	
	
	// 1. Add NUM_MSGS messages:
	System.out.println("-- adding " + NUM_MSGS + " messages:");
	for(long i=0; i < NUM_MSGS; i++) {
	    entry=new Entry(i);
	    task=new MyTask(entry);
	    msgs.put(new Long(i), entry);
	    win.schedule(task, entry.next());
	}
	System.out.println("-- done");

	// 2. Wait for at least 4 xmits/msg: total of 1000 + 2000 + 4000 + 8000ms = 15000ms; wait for 20000ms
	System.out.println("-- waiting for 20 secs for all retransmits");
	Util.sleep(20000);
	
	// 3. Check whether all Entries have correct retransmission times
	for(long i=0; i < NUM_MSGS; i++) {
	    entry=(Entry)msgs.get(new Long(i));
	    if(!entry.isCorrect()) {
		num_non_correct_entries++;
	    }
	}
	
	if(num_non_correct_entries > 0)
	    System.err.println("Number of incorrect retransmission timeouts: " + num_non_correct_entries);
	else {
	    for(long i=0; i < NUM_MSGS; i++) {
		entry=(Entry)msgs.get(new Long(i));
		if(entry != null)
		    System.out.println(i + ": " + entry);
	    }
	}
	assertTrue(num_non_correct_entries == 0);
	try {
	    win.cancel();
	}
	catch(Exception ex) {
	    System.err.println(ex);
	}
    }
    
    
    
    public static Test suite() {
	TestSuite suite;
	suite = new TestSuite(TimerTest.class);
	return(suite);
    }

    public static void main(String[] args) {
	String[] name = {TimerTest.class.getName()};
	junit.textui.TestRunner.main(name);
    }
}
