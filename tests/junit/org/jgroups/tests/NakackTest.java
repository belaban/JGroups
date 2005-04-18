// $Id: NakackTest.java,v 1.4 2005/04/18 15:35:12 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.*;
import org.jgroups.debug.ProtocolTester;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;

import java.util.Hashtable;
import java.util.Vector;


public class NakackTest extends TestCase {
    final long WAIT_TIME=5000;
    public final long NUM_MSGS=500;
    long num_msgs_received=0;
    long num_msgs_sent=0;


    public NakackTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
        num_msgs_received=0;
        num_msgs_sent=0;
    }


    public void test0() throws Exception {
        Object mutex=new Object();
        CheckNoGaps check=new CheckNoGaps(-1, this, mutex);
        ProtocolTester t=new ProtocolTester("pbcast.NAKACK", check);
        Address my_addr=new IpAddress("localhost", 10000);
        ViewId vid=new ViewId(my_addr, 322649);
        Vector mbrs=new Vector();
        View view=null;
        IpAddress local_addr=new IpAddress(5555);

        mbrs.addElement(my_addr);
        view=new View(vid, mbrs);
        check.down(new Event(Event.BECOME_SERVER));
        check.down(new Event(Event.VIEW_CHANGE, view));

        synchronized(mutex) {
            for(long i=0; i < NUM_MSGS; i++) {
                if(i % 100 == 0 && i > 0)
                    System.out.println("sending msg #" + i);
                check.down(new Event(Event.MSG, new Message(null, local_addr, new Long(i))));
                num_msgs_sent++;
            }
            mutex.wait(WAIT_TIME);
        }
        System.out.println("\nMessages sent: " + num_msgs_sent + ", messages received: " + num_msgs_received);
        assertTrue(num_msgs_received == num_msgs_sent);
        t.stop();
    }


    public static Test suite() {
        TestSuite s=new TestSuite(NakackTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }


    private static class CheckNoGaps extends Protocol {
        long starting_seqno=0;
        long num_msgs=0;
        Hashtable senders=new Hashtable(); // sender --> highest seqno received so far
        NakackTest t=null;
        Object mut=null;

        CheckNoGaps(long seqno, NakackTest t, Object mut) {
            starting_seqno=seqno;
            this.t=t;
            this.mut=mut;
        }

        public String getName() {
            return "CheckNoGaps";
        }


        public void up(Event evt) {
            Message msg=null;
            Address sender;
            long highest_seqno, received_seqno;
            Long s;

            if(evt == null)
                return;
            if(evt.getType() != Event.MSG)
                return;
            msg=(Message)evt.getArg();
            sender=msg.getSrc();
            if(sender == null) {
                System.err.println("NakackTest.CheckNoGaps.up(): sender is null; discarding msg");
                return;
            }
            s=(Long)senders.get(sender);
            if(s == null) {
                s=new Long(starting_seqno);
                senders.put(sender, s);
            }

            highest_seqno=s.longValue();

            try {
                s=(Long)msg.getObject();
                received_seqno=s.longValue();
                if(received_seqno == highest_seqno + 1) {
                    // correct
                    if(received_seqno % 100 == 0 && received_seqno > 0)
                        System.out.println("PASS: received msg #" + received_seqno);
                    senders.put(sender, new Long(highest_seqno + 1));
                    num_msgs++;
                    if(num_msgs >= t.NUM_MSGS) {
                        synchronized(mut) {
                            t.num_msgs_received=num_msgs;
                            mut.notifyAll();
                        }
                    }
                }
                else {
                    // error, terminate test
                    System.err.println("FAIL: received msg #" + received_seqno);
                }
            }
            catch(Exception ex) {
                System.err.println("NakackTest.CheckNoGaps.up(): " + ex);
            }

        }

        public void startUpHandler() {
            ;
        }

        public void startDownHandler() {
            ;
        }

    }
}


