// $Id: NakackTest.java,v 1.1 2007/07/04 07:29:33 belaban Exp $

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
    static final long WAIT_TIME=5000;
    public static final long NUM_MSGS=10000;
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
        CheckNoGaps check=new CheckNoGaps(1, this, mutex);
        ProtocolTester t=new ProtocolTester("pbcast.NAKACK", check);
        Address my_addr=new IpAddress("localhost", 10000);
        ViewId vid=new ViewId(my_addr, 322649);
        Vector mbrs=new Vector();
        View view;

        mbrs.addElement(my_addr);
        view=new View(vid, mbrs);

        t.start();
        t.getBottom().up(new Event(Event.SET_LOCAL_ADDRESS, my_addr));


        check.down(new Event(Event.BECOME_SERVER));
        check.down(new Event(Event.VIEW_CHANGE, view));

        for(long i=1; i <= NUM_MSGS; i++) {
            if(i % 1000 == 0)
                System.out.println("sending msg #" + i);
            check.down(new Event(Event.MSG, new Message(null, my_addr, new Long(i))));
            num_msgs_sent++;
        }

        synchronized(mutex) {
            while(!check.isDone())
                mutex.wait(WAIT_TIME);
        }
        System.out.println("\nMessages sent: " + num_msgs_sent + ", messages received: " + num_msgs_received);
        assertEquals(num_msgs_received, num_msgs_sent);
        t.stop();
    }


    public static Test suite() {
        return new TestSuite(NakackTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }


    private static class CheckNoGaps extends Protocol {
        long starting_seqno=1;
        Hashtable senders=new Hashtable(); // sender --> highest seqno received so far
        NakackTest t=null;
        final Object mut;
        long highest_seqno=starting_seqno;
        boolean done=false;



        CheckNoGaps(long seqno, NakackTest t, Object mut) {
            starting_seqno=seqno;
            this.t=t;
            this.mut=mut;
        }

        public String getName() {
            return "CheckNoGaps";
        }


        public boolean isDone() {
            return done;
        }

        public Object up(Event evt) {
            Message msg=null;
            Address sender;
            long received_seqno;
            Long s;

            if(evt == null)
                return null;

            if(evt.getType() == Event.SET_LOCAL_ADDRESS) {
                System.out.println("local address is " + evt.getArg());
            }

            if(evt.getType() != Event.MSG)
                return null;
            msg=(Message)evt.getArg();
            sender=msg.getSrc();
            if(sender == null) {
                log.error("NakackTest.CheckNoGaps.up(): sender is null; discarding msg");
                return null;
            }
            s=(Long)senders.get(sender);
            if(s == null) {
                s=new Long(starting_seqno);
                senders.put(sender, s);
            }

            try {
                s=(Long)msg.getObject();
                received_seqno=s.longValue();
                if(received_seqno == highest_seqno) {
                    // correct
                    if(received_seqno % 1000 == 0 && received_seqno > 0)
                        System.out.println("PASS: received msg #" + received_seqno);
                    senders.put(sender, new Long(highest_seqno));
                    highest_seqno++;

                    synchronized(mut) {
                        t.num_msgs_received++;
                        if(t.num_msgs_received >= NakackTest.NUM_MSGS) {
                            done=true;
                            mut.notifyAll();
                        }

                    }
                }
                else {
                    // error, terminate test
                    fail("FAIL: received msg #" + received_seqno + ", expected " + highest_seqno);
                }
            }
            catch(Exception ex) {
                log.error("NakackTest.CheckNoGaps.up()", ex);
            }
            return null;
        }


    }
}


