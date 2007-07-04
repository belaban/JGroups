package org.jgroups.tests;

import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.debug.Simulator;
import org.jgroups.protocols.BARRIER;
import org.jgroups.protocols.PING;
import org.jgroups.protocols.VIEW_SYNC;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.Vector;

/**
 * Tests the BARRIER protocol
 * @author Bela Ban
 * @version $Id: BARRIERTest.java,v 1.1 2007/07/04 07:29:33 belaban Exp $
 */
public class BARRIERTest extends TestCase {
    IpAddress a1;
    Vector members;
    View v;
    Simulator s;
    BARRIER barrier_prot=new BARRIER();
    PING bottom_prot;


    public BARRIERTest(String name) {
        super(name);
    }


    public void setUp() throws Exception {
        super.setUp();
        a1=new IpAddress(1111);
        members=new Vector();
        members.add(a1);
        v=new View(a1, 1, members);
        s=new Simulator();
        s.setLocalAddress(a1);
        s.setView(v);
        s.addMember(a1);
        bottom_prot=new PING();
        Protocol[] stack=new Protocol[]{new VIEW_SYNC(), barrier_prot, bottom_prot};
        s.setProtocolStack(stack);
        s.start();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        s.stop();
    }


    public void testBlocking() {
        assertFalse(barrier_prot.isClosed());
        s.send(new Event(Event.CLOSE_BARRIER));
        assertTrue(barrier_prot.isClosed());
        s.send(new Event(Event.OPEN_BARRIER));
        assertFalse(barrier_prot.isClosed());
    }


    public void testThreadsBlockedOnBarrier() {
        MyReceiver receiver=new MyReceiver();
        s.setReceiver(receiver);
        s.send(new Event(Event.CLOSE_BARRIER));
        for(int i=0; i < 5; i++) {
            new Thread() {
                public void run() {
                    bottom_prot.up(new Event(Event.MSG, new Message(null, null, null)));
                }
            }.start();
        }

        Util.sleep(500);
        int num_in_flight_threads=barrier_prot.getNumberOfInFlightThreads();
        assertEquals(5, num_in_flight_threads);

        s.send(new Event(Event.OPEN_BARRIER));
        Util.sleep(500);
        num_in_flight_threads=barrier_prot.getNumberOfInFlightThreads();
        assertEquals(0, num_in_flight_threads);
        assertEquals(5, receiver.getNumberOfReceivedMessages());
    }





    static class MyReceiver implements Simulator.Receiver {
        int num_mgs_received=0;

        public void receive(Event evt) {
            if(evt.getType() == Event.MSG) {
                num_mgs_received++;
                if(num_mgs_received % 1000 == 0)
                    System.out.println("<== " + num_mgs_received);
            }
        }

        public int getNumberOfReceivedMessages() {
            return num_mgs_received;
        }
    }



    public static junit.framework.Test suite() {
        return new TestSuite(BARRIERTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
