// $Id: UNICASTTest.java,v 1.3 2005/08/22 14:47:48 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.debug.Simulator;
import org.jgroups.protocols.UNICAST;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.Properties;
import java.util.Vector;
import java.nio.ByteBuffer;


/**
 * Tests the UNICAST protocol
 * @author Bela Ban
 */
public class UNICASTTest extends TestCase {
    IpAddress a1, a2;
    Vector members;
    View v;
    Simulator s;

    final int SIZE=1000; // bytes
    final int NUM_MSGS=10000;


    public UNICASTTest(String name) {
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
        UNICAST unicast=new UNICAST();
        Properties props=new Properties();
        props.setProperty("timeout", "100,200,400,800,1000,2000,3000");
        unicast.setProperties(props);
        Protocol[] stack=new Protocol[]{unicast};
        s.setProtocolStack(stack);
        s.start();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        s.stop();
    }


    public void testReceptionOfAllMessages() throws Throwable {
        int num_received=0;
        Receiver r=new Receiver();
        s.setReceiver(r);
        for(int i=1; i <= NUM_MSGS; i++) {
            Message msg=new Message(a1, null, createPayload(SIZE, i)); // unicast message
            Event evt=new Event(Event.MSG, msg);
            s.send(evt);
            if(i % 1000 == 0)
                System.out.println("==> " + i);
        }
        int num_tries=10;
        while((num_received=r.getNumberOfReceivedMessages()) != NUM_MSGS && num_tries > 0) {
            if(r.getException() != null)
            throw r.getException();
            Util.sleep(3000);
            // System.out.println("-- num received=" + num_received);
            num_tries--;
        }
        System.out.println("-- num received=" + num_received + ", stats:\n" + s.dumpStats());
        assertTrue(num_received == NUM_MSGS);
    }



    private static byte[] createPayload(int size, int seqno) {
        ByteBuffer buf=ByteBuffer.allocate(size);
        buf.putInt(seqno);
        return buf.array();
    }


    /** Checks that messages 1 - NUM_MSGS are received in order */
    class Receiver implements Simulator.Receiver {
        int num_mgs_received=0, next=1;
        Throwable exception=null;

        public void receive(Event evt) {
            if(evt.getType() == Event.MSG) {
                if(exception != null)
                return;
                Message msg=(Message)evt.getArg();
                ByteBuffer buf=ByteBuffer.wrap(msg.getRawBuffer());
                int seqno=buf.getInt();
                if(seqno != next) {
                    exception=new Exception("expected seqno was " + next + ", but received " + seqno);
                    return;
                }
                next++;
                num_mgs_received++;
                if(num_mgs_received % 1000 == 0)
                    System.out.println("<== " + num_mgs_received);
            }
        }

        public int getNumberOfReceivedMessages() {
            return num_mgs_received;
        }

        public Throwable getException() {
            return exception;
        }
    }



    public static Test suite() {
        TestSuite s=new TestSuite(UNICASTTest.class);
        return s;
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
