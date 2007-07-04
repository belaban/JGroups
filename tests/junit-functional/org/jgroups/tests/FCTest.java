// $Id: FCTest.java,v 1.1 2007/07/04 07:29:33 belaban Exp $

package org.jgroups.tests;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.debug.Simulator;
import org.jgroups.protocols.FC;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;

import java.util.Properties;
import java.util.Vector;


/**
 * Tests the flow control (FC) protocol
 * @author Bela Ban
 */
public class FCTest extends TestCase {
    IpAddress a1;
    Vector members;
    View v;
    Simulator s;

    final int SIZE=1000; // bytes
    final int NUM_MSGS=100000;


    public FCTest(String name) {
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
        FC fc=new FC();
        Properties props=new Properties();
        props.setProperty("max_credits", "10000");
        props.setProperty("min_credits", "1000");
        props.setProperty("max_block_time", "1000");
        fc.setProperties(props);
        Protocol[] stack=new Protocol[]{fc};
        s.setProtocolStack(stack);
        s.start();
    }

    public void tearDown() throws Exception {
        super.tearDown();
        s.stop();
    }


    public void testReceptionOfAllMessages() {
        int num_received=0;
        Receiver r=new Receiver();
        s.setReceiver(r);
        for(int i=1; i <= NUM_MSGS; i++) {
            Message msg=new Message(null, null, createPayload(SIZE));
            Event evt=new Event(Event.MSG, msg);
            s.send(evt);
            if(i % 1000 == 0)
                System.out.println("==> " + i);
        }
        int num_tries=10;
        while(num_tries > 0) {
            Util.sleep(1000);
            num_received=r.getNumberOfReceivedMessages();
            System.out.println("-- num received=" + num_received + ", stats:\n" + s.dumpStats());
            if(num_received >= NUM_MSGS)
                break;
            num_tries--;
        }
        assertEquals(num_received, NUM_MSGS);
    }



    private static byte[] createPayload(int size) {
        byte[] retval=new byte[size];
        for(int i=0; i < size; i++)
            retval[i]='0';
        return retval;
    }


    static class Receiver implements Simulator.Receiver {
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
    
   

    public static Test suite() {
        return new TestSuite(FCTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }
}
