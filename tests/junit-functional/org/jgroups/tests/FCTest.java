// $Id: FCTest.java,v 1.2 2008/03/10 15:39:22 belaban Exp $

package org.jgroups.tests;

import org.jgroups.Event;
import org.jgroups.Message;
import org.jgroups.View;
import org.jgroups.Global;
import org.jgroups.debug.Simulator;
import org.jgroups.protocols.FC;
import org.jgroups.stack.IpAddress;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Properties;
import java.util.Vector;


/**
 * Tests the flow control (FC) protocol
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class FCTest {
    Simulator s=null;
    static final int SIZE=1000; // bytes
    static final int NUM_MSGS=100000;
    static final int PRINT=NUM_MSGS / 10;



    @BeforeMethod
    void setUp() throws Exception {
        IpAddress a1=new IpAddress(1111);
        Vector members=new Vector();
        members.add(a1);
        View v=new View(a1, 1, members);
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

    @AfterMethod
    void tearDown() throws Exception {
        s.stop();
    }


    @Test(groups=Global.FUNCTIONAL)
    public void testReceptionOfAllMessages() {
        int num_received=0;
        Receiver r=new Receiver();
        s.setReceiver(r);
        for(int i=1; i <= NUM_MSGS; i++) {
            Message msg=new Message(null, null, createPayload(SIZE));
            Event evt=new Event(Event.MSG, msg);
            s.send(evt);
            if(i % PRINT == 0)
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
        assert num_received == NUM_MSGS;
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
                if(num_mgs_received % PRINT == 0)
                    System.out.println("<== " + num_mgs_received);
            }
        }

        public int getNumberOfReceivedMessages() {
            return num_mgs_received;
        }
    }
    
   

}
