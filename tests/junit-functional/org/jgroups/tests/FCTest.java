
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * Tests the flow control (FC) protocol
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,sequential=true)
public class FCTest {
    JChannel ch;
    static final int SIZE=1000; // bytes
    static final int NUM_MSGS=100000;
    static final int PRINT=NUM_MSGS / 10;



    @BeforeMethod
    void setUp() throws Exception {
        ch=Util.createChannel(new SHARED_LOOPBACK().setValue("thread_pool_rejection_policy", "run").setValue("loopback", true),
                              new PING(),
                              new NAKACK().setValue("use_mcast_xmit", false),
                              new UNICAST2(),
                              new STABLE().setValue("max_bytes", 50000),
                              new GMS().setValue("print_local_addr", false),
                              new FC().setValue("min_credits", 1000).setValue("max_credits", 10000).setValue("max_block_time", 1000),
                              new FRAG2());
        ch.connect("FCTest");
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(ch);
    }


    public void testReceptionOfAllMessages() throws Exception {
        int num_received=0;
        Receiver r=new Receiver();
        ch.setReceiver(r);
        for(int i=1; i <= NUM_MSGS; i++) {
            Message msg=new Message(null, null, createPayload(SIZE));
            ch.send(msg);
            if(i % PRINT == 0)
                System.out.println("==> " + i);
        }
        int num_tries=10;
        while(num_tries > 0) {
            Util.sleep(1000);
            num_received=r.getNumberOfReceivedMessages();
            System.out.println("-- num received=" + num_received);
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


    static class Receiver extends ReceiverAdapter {
        int num_mgs_received=0;

        public void receive(Message msg) {
            num_mgs_received++;
            if(num_mgs_received % PRINT == 0)
                System.out.println("<== " + num_mgs_received);
        }

        public int getNumberOfReceivedMessages() {
            return num_mgs_received;
        }
    }
    
   

}
