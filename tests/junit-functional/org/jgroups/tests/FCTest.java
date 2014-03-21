
package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ReceiverAdapter;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;


/**
 * Tests the flow control (FC and MFC) protocols
 * @author Bela Ban
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class FCTest {
    JChannel         ch;
    static final int SIZE=1000; // bytes
    static final int NUM_MSGS=100000;
    static final int PRINT=NUM_MSGS / 10;


    @DataProvider
    static Object[][] configProvider() {
        return new Object[][]{
          {FC.class},
          {MFC.class}
        };
    }

    protected void setUp(Class<? extends Protocol> flow_control_class) throws Exception {
        Protocol flow_control_prot=flow_control_class.newInstance();
        flow_control_prot.setValue("min_credits", 1000).setValue("max_credits", 10000).setValue("max_block_time", 1000);

        ch=new JChannel(new SHARED_LOOPBACK().setValue("thread_pool_rejection_policy", "run"),
                        new SHARED_LOOPBACK_PING(),
                        new NAKACK2().setValue("use_mcast_xmit", false),
                        new UNICAST3(),
                        new STABLE().setValue("max_bytes", 50000),
                        new GMS().setValue("print_local_addr", false),
                        flow_control_prot,
                        new FRAG2().fragSize(800));
        ch.connect("FCTest");
    }

    @AfterMethod void tearDown() throws Exception {Util.close(ch);}


    @Test(dataProvider="configProvider")
    public void testReceptionOfAllMessages(Class<? extends Protocol> flow_control_class) throws Exception {
        int num_received=0;
        Receiver r=new Receiver();
        setUp(flow_control_class);
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
