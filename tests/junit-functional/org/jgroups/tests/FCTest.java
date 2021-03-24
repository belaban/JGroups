
package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
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
    static final int NUM_MSGS=100_000;
    static final int PRINT=NUM_MSGS / 10;


    @DataProvider
    static Object[][] configProvider() {
        return new Object[][] {
          {MFC.class}
        };
    }

    protected void setUp(Class<? extends FlowControl> flow_control_class) throws Exception {
        FlowControl f=flow_control_class.getDeclaredConstructor().newInstance();
        f.setMinCredits(1000).setMaxCredits(10000).setMaxBlockTime(1000);

        ch=new JChannel(new SHARED_LOOPBACK(),
                        new SHARED_LOOPBACK_PING(),
                        new NAKACK2().useMcastXmit(false),
                        new UNICAST3(),
                        new STABLE().setMaxBytes(50000),
                        new GMS().printLocalAddress(false),
                        f,
                        new FRAG2().setFragSize(800));
        ch.connect("FCTest");
    }

    @AfterMethod void tearDown() throws Exception {Util.close(ch);}


    @Test(dataProvider="configProvider")
    public void testReceptionOfAllMessages(Class<? extends FlowControl> flow_control_class) throws Exception {
        MyReceiver r=new MyReceiver();
        setUp(flow_control_class);
        ch.setReceiver(r);
        for(int i=1; i <= NUM_MSGS; i++) {
            Message msg=new BytesMessage(null, createPayload(SIZE));
            ch.send(msg);
            if(i % PRINT == 0)
                System.out.println("==> " + i);
        }
        Util.waitUntil(10000, 1000, () -> r.getNumberOfReceivedMessages() >= NUM_MSGS,
                       () -> String.format("expected %d messages, but got %d", NUM_MSGS, r.getNumberOfReceivedMessages()));
    }





    private static byte[] createPayload(int size) {
        byte[] retval=new byte[size];
        for(int i=0; i < size; i++)
            retval[i]='0';
        return retval;
    }


    protected static class MyReceiver implements Receiver {
        protected int num_mgs_received;

        public synchronized void receive(Message msg) {
            num_mgs_received++;
            if(num_mgs_received % PRINT == 0)
                System.out.println("<== " + num_mgs_received);
        }

        public synchronized int getNumberOfReceivedMessages() {
            return num_mgs_received;
        }
    }
    
   

}
