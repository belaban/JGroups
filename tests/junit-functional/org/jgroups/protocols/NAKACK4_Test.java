package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.util.MyReceiver;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * Tests sender blocked on spurious ACK by non-member (https://issues.redhat.com/browse/JGRP-2904)
 * @author Bela Ban
 * @since  5.5.0
 */
@Test(groups=Global.FUNCTIONAL)
public class NAKACK4_Test {
    protected JChannel[] channels;

    @BeforeMethod
    protected void setup() throws Exception {
        channels=new JChannel[4];
        for(int i=0; i < channels.length; i++)
            channels[i]=create(String.valueOf((char)('A' +i))).connect(NAKACK4_Test.class.getSimpleName());
        Util.waitUntilAllChannelsHaveSameView(3000, 100, channels);
    }

    @AfterMethod
    protected void destroy() {
        Util.closeReverse(channels);
    }

    public void testSenderBlocked() throws Exception {
        JChannel coord=channels[0];
        NAKACK4 nak=coord.stack().findProtocol(NAKACK4.class);
        // every node sends a few messsages:
        for(JChannel ch: channels) {
            for(int i=1; i <= 1000; i++)
                ch.send(null, i);
        }
        Address b=channels[1].address();
        for(int i=1; i < channels.length; i++)
            Util.close(channels[i]);
        Util.waitUntil(5000, 100, () -> coord.view().size() == 1);

        // B sends a spurious ACK; this blocks coord when sending messages: https://issues.redhat.com/browse/JGRP-2904
        nak.handleAck(b, 1005);
        MyReceiver<Integer> r=new MyReceiver<>();
        coord.setReceiver(r);
        int capacity=nak.capacity();
        Thread t=coord.stack().getTransport().getThreadFactory().newThread(() -> {
            for(int i=1; i <= capacity+100; i++) {
                try {
                    coord.send(null, i);
                }
                catch(Exception e) {
                    throw new RuntimeException(e);
                }
            }
        });
        t.start();

        Util.waitUntil(5000, 500, () -> r.size() == capacity+100);
        System.out.printf("-- coord received %,d messages\n", capacity+100);
    }

    protected static JChannel create(String name) throws Exception {
        JChannel ch=new JChannel(Util.getTestStackNew()).name(name);
        NAKACK4 nak=ch.stack().findProtocol(NAKACK4.class);
        nak.ackThreshold(1).capacity(512);
        return ch;
    }
}
