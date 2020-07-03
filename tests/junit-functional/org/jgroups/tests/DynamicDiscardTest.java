package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.ByteArray;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Test the VERIFY_SUSPECT protocol combined with FD and DISCARD.
 *
 * @author Dan Berindei
 * @since 3.3
 */
@Test(groups=Global.FUNCTIONAL)
public class DynamicDiscardTest {
    private static final int NUM = 2;

    public void testLeaveDuringSend() throws Exception {
        final JChannel[]          channels = new JChannel[NUM];
        final MessageDispatcher[] dispatchers = new MessageDispatcher[NUM];

        for(int i=0; i < NUM; i++) {
            channels[i]= new JChannel(new SHARED_LOOPBACK(),
                                      new SHARED_LOOPBACK_PING(),
                                      new MERGE3(),
                                      new FD_ALL3().setTimeout(1000).setInterval(300),
                                      new NAKACK2(),
                                      new UNICAST3(),
                                      new STABLE(),
                                      new GMS(),
                                      new RSVP().ackOnDelivery(false).throwExceptionOnTimeout(false));
            channels[i].setName(Character.toString((char) ('A' + i)));
            channels[i].setDiscardOwnMessages(true);
            dispatchers[i]=new MessageDispatcher(channels[i], new MyRequestHandler());
            dispatchers[i].setReceiver(new MyMembershipListener(channels[i]));
            channels[i].connect("DynamicDiscardTest");
            System.out.print(i + 1 + " ");
        }
        Util.waitUntilAllChannelsHaveSameView(10000, 1000, channels);

        // discard all messages (except those to self)
        DISCARD discard = new DISCARD().discardAll(true);
        channels[0].getProtocolStack().insertProtocol(discard, ProtocolStack.Position.ABOVE, TP.class);

        // send a RSVP message
        byte[] data="message2".getBytes();
        ByteArray buf=new ByteArray(data, 0, data.length);
        RspList<Object> rsps=dispatchers[0].castMessage(null, new BytesMessage(null, buf),
                                                        RequestOptions.SYNC().timeout(5000)
          .flags(Message.Flag.RSVP, Message.Flag.OOB));
        Rsp<Object> objectRsp=rsps.get(channels[1].getAddress());
        assertFalse(objectRsp.wasReceived());
        assertTrue(objectRsp.wasSuspected());
    }

    private static class MyRequestHandler implements RequestHandler {
        @Override
        public Object handle(Message msg) throws Exception {
            System.out.println(String.format("Received message %s", msg));
            return "bla";
        }
    }

    protected static class MyMembershipListener implements Receiver {
        protected final JChannel ch;

        public MyMembershipListener(JChannel ch) {
            this.ch=ch;
        }

        public void viewAccepted(View view) {
            System.out.printf("[%s] view: %s\n", ch.getAddress(), view);
        }
    }
}
