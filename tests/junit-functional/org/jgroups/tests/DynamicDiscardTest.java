package org.jgroups.tests;

import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.MessageDispatcher;
import org.jgroups.blocks.RequestHandler;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.protocols.*;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.ProtocolStack;
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
                                      new FD().setValue("timeout", 1000).setValue("max_tries", 1),
                                      new NAKACK2(),
                                      new UNICAST3(),
                                      new STABLE(),
                                      new GMS(),
                                      new RSVP().setValue("ack_on_delivery", false)
                                        .setValue("throw_exception_on_timeout", false));
            channels[i].setName(Character.toString((char) ('A' + i)));
            channels[i].setDiscardOwnMessages(true);
            dispatchers[i]=new MessageDispatcher(channels[i], null, null, new MyRequestHandler());
            channels[i].connect("DynamicDiscardTest");
            System.out.print(i + 1 + " ");
        }
        Util.waitUntilAllChannelsHaveSameSize(10000, 1000, channels);

        // discard all messages (except those to self)
        DISCARD discard = new DISCARD();
        channels[0].getProtocolStack().insertProtocol(discard, ProtocolStack.ABOVE, TP.class);
        discard.setDiscardAll(true);

        // send a RSVP message
        Message msg = new Message(null, "message2");
        msg.setFlag(Message.Flag.RSVP, Message.Flag.OOB);
        RspList<Object> rsps = dispatchers[0].castMessage(null, msg, RequestOptions.SYNC().setTimeout(5000));

        Rsp<Object> objectRsp = rsps.get(channels[1].getAddress());
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
}
