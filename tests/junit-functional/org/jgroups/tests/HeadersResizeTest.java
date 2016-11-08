package org.jgroups.tests;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.SenderSendsBundler;
import org.jgroups.protocols.TP;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static org.jgroups.Message.Flag.OOB;

/**
 * Tests unnecessary resizing of headers (https://issues.jboss.org/browse/JGRP-2120)
 * @author Bela Ban
 * @since  4.0
 */
@Test(groups=Global.FUNCTIONAL)
public class HeadersResizeTest {
    protected JChannel           a,b;
    protected static final short transport_id;

    static {
        transport_id=ClassConfigurator.getProtocolId(TP.class);
    }

    @BeforeMethod protected void setup() throws Exception {
        a=new JChannel(Util.getTestStack()).name("A");
        a.connect(HeadersResizeTest.class.getSimpleName());
        b=new JChannel(Util.getTestStack()).name("B");
        b.connect(HeadersResizeTest.class.getSimpleName());
        Util.waitUntilAllChannelsHaveSameView(10000, 500, a,b);
    }

    @AfterMethod protected void tearDown() {Util.close(b, a);}

    public void testResizing() throws Exception {
        BatchingBundler bundler=new BatchingBundler();
        TP transport=a.getProtocolStack().getTransport();
        bundler.init(transport);
        a.getProtocolStack().getTransport().setBundler(bundler);

        MyReceiver receiver=new MyReceiver();
        b.setReceiver(receiver);

        Address dest=b.getAddress();
        bundler.hold();
        for(int i=1; i <= 5; i++) { // these 5 messages will be queued by the bundler
            Message msg=new Message(dest, i).setFlag(OOB, Message.Flag.DONT_BUNDLE);
            a.send(msg);
        }
        bundler.release(); // sends all bundled messages as a batch

        for(int i=0; i < 10; i++) {
            if(receiver.num_msgs >= 5)
                break;
            Util.sleep(200);
        }
        System.out.printf("Number of transport headers: %d\n", receiver.num_transport_headers);
        assert receiver.num_transport_headers == 0;
    }


    protected static class BatchingBundler extends SenderSendsBundler {
        protected boolean             queue;
        protected final List<Message> list=new ArrayList<>(16);

        public synchronized void send(Message msg) throws Exception {
            if(!queue)
                super.send(msg);
            else
                list.add(msg);
        }

        protected synchronized void hold() { // messages will not be sent from now on (they're queued)
            queue=true;
        }

        protected synchronized void release() { // the next send() will send all queued messages
            for(Message msg: list)
                addMessage(msg, msg.size());
            list.clear();
            queue=false;
            sendBundledMessages();
        }
    }

    protected static class MyReceiver extends ReceiverAdapter {
        protected int num_msgs, num_transport_headers;

        public void receive(Message msg) {
            System.out.printf("received message from %s: %s\n", msg.src(), msg.getObject());
            num_msgs++;
            Header hdr=msg.getHeader(transport_id);
            if(hdr != null)
                num_transport_headers++;
        }
    }
}
