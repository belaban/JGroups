package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.*;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests thata there aren't unnecessary retransmissions caused by the retransmit task in NAKACK<p/>
 * https://issues.jboss.org/browse/JGRP-1539
 * @author Bela Ban
 * @since 3.3
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class NAKACK2_RetransmissionTest {
    protected static final short ID=ClassConfigurator.getProtocolId(NAKACK2.class);
    protected static final Address A=Util.createRandomAddress("A"), B=Util.createRandomAddress("B");
    protected static final View    view=View.create(A, 1, A, B);
    protected NAKACK2       nak;
    protected MockTransport transport;
    protected MockProtocol  receiver;

    @BeforeMethod
    protected void setup() throws Exception {
        receiver=new MockProtocol();
        nak=(NAKACK2)new NAKACK2().setValue("use_mcast_xmit", false);
        transport=new MockTransport();
        ProtocolStack stack=new ProtocolStack();
        stack.addProtocols(transport, nak, receiver);
        stack.init();

        nak.down(new Event(Event.BECOME_SERVER));
        nak.down(new Event(Event.SET_LOCAL_ADDRESS, A));
        Digest digest=new Digest(view.getMembersRaw(), new long[]{0, 0, 0, 0});
        nak.down(new Event(Event.SET_DIGEST, digest));
    }


    /**
     * - Send a few messages such that missing messages are 5,7, 9-13, 18
     * - Kick off the retransmission task: no retransmit messages must be seen
     * - Send some missing messages and create a few more gaps, such that the missing messages are
     *   5, 10-12, 20-22, 25, 30
     * - Trigger a retransmit task run
     * - The retransmit requests need to be: 5, 10-12
     * - Trigger a next run of the retransmit task. The retransmit messages need to be 5, 10-12, 20-22, 25, 30
     * - Make NAKACK2 receive missing messages 5, 10-12
     * - Kick off another retransmission run: the missing messages are 20-22, 25, 30
     *
     *
     * - Receive all missing messages
     * - On the last run of the retransmit task, no messages are retransmitted
     */
    public void testRetransmission() {
        injectMessages(1,2,3,4,   6,  8,  14,15,16,17,   19);
        assertReceived(1,2,3,4);  // only messages 1-4 are delivered, there's a gap at 5

        nak.triggerXmit();
        // assertXmitRequests(5, 7,  9,10,11,12,13,   18);
        assertXmitRequests(); // the first time, there will *not* be any retransmit requests !

        injectMessages(7,  9,  13,  18,  23, 24,  26, 27, 28, 29,   31);
        nak.triggerXmit();
        assertXmitRequests(5,   10,11,12);

        nak.triggerXmit();
        assertXmitRequests(5,   10,11,12,   20,21,22,  25,  30);

        injectMessages(5,  10,11,12);
        nak.triggerXmit();
        assertXmitRequests(20,21,22,  25, 30);

        injectMessages(20,21,22,  25,  30);
        nak.triggerXmit();
        assertXmitRequests();
    }


    protected void injectMessages(long ... seqnos) {
        for(long seqno: seqnos)
            injectMessage(seqno);
    }


    /** Makes NAKACK2 receive a message with the given seqno */
    protected void injectMessage(long seqno) {
        Message msg=new Message(null, B, null);
        NakAckHeader2 hdr=NakAckHeader2.createMessageHeader(seqno);
        msg.putHeader(ID, hdr);
        nak.up(new Event(Event.MSG, msg));
    }

    /** Asserts that the delivered messages are in the same order than the expected seqnos and then clears the list */
    protected void assertReceived(long ... seqnos) {
        List<Long> msgs=receiver.getMsgs();
        assert msgs.size() == seqnos.length : "expected=" + Arrays.toString(seqnos) + ", received=" + msgs;
        for(int i=0; i < seqnos.length; i++)
            assert seqnos[i] == msgs.get(i) : "expected=" + Arrays.toString(seqnos) + ", received=" + msgs;
        msgs.clear();
    }

    /** Asserts that the expected retransmission requests match the actual ones */
    protected void assertXmitRequests(long ... expected_seqnos) {
        List<Long> actual_xmit_reqs=transport.getXmitRequests();
        assert actual_xmit_reqs.size() == expected_seqnos.length
          : "size mismatch: expected=" + Arrays.toString(expected_seqnos) + ", received=" + actual_xmit_reqs;
        for(int i=0; i < expected_seqnos.length; i++) {
            assert expected_seqnos[i] == actual_xmit_reqs.get(i)
              : "expected=" + Arrays.toString(expected_seqnos) + ", received=" + actual_xmit_reqs;
        }
        actual_xmit_reqs.clear();
    }


    /** Used to catch retransmit requests sent by NAKACK to the transport */
    protected static class MockTransport extends TP {
        protected final List<Long> xmit_requests=new LinkedList<>();

        public List<Long>         getXmitRequests() {return xmit_requests;}
        public void               clear() {xmit_requests.clear();}
        public void               init() throws Exception {}
        public boolean            supportsMulticasting() {return true;}
        public void               sendMulticast(AsciiString cluster_name, byte[] data, int offset, int length) throws Exception {}
        public void               sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
        public String             getInfo() {return null;}
        protected PhysicalAddress getPhysicalAddress() {return null;}

        public Object down(Event evt) {
            switch(evt.getType()) {
                case Event.MSG:
                    Message msg=(Message)evt.getArg();
                    NakAckHeader2 hdr=(NakAckHeader2)msg.getHeader(ID);
                    if(hdr == null)
                        break;
                    if(hdr.getType() == NakAckHeader2.XMIT_REQ) {
                        SeqnoList seqnos=null;
                        try {
                            seqnos=Util.streamableFromBuffer(SeqnoList.class, msg.getRawBuffer(), msg.getOffset(), msg.getLength());
                            System.out.println("-- XMIT-REQ: request retransmission for " + seqnos);
                            for(Long seqno: seqnos)
                                xmit_requests.add(seqno);
                        }
                        catch(Exception e) {
                            e.printStackTrace();
                        }
                    }
                    break;
            }
            return null;
        }


    }

    protected static class MockProtocol extends Protocol {
        protected final List<Long> msgs=new LinkedList<>();

        public List<Long> getMsgs() {return msgs;}
        public void       clear()   {msgs.clear();}

        public Object up(Event evt) {
            switch(evt.getType()) {
                case Event.MSG:
                    Message msg=(Message)evt.getArg();
                    NakAckHeader2 hdr=(NakAckHeader2)msg.getHeader(ID);
                    if(hdr != null && hdr.getType() == NakAckHeader2.MSG) {
                        long seqno=hdr.getSeqno();
                        msgs.add(seqno);
                        System.out.println("-- received message #" + seqno + " from " + msg.getSrc());
                    }
                    break;
            }
            return null;
        }

        public void up(MessageBatch batch) {
            for(Message msg: batch) {
                NakAckHeader2 hdr=(NakAckHeader2)msg.getHeader(ID);
                if(hdr != null && hdr.getType() == NakAckHeader2.MSG) {
                    long seqno=hdr.getSeqno();
                    msgs.add(seqno);
                    System.out.println("-- received message #" + seqno + " from " + msg.getSrc());
                }
            }
        }
    }

}
