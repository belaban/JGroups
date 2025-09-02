package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.NakAckHeader2;
import org.jgroups.stack.Protocol;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Digest;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.SeqnoList;
import org.jgroups.util.Util;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * Tests that there aren't unnecessary retransmissions caused by the retransmit task in NAKACK{2,4}<p>
 * https://issues.redhat.com/browse/JGRP-1539
 * @author Bela Ban
 * @since 3.3
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true,dataProvider="create")
public class NAKACK_RetransmissionTest {
    protected static final short   NAK2=ClassConfigurator.getProtocolId(NAKACK2.class),
      NAK4=ClassConfigurator.getProtocolId(NAKACK4.class);
    protected static final Address A=Util.createRandomAddress("A"), B=Util.createRandomAddress("B");
    protected static final View    view=View.create(A, 1, A, B);
    protected Protocol      nak;
    protected MockTransport transport;
    protected MockProtocol  receiver;

    @DataProvider
    protected static Object[][] create() {
        return new Object[][] {
          {new NAKACK2().useMcastXmit(false)},
          {new NAKACK4().useMcastXmit(false)}
        };
    }

    protected void setup(Protocol prot) throws Exception {
        receiver=new MockProtocol();
        transport=new MockTransport();
        nak=prot;
        ProtocolStack stack=new ProtocolStack();
        stack.addProtocols(transport, prot, receiver);
        stack.init();

        nak.down(new Event(Event.BECOME_SERVER));
        for(Protocol p=nak; p != null; p=p.getDownProtocol())
            p.setAddress(A);
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
     * - Receive all missing messages
     * - On the last run of the retransmit task, no messages are retransmitted
     */
    public void testRetransmission(Protocol prot) throws Exception {
        setup(prot);
        Method triggerXmit=prot.getClass().getMethod("triggerXmit");

        injectMessages(prot.getClass(), 1,2,3,4,   6,  8,  14,15,16,17,   19);
        assertReceived(1,2,3,4);  // only messages 1-4 are delivered, there's a gap at 5

        triggerXmit.invoke(nak);
        // assertXmitRequests(5, 7,  9,10,11,12,13,   18);
        assertXmitRequests(); // the first time, there will *not* be any retransmit requests !

        injectMessages(prot.getClass(),7,  9,  13,  18,  23, 24,  26, 27, 28, 29,   31);
        triggerXmit.invoke(nak);
        assertXmitRequests(5,   10,11,12);

        triggerXmit.invoke(nak);
        assertXmitRequests(5,   10,11,12,   20,21,22,  25,  30);

        injectMessages(prot.getClass(),5,  10,11,12);
        triggerXmit.invoke(nak);
        assertXmitRequests(20,21,22,  25, 30);

        injectMessages(prot.getClass(),20,21,22,  25,  30);
        triggerXmit.invoke(nak);
        assertXmitRequests();
    }


    protected void injectMessages(Class<? extends Protocol> cl, long ... seqnos) {
        for(long seqno: seqnos)
            injectMessage(seqno, cl);
    }


    /** Makes NAKACK2 receive a message with the given seqno */
    protected void injectMessage(long seqno, Class<? extends Protocol> cl) {
        Message msg=new ObjectMessage(null, seqno).setSrc(B);
        if(cl.equals(NAKACK2.class)) {
            NakAckHeader2 hdr2=NakAckHeader2.createMessageHeader(seqno);
            msg.putHeader(NAK2, hdr2);
        }
        else if(cl.equals(NAKACK4.class)) {
            NakAckHeader hdr4=NakAckHeader.createMessageHeader(seqno);
            msg.putHeader(NAK4, hdr4);
        }
        nak.up(msg);
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
        public void               init() throws Exception {
            super.init();
            diag_handler=createDiagnosticsHandler();
            bundler=new NoBundler();
        }
        public boolean            supportsMulticasting() {return true;}
        public void               sendUnicast(PhysicalAddress dest, byte[] data, int offset, int length) throws Exception {}
        public String             getInfo() {return null;}
        protected PhysicalAddress getPhysicalAddress() {return null;}


        public Object down(Message msg) {
            SeqnoList seqnos=getMissingSeqnos(msg);
            if(seqnos != null) {
                System.out.println("-- XMIT-REQ: request retransmission for " + seqnos);
                for(Long seqno: seqnos)
                    xmit_requests.add(seqno);
            }
            return null;
        }

        @Override
        public Object up(Event evt) {
            return null;
        }

        protected static SeqnoList getMissingSeqnos(Message msg) {
            NakAckHeader2 hdr2=msg.getHeader(NAK2);
            if(hdr2 != null && hdr2.getType() == NakAckHeader2.XMIT_REQ)
                return msg.getObject();
            NakAckHeader hdr4=msg.getHeader(NAK4);
            if(hdr4 != null && hdr4.getType() == NakAckHeader.XMIT_REQ)
                return msg.getObject();
            return null;
        }
    }

    protected static class MockProtocol extends Protocol {
        protected final List<Long> msgs=new LinkedList<>();

        public List<Long> getMsgs() {return msgs;}
        public void       clear()   {msgs.clear();}

        public Object up(Message msg) {
            long seqno=getSeqno(msg);
            if(seqno >= 0) {
                msgs.add(seqno);
                System.out.println("-- received message #" + seqno + " from " + msg.getSrc());
            }
            return null;
        }

        public void up(MessageBatch batch) {
            for(Message msg: batch) {
                long seqno=getSeqno(msg);
                if(seqno >= 0) {
                    msgs.add(seqno);
                    System.out.println("-- received message #" + seqno + " from " + msg.getSrc());
                }
            }
        }

        protected static long getSeqno(Message msg) {
            NakAckHeader2 hdr2=msg.getHeader(NAK2);
            if(hdr2 != null && hdr2.getType() == NakAckHeader2.MSG)
                return hdr2.getSeqno();
            NakAckHeader hdr4=msg.getHeader(NAK4);
            if(hdr4 != null && hdr4.getType() == NakAckHeader.MSG)
                return hdr4.getSeqno();
            return -1;
        }

    }

}
