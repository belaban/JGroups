package org.jgroups.protocols;

import org.jgroups.*;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.conf.ClassConfigurator;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.stack.Protocol;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

import static org.jgroups.Message.TransientFlag.DONT_LOOPBACK;

/**
 * Tests blocking in UFC / MFC (https://issues.redhat.com/browse/JGRP-1665)
 * @author Bela Ban
 * @since  3.4
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class FlowControlUnitTest {
    protected JChannel            a, b;
    protected RpcDispatcher       da, db;
    protected static final Method FORWARD, RECEIVE;
    protected static final int    MAX_CREDITS=10000;
    protected static final short  UFC_ID, MFC_ID;
    protected final LongAdder     received_msgs=new LongAdder();

    static {
        UFC_ID=ClassConfigurator.getProtocolId(UFC.class);
        MFC_ID=ClassConfigurator.getProtocolId(MFC.class);
        try {
            FORWARD=FlowControlUnitTest.class.getMethod("forward", Address.class, int.class);
            RECEIVE=FlowControlUnitTest.class.getMethod("receive", Address.class, byte[].class);
        }
        catch(NoSuchMethodException e) {
            throw new IllegalStateException(e);
        }
    }

    @BeforeMethod
    protected void setup() throws Exception {
        a=create("A");
        b=create("B");
        da=new RpcDispatcher(a, this);
        db=new RpcDispatcher(b, this);
        a.connect("FlowControlUnitTest");
        b.connect("FlowControlUnitTest");
        received_msgs.reset();
    }

    @AfterMethod protected void cleanup() {Util.close(b,a,db,da);}

    /**
     * First callback called by B (on A); this will call receive(byte[])
     * @param target The target to call. Null == multicast RPC
     * @param num_bytes The number of bytes to send
     */
    @Test(enabled=false)
    public void forward(Address target, int num_bytes) throws Exception {
        byte[] buffer=new byte[num_bytes];
        if(target != null) { // unicast
            Object retval=da.callRemoteMethod(target,new MethodCall(RECEIVE, a.getAddress(), buffer),RequestOptions.SYNC()
              .timeout(5000).flags(Message.Flag.OOB));
            System.out.println("retval=" + retval);
            int actual_bytes=(Integer)retval;
            assert actual_bytes == num_bytes : "expected " + Util.printBytes(num_bytes) + ", but call returned " + Util.printBytes(actual_bytes);
        }
        else {               // multicast
            RspList<Object> rsps=da.callRemoteMethods(null,new MethodCall(RECEIVE,a.getAddress(),buffer),RequestOptions.SYNC()
              .timeout(5000).flags(Message.Flag.OOB));
            System.out.println("rsps:\n" + rsps);
            assert rsps.size() == 2;
            for(Map.Entry<Address,Rsp<Object>> entry: rsps.entrySet()) {
                Rsp<Object> rsp=entry.getValue();
                assert rsp.wasReceived() : " rsp from " + entry.getKey() + " was not received";
                int actual_bytes=(Integer)rsp.getValue();
                assert actual_bytes == num_bytes : "expected " + Util.printBytes(num_bytes) + ", but call returned " + Util.printBytes(actual_bytes);
            }
        }
    }

    /**
     * Second callback, called from A.forward. This blocks if buffer.length > {UFC,MFC}.max_credits, until
     * JGRP-1665 has been fixed
     */
    @Test(enabled=false)
    public int receive(Address sender, byte[] buffer) {
        received_msgs.increment();
        System.out.printf("received %s from %s (num=%d)\n", Util.printBytes(buffer.length), sender, received_msgs.intValue());
        return buffer.length;
    }

    public void testUnicastBlocking() throws Exception {
        invoke(db, a.getAddress(), (int)(MAX_CREDITS * 1.2)); // 20% above max_credits
    }

    public void testMulticastBlocking() throws Exception {
        invoke(db, null, (int)(MAX_CREDITS * 1.2)); // 20% above max_credits
    }

    /**
     * Tests A sending async unicasts to itself; the credits should be adjusted accordingly
     */
    public void testUnicastToSelf() throws Exception {
        Address self=a.getAddress();
        UFC ufc=a.getProtocolStack().findProtocol(UFC.class);
        long before_sender_creds=ufc.getSenderCreditsFor(self), before_receiver_creds=ufc.getReceiverCreditsFor(self);
        callReceive(da, self, MAX_CREDITS / 2, RequestOptions.ASYNC());
        Util.sleep(1000);
        long after_sender_creds=ufc.getSenderCreditsFor(self), after_receiver_creds=ufc.getReceiverCreditsFor(self);
        System.out.printf("sender credits for %s before: %d, after: %d\nreceiver credits for %s before: %d, after: %s\n",
                          self, before_sender_creds, after_sender_creds, self, before_receiver_creds, after_receiver_creds);
        assert after_sender_creds <= before_sender_creds / 2;
        assert after_receiver_creds <= before_receiver_creds / 2;
    }

    /**
     * Tests A sending async unicasts to self with DONT_LOOPBACK; the credits should remain unchanged.
     */
    public void testUnicastToSelfWithDontLoopback() throws Exception {
        Address self=a.getAddress();
        UFC ufc=a.getProtocolStack().findProtocol(UFC.class);
        long before_sender_creds=ufc.getSenderCreditsFor(self), before_receiver_creds=ufc.getReceiverCreditsFor(self);
        callReceive(da, self, MAX_CREDITS / 2, RequestOptions.ASYNC().setTransientFlags(DONT_LOOPBACK));
        Util.sleep(1000);
        long after_sender_creds=ufc.getSenderCreditsFor(self), after_receiver_creds=ufc.getReceiverCreditsFor(self);
        System.out.printf("sender credits for %s before: %d, after: %d\nreceiver credits for %s before: %d, after: %s\n",
                          self, before_sender_creds, after_sender_creds, self, before_receiver_creds, after_receiver_creds);
        assert after_sender_creds == before_sender_creds;
        assert after_receiver_creds == before_receiver_creds;
    }

    protected static void callReceive(RpcDispatcher d, Address self, int bytes, RequestOptions opts) throws Exception {
        d.callRemoteMethod(self, new MethodCall(RECEIVE, self, new byte[bytes]), opts);
    }


    protected void invoke(RpcDispatcher disp, Address target, int num_bytes) throws Exception {
        // B invokes (blocking) A.forward
        disp.callRemoteMethod(a.getAddress(), new MethodCall(FORWARD, target, num_bytes), RequestOptions.SYNC().timeout(5000));
    }



    protected static JChannel create(String name) throws Exception {
        return new JChannel(new SHARED_LOOPBACK(),
                            new SHARED_LOOPBACK_PING(),
                            new NAKACK2(),
                            new UNICAST3(),
                            new STABLE(),
                            new GMS(),
                            new UFC().setMaxCredits(MAX_CREDITS).setMinThreshold(0.2),
                            new MFC().setMaxCredits(MAX_CREDITS).setMinThreshold(0.2),
                            new FRAG2().setFragSize(1500)).name(name);
    }



    protected static class DropCreditResponses extends Protocol {
        public Object up(Message msg) {
            FcHeader hdr=getHeader(msg, UFC_ID, MFC_ID);
            if(hdr != null && hdr.type == FcHeader.REPLENISH) {
                System.out.println("-- dropping credits from " + msg.getSrc());
                return null;
            }
            return up_prot.up(msg);
        }

        public void up(MessageBatch batch) {
            for(Iterator<Message> it=batch.iterator(); it.hasNext();) {
                Message msg=it.next();
                FcHeader hdr=getHeader(msg, UFC_ID, MFC_ID);
                if(hdr != null && hdr.type == FcHeader.REPLENISH) {
                    System.out.println("-- dropping credits from " + batch.sender());
                    it.remove();
                }
            }
            if(!batch.isEmpty())
                up_prot.up(batch);
        }

    }

    protected static <T extends Header> T getHeader(Message msg, short ... ids) {
        for(short id: ids) {
            Header hdr=msg.getHeader(id);
            if(hdr != null)
                return (T)hdr;
        }
        return null;
    }

}
