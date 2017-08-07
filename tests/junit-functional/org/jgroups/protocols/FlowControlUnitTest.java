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
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.MessageBatch;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tests blocking in UFC / MFC (https://issues.jboss.org/browse/JGRP-1665)
 * @author Bela Ban
 * @since  3.4
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class FlowControlUnitTest {
    protected JChannel            a, b;
    protected RpcDispatcher       da, db;
    protected static final Method FORWARD, RECEIVE;
    protected static final int    MAX_CREDITS=10000;
    protected static final short  UFC_ID, UFC_NB_ID, MFC_ID, MFC_NB_ID;
    protected final LongAdder     received_msgs=new LongAdder();

    static {
        UFC_ID=ClassConfigurator.getProtocolId(UFC.class);
        UFC_NB_ID=ClassConfigurator.getProtocolId(UFC_NB.class);
        MFC_ID=ClassConfigurator.getProtocolId(MFC.class);
        MFC_NB_ID=ClassConfigurator.getProtocolId(MFC_NB.class);
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
     * @param buffer
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
     * A invokes 15 async RPCs of 1000 bytes to B, but drops credits it gets from B. With {@link UFC}, the caller
     * would block, but with {@link UFC_NB}, all RPCs return successfully as the messages with insufficient credits
     * are queued by UFC_NB.<br/>
     * Next, the droping of requests is stopped and retransmission will deliver credits at A, so the queued messages
     * are drained and sent to B. The test finally confirms that B indeed received 15 RPCs.
     */
    public void testNonBlockingFlowControlUnicast() throws Exception {
        DropCreditResponses drop_credits=new DropCreditResponses();
        // Prevent credit replenishments from being received in A. At this point, A will block sending messages to B
        // after it runs out of credits
        a.getProtocolStack().insertProtocol(drop_credits, ProtocolStack.Position.ABOVE, SHARED_LOOPBACK.class);
        replaceUFC(60_000, a,b);

        final byte[] buf=new byte[1000];
        Address local=a.getAddress(), target=b.getAddress();

        for(int i=1; i <= 15; i++)
            da.callRemoteMethod(target, new MethodCall(RECEIVE, local, buf), RequestOptions.ASYNC());

        UFC_NB ufc_nb=a.getProtocolStack().findProtocol(UFC_NB.class);
        System.out.printf("A's sender credits: %s\n", ufc_nb.printCredits());
        assert ufc_nb.isQueuingTo(target);
        assert ufc_nb.getQueuedMessagesTo(target) >= 5; // 5 1K messages plus metadata

        a.getProtocolStack().removeProtocol(DropCreditResponses.class); // now credits are retransmitted

        for(int i=0; i < 10; i++) {
            if(received_msgs.intValue() == 15)
                break;
            Util.sleep(1000);
        }
        assert received_msgs.intValue() == 15
          : String.format("B was expected to get 15 messages but only received %s", received_msgs.intValue());
    }


    public void testNonBlockingFlowControlMulticast() throws Exception {
        DropCreditResponses drop_credits=new DropCreditResponses();
        // Prevent credit replenishments from being received in A. At this point, A will block sending messages to B
        // after it runs out of credits

        a.getProtocolStack().insertProtocol(drop_credits, ProtocolStack.Position.ABOVE, SHARED_LOOPBACK.class);
        replaceMFC(60_000, a,b);

        final byte[] buf=new byte[1000];
        Address local=a.getAddress();

        for(int i=1; i <= 15; i++)
            da.callRemoteMethods(null, new MethodCall(RECEIVE, local, buf),
                                 RequestOptions.ASYNC().transientFlags(Message.TransientFlag.DONT_LOOPBACK));

        MFC_NB mfc_nb=a.getProtocolStack().findProtocol(MFC_NB.class);
        System.out.printf("A's sender credits: %s\n", mfc_nb.printCredits());
        assert mfc_nb.isQueuing();
        assert mfc_nb.getNumberOfQueuedMessages() >= 5; // 5 1K messages plus metadata

        a.getProtocolStack().removeProtocol(DropCreditResponses.class); // now credits are retransmitted
        for(int i=0; i < 10; i++) {
            if(received_msgs.intValue() >= 15)
                break;
            Util.sleep(1000);
        }
        assert received_msgs.intValue() == 15
          : String.format("B was expected to get 15 messages but only received %s", received_msgs.intValue());
    }



    /**
     * Same as {@link #testNonBlockingFlowControlUnicast()}, but now the max_queue_size in UFC_NB is very small, so that only
     * 1-2 messages will be queued and the next message blocks until credits have been received. These credits will
     * drain the message queue and thus sent the messages and unblock the blocked sender thread.
     */
    public void testNonBlockingFlowControlWithMessageQueueBlocking() throws Exception {
        DropCreditResponses drop_credits=new DropCreditResponses();
        // Prevent credit replenishments from being received in A. At this point, A will block sending messages to B
        // after it runs out of credits
        a.getProtocolStack().insertProtocol(drop_credits, ProtocolStack.Position.BELOW, UFC.class);
        replaceUFC(1500, a,b); // small max_queue_size

        final byte[] buf=new byte[1000];
        Address local=a.getAddress(), target=b.getAddress();

        Thread remover=new Thread(() -> {
            Util.sleep(2000);
            System.out.printf("-- removing %s\n", DropCreditResponses.class.getSimpleName());
            a.getProtocolStack().removeProtocol(DropCreditResponses.class);
        });
        remover.start();

        for(int i=1; i <= 15; i++)
            da.callRemoteMethod(target, new MethodCall(RECEIVE, local, buf), RequestOptions.ASYNC());

        UFC_NB ufc_nb=a.getProtocolStack().findProtocol(UFC_NB.class);
        System.out.printf("A's sender credits: %s\n", ufc_nb.printCredits());

        for(int i=0; i < 10; i++) {
            if(received_msgs.intValue() >= 15)
                break;
            Util.sleep(1000);
        }
        assert received_msgs.intValue() == 15
          : String.format("B was expected to get 15 messages but only received %s", received_msgs.intValue());
    }


    public void testNonBlockingFlowControlWithMessageQueueBlockingMulticast() throws Exception {
        DropCreditResponses drop_credits=new DropCreditResponses();
        // Prevent credit replenishments from being received in A. At this point, A will block sending messages to B
        // after it runs out of credits

        a.getProtocolStack().insertProtocol(drop_credits, ProtocolStack.Position.BELOW, MFC.class);
        replaceMFC(1500, a,b);

        final byte[] buf=new byte[1000];
        Address local=a.getAddress();

        Thread remover=new Thread(() -> {
            Util.sleep(2000);
            System.out.printf("-- removing %s\n", DropCreditResponses.class.getSimpleName());
            a.getProtocolStack().removeProtocol(DropCreditResponses.class);
        });
        remover.start();

        for(int i=1; i <= 15; i++)
            da.callRemoteMethods(null, new MethodCall(RECEIVE, local, buf),
                                 RequestOptions.ASYNC().transientFlags(Message.TransientFlag.DONT_LOOPBACK));

        MFC_NB mfc_nb=a.getProtocolStack().findProtocol(MFC_NB.class);
        System.out.printf("A's sender credits: %s\n", mfc_nb.printCredits());

        for(int i=0; i < 10; i++) {
            if(received_msgs.intValue() >= 15)
                break;
            Util.sleep(1000);
        }
        assert received_msgs.intValue() == 15
          : String.format("B was expected to get 15 messages but only received %s", received_msgs.intValue());
    }


    protected void invoke(RpcDispatcher disp, Address target, int num_bytes) throws Exception {
        // B invokes (blocking) A.forward
        disp.callRemoteMethod(a.getAddress(), new MethodCall(FORWARD, target, num_bytes), RequestOptions.SYNC().timeout(5000));
    }



    protected JChannel create(String name) throws Exception {
        return new JChannel(new SHARED_LOOPBACK(),
                            new SHARED_LOOPBACK_PING(),
                            new NAKACK2(),
                            new UNICAST3(),
                            new STABLE(),
                            new GMS(),
                            new UFC().setValue("max_credits", MAX_CREDITS).setValue("min_threshold", 0.2),
                            new MFC().setValue("max_credits", MAX_CREDITS).setValue("min_threshold", 0.2),
                            new FRAG2().fragSize(1500)).name(name);
    }

    protected void replaceUFC(int max_queue_size, JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            UFC_NB ufc_nb=(UFC_NB)new UFC_NB().setValue("max_credits", MAX_CREDITS).setValue("min_threshold", 0.2);
            ufc_nb.setMaxQueueSize(max_queue_size);
            ufc_nb.frag_size=1500;
            View view=ch.getView();
            ufc_nb.handleViewChange(view.getMembers()); // needs to setup received and sent hashmaps
            stack.replaceProtocol(stack.findProtocol(UFC.class), ufc_nb);
            ufc_nb.down(new Event(Event.SET_LOCAL_ADDRESS, ch.getAddress()));
            ufc_nb.start();
        }
    }


    protected void replaceMFC(int max_queue_size, JChannel ... channels) throws Exception {
        for(JChannel ch: channels) {
            ProtocolStack stack=ch.getProtocolStack();
            MFC_NB mfc_nb=(MFC_NB)new MFC_NB().setValue("max_credits", MAX_CREDITS).setValue("min_threshold", 0.2);
            mfc_nb.setMaxQueueSize(max_queue_size);
            mfc_nb.frag_size=1500;
            mfc_nb.init();
            stack.replaceProtocol(stack.findProtocol(MFC.class), mfc_nb);
            View view=ch.getView();
            mfc_nb.handleViewChange(view.getMembers()); // needs to setup received and sent hashmaps
            mfc_nb.down(new Event(Event.SET_LOCAL_ADDRESS, ch.getAddress()));
            mfc_nb.start();
        }
    }

    protected static class DropCreditResponses extends Protocol {
        public Object up(Message msg) {
            FcHeader hdr=getHeader(msg, UFC_ID, UFC_NB_ID, MFC_ID, MFC_NB_ID);
            if(hdr != null && hdr.type == FcHeader.REPLENISH) {
                System.out.println("-- dropping credits from " + msg.src());
                return null;
            }
            return up_prot.up(msg);
        }

        public void up(MessageBatch batch) {
            for(Message msg: batch) {
                FcHeader hdr=getHeader(msg, UFC_ID, UFC_NB_ID, MFC_ID, MFC_NB_ID);
                if(hdr != null && hdr.type == FcHeader.REPLENISH) {
                    System.out.println("-- dropping credits from " + batch.sender());
                    batch.remove(msg);
                }
            }
            if(!batch.isEmpty())
                up_prot.up(batch);
        }

        protected static FcHeader getHeader(Message msg, short... ids) {
            for(short id: ids) {
                FcHeader hdr=msg.getHeader(id);
                if(hdr != null)
                    return hdr;
            }
            return null;
        }
    }


}
