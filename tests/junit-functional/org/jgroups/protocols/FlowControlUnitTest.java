package org.jgroups.protocols;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.blocks.MethodCall;
import org.jgroups.blocks.RequestOptions;
import org.jgroups.blocks.RpcDispatcher;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.protocols.pbcast.NAKACK2;
import org.jgroups.protocols.pbcast.STABLE;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.lang.reflect.Method;

/**
 * Tests blocking in UFC / MFC (https://issues.jboss.org/browse/JGRP-1665)
 * @author Bela Ban
 * @since  3.4
 */
@Test(groups=Global.FUNCTIONAL,singleThreaded=true)
public class FlowControlUnitTest {
    protected JChannel      a, b;
    protected RpcDispatcher da, db;
    protected static final Method FORWARD, RECEIVE;
    protected static final int MAX_CREDITS=10000;

    static {
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
    }

    @AfterMethod protected void cleanup() {Util.close(b,a); db.stop(); da.stop();}

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
              .setTimeout(5000).setFlags(Message.Flag.OOB));
            System.out.println("retval=" + retval);
            int actual_bytes=(Integer)retval;
            assert actual_bytes == num_bytes : "expected " + Util.printBytes(num_bytes) + ", but call returned " + Util.printBytes(actual_bytes);
        }
        else {               // multicast
            RspList<Object> rsps=da.callRemoteMethods(null,new MethodCall(RECEIVE,a.getAddress(),buffer),RequestOptions.SYNC()
              .setTimeout(5000).setFlags(Message.Flag.OOB));
            System.out.println("rsps:\n" + rsps);
            assert rsps.size() == 2;
            for(Rsp rsp: rsps) {
                assert rsp.wasReceived() : " rsp from " + rsp.getSender() + " was not received";
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
        System.out.println("received " + Util.printBytes(buffer.length) + " from " + sender);
        return buffer.length;
    }

    public void testUnicastBlocking() throws Exception {
        invoke(db, b.getAddress(), (int)(MAX_CREDITS * 1.2)); // 20% above max_credits
    }

    public void testMulticastBlocking() throws Exception {
        invoke(db, null, (int)(MAX_CREDITS * 1.2)); // 20% above max_credits
    }

    protected void invoke(RpcDispatcher disp, Address target, int num_bytes) throws Exception {
        // B invokes (blocking) A.forward
        disp.callRemoteMethod(a.getAddress(), new MethodCall(FORWARD, target, num_bytes), RequestOptions.SYNC().setTimeout(5000));
    }



    protected JChannel create(String name) throws Exception {
        return new JChannel(new SHARED_LOOPBACK(),
                            new PING().timeout(1000),
                            new NAKACK2(),
                            new UNICAST3(),
                            new STABLE(),
                            new GMS(),
                            new UFC().setValue("max_credits", MAX_CREDITS).setValue("min_threshold", 0.2),
                            new MFC().setValue("max_credits", MAX_CREDITS).setValue("min_threshold", 0.2),
                            new FRAG2().fragSize(1500)).name(name);
    }



}
