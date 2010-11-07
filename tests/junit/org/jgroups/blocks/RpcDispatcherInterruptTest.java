package org.jgroups.blocks;


import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.Map;

/**
 * Tests interruption of a blocked call with the timeout and a thread pool
 * @author Bela Ban
 * @version $Id: RpcDispatcherInterruptTest.java,v 1.10 2008/08/08 17:07:30 vlada Exp $
 */
@Test(groups=Global.STACK_DEPENDENT)
public class RpcDispatcherInterruptTest extends ChannelTestBase {
    private RpcDispatcher disp, disp2;
    private JChannel ch, ch2;

    @BeforeMethod
    void setUp() throws Exception {
        ch=createChannel(true);
        modifyStack(ch);
        ServerObject obj=new ServerObject();
        disp=new RpcDispatcher(ch, null, null, obj);
        ch.connect("RpcDispatcherInterruptTest");

        ch2=createChannel(ch);
        ServerObject obj2=new ServerObject();
        disp2=new RpcDispatcher(ch2, null, null, obj2);
        ch2.connect("RpcDispatcherInterruptTest");
    }

    @AfterMethod
    void tearDown() throws Exception {
        ch2.close();
        disp2.stop();
        ch.close();
        disp.stop();
    }


    public void testMethodCallWithTimeoutNoInterrupt() {
        long timeout, block_time;
        RspList rsps;

        timeout=0;
        block_time=0;
        rsps=call(timeout, block_time);
        checkResults(rsps, 2, true);

        timeout=0;
        block_time=1000L;
        rsps=call(timeout, block_time);
        checkResults(rsps, 2, true);

        timeout=1000;
        block_time=0L;
        rsps=call(timeout, block_time);
        checkResults(rsps, 2, true);

        timeout=1000;
        block_time=10000L;
        rsps=call(timeout, block_time);
        checkResults(rsps, 2, false);
    }


    private static void modifyStack(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        GMS gms=(GMS)stack.findProtocol(GMS.class);
        if(gms != null)
            gms.setLogCollectMessages(false);
    }

    private RspList call(long timeout, long block_time) {
        long start, stop, diff;
        System.out.println("calling with timeout=" + timeout + ", block_time=" + block_time);
        start=System.currentTimeMillis();
        RspList retval=disp.callRemoteMethods(null, "foo", new Object[]{block_time}, new Class[]{long.class}, GroupRequest.GET_ALL, timeout);
        stop=System.currentTimeMillis();
        diff=stop-start;
        System.out.println("rsps (in " + diff + "ms:)\n" + retval);
        return retval;
    }

    private static void checkResults(RspList rsps, int num, boolean received) {
        assertEquals("responses: " + rsps, num, rsps.size());        
        for(Iterator<Map.Entry<Address,Rsp>> it=rsps.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Address,Rsp> entry=it.next();
            Rsp rsp=entry.getValue();
            assertEquals("rsp: " + rsp, rsp.wasReceived(), received);
        }
    }


    static class ServerObject {

        public static void foo(long timeout) {
            Util.sleep(timeout);
        }
    }
}