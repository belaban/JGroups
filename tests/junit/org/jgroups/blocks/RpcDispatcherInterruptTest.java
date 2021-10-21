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
 */
@Test(groups=Global.STACK_DEPENDENT)
public class RpcDispatcherInterruptTest extends ChannelTestBase {
    private RpcDispatcher disp, disp2;
    private JChannel      a, b;

    @BeforeMethod
    void setUp() throws Exception {
        a=createChannel().name("A");
        modifyStack(a);
        ServerObject obj=new ServerObject();
        disp=new RpcDispatcher(a, obj);

        b=createChannel().name("B");
        ServerObject obj2=new ServerObject();
        disp2=new RpcDispatcher(b, obj2);
        makeUnique(a,b);

        a.connect("RpcDispatcherInterruptTest");
        b.connect("RpcDispatcherInterruptTest");
    }

    @AfterMethod
    void tearDown() throws Exception {
        Util.close(disp2, b, disp, a);
    }


    public void testMethodCallWithTimeoutNoInterrupt() throws Exception {
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
        GMS gms=stack.findProtocol(GMS.class);
        if(gms != null)
            gms.setLogCollectMessages(false);
    }

    private RspList call(long timeout, long block_time) throws Exception {
        long start, stop, diff;
        System.out.println("calling with timeout=" + timeout + ", block_time=" + block_time);
        start=System.currentTimeMillis();
        RspList retval=disp.callRemoteMethods(null, "foo", new Object[]{block_time}, new Class[]{long.class},
                                              new RequestOptions(ResponseMode.GET_ALL, timeout));
        stop=System.currentTimeMillis();
        diff=stop-start;
        System.out.println("rsps (in " + diff + "ms:)\n" + retval);
        return retval;
    }

    private static void checkResults(RspList<Void> rsps, int num, boolean received) {
        assert num == rsps.size();
        for(Iterator<Map.Entry<Address,Rsp<Void>>> it=rsps.entrySet().iterator(); it.hasNext();) {
            Map.Entry<Address,Rsp<Void>> entry=it.next();
            Rsp<Void> rsp=entry.getValue();
            assert rsp.wasReceived() == received;
        }
    }


    static class ServerObject {

        public static void foo(long timeout) {
            Util.sleep(timeout);
        }
    }
}