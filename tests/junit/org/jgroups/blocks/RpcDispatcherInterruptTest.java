package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.Channel;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.util.Iterator;
import java.util.Map;

/**
 * Tests interruption of a blocked call with the timeout and a thread pool
 * @author Bela Ban
 * @version $Id: RpcDispatcherInterruptTest.java,v 1.1 2007/06/18 08:42:16 belaban Exp $
 */
public class RpcDispatcherInterruptTest extends ChannelTestBase {
    RpcDispatcher disp, disp2;
    Channel ch, ch2;

    public void setUp() throws Exception {
        super.setUp();

        ch=createChannel("A");
        ServerObject obj=new ServerObject();
        disp=new RpcDispatcher(ch, null, null, obj);
        ch.connect("demo");

        ch2=createChannel("A");
        ServerObject obj2=new ServerObject();
        disp2=new RpcDispatcher(ch2, null, null, obj2);
        ch2.connect("demo");
    }

    public void tearDown() throws Exception {
        super.tearDown();
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

    private void checkResults(RspList rsps, int num, boolean received) {
        assertEquals("responses: " + rsps, num, rsps.size());
        Map.Entry entry;
        for(Iterator it=rsps.entrySet().iterator(); it.hasNext();) {
            entry=(Map.Entry)it.next();
            Rsp rsp=(Rsp)entry.getValue();
            assertEquals("rsp: " + rsp, rsp.wasReceived(), received);
        }
    }


    static class ServerObject {

        public void foo(long timeout) {
            System.out.println("-- received foo(), blocking for " + timeout + " ms");
            Util.sleep(timeout);
            System.out.println("-- returning");
        }
    }


    public static Test suite() {
        return new TestSuite(RpcDispatcherAnycastTest.class);
    }


    public static void main(String[] args) {
        junit.textui.TestRunner.run(RpcDispatcherAnycastTest.suite());
    }
}