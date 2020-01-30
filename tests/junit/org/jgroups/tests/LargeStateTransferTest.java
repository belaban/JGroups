package org.jgroups.tests;


import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.Receiver;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.InputStream;
import java.io.OutputStream;

/**
 * Tests transfer of large states (http://jira.jboss.com/jira/browse/JGRP-225).
 * Note that on Mac OS, FRAG2.frag_size and max_bundling_size in the transport should be less than 16'000 due to
 * http://jira.jboss.com/jira/browse/JGRP-560. As alternative, increase the MTU of the loopback device to a value
 * greater than max_bundle_size, e.g.
 * ifconfig lo0 mtu 65000
 * @author Bela Ban
 */
@Test(groups={Global.STACK_DEPENDENT,Global.EAP_EXCLUDED}, singleThreaded=true)
public class LargeStateTransferTest extends ChannelTestBase {
    JChannel          provider, requester;
    Promise<Integer>  p=new Promise<>();
    final static int  SIZE_1=100000, SIZE_2=1000000, SIZE_3=5000000, SIZE_4=10000000;



    @BeforeMethod
    protected void setUp() throws Exception {
        provider=createChannel(true, 2, "provider");
        modifyStack(provider);
        requester=createChannel(provider, "requester");
        setThreadPoolSize(provider, requester);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        Util.close(requester, provider);
    }


    public void testStateTransfer1() throws Exception {
        _testStateTransfer(SIZE_1, "testStateTransfer1");
    }

    public void testStateTransfer2() throws Exception {
        _testStateTransfer(SIZE_2, "testStateTransfer2");
    }

    public void testStateTransfer3() throws Exception {
        _testStateTransfer(SIZE_3, "testStateTransfer3");
    }

    public void testStateTransfer4() throws Exception {
        _testStateTransfer(SIZE_4, "testStateTransfer4");
    }



    private void _testStateTransfer(int size, String suffix) throws Exception {
        final String GROUP="LargeStateTransferTest-" + suffix;
        provider.setReceiver(new Provider(size));
        provider.connect(GROUP);
        p.reset();
        requester.setReceiver(new Requester(p));
        requester.connect(GROUP);
        Util.waitUntilAllChannelsHaveSameView(20000, 1000, provider, requester);

        log("requesting state of " + Util.printBytes(size));
        long start=System.currentTimeMillis();
        requester.getState(provider.getAddress(), 20000);
        Integer result=p.getResult(20000);
        long stop=System.currentTimeMillis();
        assertNotNull(result);
        log("received " + Util.printBytes(result) + " (in " + (stop-start) + "ms)");
        assert result == size : "result=" + result + ", expected=" + size;
    }


    private static void setThreadPoolSize(JChannel... channels) {
        for(JChannel channel: channels) {
            TP transport=channel.getProtocolStack().getTransport();
            transport.setThreadPoolMinThreads(2);
            transport.setThreadPoolMaxThreads(8);
        }
    }


    static void log(String msg) {
        System.out.println(" -- "+ msg);
    }

    private static void modifyStack(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        GMS gms=stack.findProtocol(GMS.class);
        if(gms != null)
            gms.setLogCollectMessages(false);
    }


    private static class Provider implements Receiver {
        private final byte[] state;

        public Provider(int size) {
            state=new byte[size];
        }

        public void getState(OutputStream ostream) throws Exception {
            ostream.write(state, 0, state.length);
        }
    }


    private static class Requester implements Receiver {
        private final Promise<Integer> promise;

        public Requester(Promise<Integer> p) {
            this.promise=p;
        }


        public void setState(InputStream istream) throws Exception {
            int size=0;
            byte[] buf=new byte[50000];
            try {
                for(;;) {
                    int read=istream.read(buf, 0, buf.length);
                    if(read == -1)
                        break;
                    size+=read;
                }
            }
            finally {
                promise.setResult(size);
            }
        }
    }

}
