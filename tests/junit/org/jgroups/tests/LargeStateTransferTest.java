package org.jgroups.tests;


import org.jgroups.ChannelException;
import org.jgroups.ExtendedReceiverAdapter;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.View;
import org.jgroups.protocols.TP;
import org.jgroups.protocols.pbcast.GMS;
import org.jgroups.stack.ProtocolStack;
import org.jgroups.util.Promise;
import org.jgroups.util.Util;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.*;

/**
 * Tests transfer of large states (http://jira.jboss.com/jira/browse/JGRP-225).
 * Note that on Mac OS, FRAG2.frag_size and max_bundling_size in the transport should be less than 16'000 due to
 * http://jira.jboss.com/jira/browse/JGRP-560. As alternative, increase the MTU of the loopback device to a value
 * greater than max_bundle_size, e.g.
 * ifconfig lo0 mtu 65000
 * @author Bela Ban
 */
@Test(groups={Global.STACK_DEPENDENT}, sequential=true)
public class LargeStateTransferTest extends ChannelTestBase {
    JChannel          provider, requester;
    Promise<Integer>  p=new Promise<Integer>();
    final static int  SIZE_1=100000, SIZE_2=1000000, SIZE_3=5000000, SIZE_4=10000000;


    protected boolean useBlocking() {
        return true;
    }


    @BeforeMethod
    protected void setUp() throws Exception {
        provider=createChannel(true, 2);
        provider.setName("provider");
        modifyStack(provider);
        requester=createChannel(provider);
        requester.setName("requester");
        setOOBPoolSize(provider, requester);
    }

    @AfterMethod
    protected void tearDown() throws Exception {
        Util.close(requester, provider);
    }


    public void testStateTransfer1() throws ChannelException {
        _testStateTransfer(SIZE_1, "testStateTransfer1");
    }

    public void testStateTransfer2() throws ChannelException {
        _testStateTransfer(SIZE_2, "testStateTransfer2");
    }

    public void testStateTransfer3() throws ChannelException {
        _testStateTransfer(SIZE_3, "testStateTransfer3");
    }

    public void testStateTransfer4() throws ChannelException {
        _testStateTransfer(SIZE_4, "testStateTransfer4");
    }



    private void _testStateTransfer(int size, String suffix) throws ChannelException {
        final String GROUP="LargeStateTransferTest-" + suffix;
        provider.setReceiver(new Provider(size));
        provider.connect(GROUP);
        p.reset();
        requester.setReceiver(new Requester(p));
        requester.connect(GROUP);
        View requester_view=requester.getView();
        assert requester_view.size() == 2 : "requester view is " + requester_view + ", but should have 2 members";
        View provider_view=provider.getView();
        assert provider_view.size() == 2 : "provider view is " + provider_view + ", but should have 2 members";
        log("requesting state of " + Util.printBytes(size));
        long start=System.currentTimeMillis();
        requester.getState(provider.getAddress(), 20000);
        Integer result=p.getResult(20000);
        long stop=System.currentTimeMillis();
        assertNotNull(result);
        log("received " + Util.printBytes(result) + " (in " + (stop-start) + "ms)");
        assert result == size : "result=" + result + ", expected=" + size;
    }


    private static void setOOBPoolSize(JChannel... channels) {
        for(JChannel channel: channels) {
            TP transport=channel.getProtocolStack().getTransport();
            transport.setOOBThreadPoolMinThreads(1);
            transport.setOOBThreadPoolMaxThreads(2);
        }
    }


    static void log(String msg) {
        System.out.println(" -- "+ msg);
    }

    private static void modifyStack(JChannel ch) {
        ProtocolStack stack=ch.getProtocolStack();
        GMS gms=(GMS)stack.findProtocol(GMS.class);
        if(gms != null)
            gms.setLogCollectMessages(false);
    }


    private static class Provider extends ExtendedReceiverAdapter {
        private final byte[] state;

        public Provider(int size) {
            state=new byte[size];
        }

        public byte[] getState() {
            return state;
        }

        public void getState(OutputStream ostream){      
            DataOutputStream out =null;
            try{
               out=new DataOutputStream(ostream);
               out.writeInt(state.length);
               out.write(state, 0, state.length);
            }
            catch (IOException e){}
            finally{
               Util.close(out);
            }
        }
        public void setState(byte[] state) {
            throw new UnsupportedOperationException("not implemented by provider");
        }
    }


    private static class Requester extends ExtendedReceiverAdapter {
        private final Promise<Integer> promise;

        public Requester(Promise<Integer> p) {
            this.promise=p;
        }

        public byte[] getState() {
            throw new UnsupportedOperationException("not implemented by requester");
        }

        public void setState(byte[] state) {
            promise.setResult(new Integer(state.length));
        }
        public void setState(InputStream istream) {
            DataInputStream in=null;
            int size=0;
            try {
                in=new DataInputStream(istream);
                size=in.readInt();
                byte[] stateReceived=new byte[size];
                in.readFully(stateReceived, 0, stateReceived.length);
            }
            catch (IOException e) {}
            finally {
                Util.close(in);
            }
            promise.setResult(size);
        }
    }

}
