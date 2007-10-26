package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestSuite;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.stack.Protocol;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.util.Map;
import java.util.Properties;

/**
 * @author Bela Ban
 * @version $Id: RpcDispatcherTest.java,v 1.5 2007/10/26 07:47:31 belaban Exp $
 */
public class RpcDispatcherTest extends ChannelTestBase {
    RpcDispatcher disp1, disp2, disp3;
    JChannel c1, c2, c3;

    final static int[] SIZES={10000, 20000, 40000, 80000, 100000, 200000, 400000, 800000,
            1000000, 2000000, 5000000, 10000000, 20000000};

    protected void setUp() throws Exception {
        super.setUp();
        c1=createChannel("A");
        disp1=new RpcDispatcher(c1, null, null, new ServerObject(1));
        c1.connect("demo");
        c2=createChannel("A");
        disp2=new RpcDispatcher(c2, null, null, new ServerObject(2));
        c2.connect("demo");
        c3=createChannel("A");
        disp3=new RpcDispatcher(c3, null, null, new ServerObject(3));
        c3.connect("demo");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        disp3.stop();
        c3.close();
        disp2.stop();
        c2.close();
        disp1.stop();
        c1.close();
    }

    public void foo() {

    }


    public void testResponseFilter() {
        RspList rsps=disp1.callRemoteMethods(null, "foo", null, null,GroupRequest.GET_ALL, 10000, false,
                                             new RspFilter() {
                                                 int num=0;
                                                 public boolean isAcceptable(Object response, Address sender) {
                                                     boolean retval=((Integer)response).intValue() > 1;
                                                     System.out.println("-- received " + response + " from " +
                                                     sender + ": " + (retval ? "OK" : "NOTOK"));
                                                     if(retval)
                                                         num++;
                                                     return retval;
                                                 }

                                                 public boolean needMoreResponses() {
                                                     return num < 2;
                                                 }
                                             });
        System.out.println("responses are:\n" + rsps);
        assertEquals(3, rsps.size());
        assertEquals(2, rsps.numReceived());
    }


    public void testLargeReturnValue() {
        setProps(c1); setProps(c2); setProps(c3);
        for(int i=0; i < SIZES.length; i++) {
            _testLargeValue(SIZES[i]);
        }
    }

    public void testLargeReturnValueUnicastCall() throws Throwable {
        setProps(c1); setProps(c2); setProps(c3);
        for(int i=0; i < SIZES.length; i++) {
            _testLargeValueUnicastCall(c1.getLocalAddress(), SIZES[i]);
        }

        for(int i=0; i < SIZES.length; i++) {
            _testLargeValueUnicastCall(c3.getLocalAddress(), SIZES[i]);
        }


        for(int i=0; i < SIZES.length; i++) {
            _testLargeValueUnicastCall(c2.getLocalAddress(), SIZES[i]);
        }
    }


    private static void setProps(JChannel ch) {
        Properties props1=new Properties(), props2=new Properties();
        props1.setProperty("frag_size", "12000");
        props2.setProperty("max_bundle_size", "14000");
        Protocol prot=ch.getProtocolStack().findProtocol("FRAG2");
        if(prot == null)
            prot=ch.getProtocolStack().findProtocol("FRAG");
        if(prot != null)
            prot.setProperties(props1);
        prot=ch.getProtocolStack().getTransport();
        if(prot != null)
            prot.setProperties(props2);
    }

    void _testLargeValue(int size) {
        System.out.println("testing with " + size + " bytes");
        RspList rsps=disp1.callRemoteMethods(null, "largeReturnValue", new Object[]{size}, new Class[]{int.class}, GroupRequest.GET_ALL, 20000);
        System.out.println("rsps:\n" + rsps);
        assertEquals(3, rsps.size());
        for(Map.Entry<Address,Rsp> entry: rsps.entrySet()) {
            byte[] val=(byte[])entry.getValue().getValue();
            assertNotNull(val);
            assertEquals(size, val.length);
        }
    }

    void _testLargeValueUnicastCall(Address dst, int size) throws Throwable {
        System.out.println("testing unicast call with " + size + " bytes");
        assertNotNull(dst);
        Object retval=disp1.callRemoteMethod(dst, "largeReturnValue", new Object[]{size}, new Class[]{int.class}, GroupRequest.GET_ALL, 20000);
        System.out.println("rsp:\n" + retval);
        byte[] val=(byte[])retval;
        assertNotNull(val);
        assertEquals(size, val.length);
    }

    private static class ServerObject {
        int i;
        public ServerObject(int i) {
            this.i=i;
        }
        public int foo() {return i;}


        public static byte[] largeReturnValue(int size) {
            return new byte[size];
        }
    }


    public static Test suite() {
        return new TestSuite(RpcDispatcherTest.class);
    }


    public static void main(String[] args) {
        junit.textui.TestRunner.run(RpcDispatcherTest.suite());
    }
}