package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.*;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.lang.reflect.Method;
import java.util.Iterator;
import java.util.List;

@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class RpcDispatcherSerializationTest extends ChannelTestBase {
    private JChannel channel, channel2;
    private RpcDispatcher disp, disp2;
    private final Target target=new Target();



    @BeforeClass
    protected void setUp() throws Exception {
        channel=createChannel(true);
        disp=new RpcDispatcher(channel, target);
        channel.connect("RpcDispatcherSerializationTest");

        channel2=createChannel(channel);
        disp2=new RpcDispatcher(channel2, target);
        channel2.connect("RpcDispatcherSerializationTest");
    }


    @AfterClass
    protected void tearDown() throws Exception {
        Util.close(disp2, channel2, disp, channel);
    }

    public void testNonSerializableArgument() throws Exception {
        try {
            disp.callRemoteMethods(null, "foo", new Object[]{new NonSerializable()}, new Class[]{NonSerializable.class},
                                   new RequestOptions(ResponseMode.GET_ALL, 5000));
            throw new IllegalStateException("should throw NotSerializableException");
        }
        catch(Exception t) {
            assert t instanceof NotSerializableException || t.getCause() instanceof NotSerializableException
              : "exception is not of expected type: " + t;
        }
    }

    public void testTargetMethodNotFound() throws Exception {
        List<Address> members=channel.getView().getMembers();
        System.out.println("members are: " + members);
        RspList<Object> rsps=disp.callRemoteMethods(members, "foo", null, new Class[]{String.class, String.class},
                                            new RequestOptions(ResponseMode.GET_ALL, 8000));
        System.out.println("responses:\n" + rsps + ", channel.view: " + channel.getView() + ", channel2.view: " + channel2.getView());
        assert members.size() == rsps.size() : "expected " + members.size() + " responses, but got " + rsps + " (" + rsps.size() + ")";
        for(Rsp<?> rsp: rsps.values())
            assert rsp.getException() instanceof NoSuchMethodException;
    }

    public void testMarshaller() throws Exception {
        RspList<?> rsps;
        MethodCall call=new MethodCall(Target.A, Boolean.TRUE, 322649L);
        rsps=disp.callRemoteMethods(null, call, new RequestOptions(ResponseMode.GET_ALL, 0));
        assert rsps.size() == 2;
        for(Iterator<? extends Rsp<?>> it=rsps.values().iterator(); it.hasNext();) {
            Rsp<?> rsp=it.next();
            assert rsp.getValue() == null;
            assertTrue(rsp.wasReceived());
            assertFalse(rsp.wasSuspected());
        }

        rsps=disp.callRemoteMethods(null, new MethodCall(Target.B), new RequestOptions(ResponseMode.GET_ALL, 0));
        assertEquals(2, rsps.size());
        for(Iterator<? extends Rsp<?>> it=rsps.values().iterator(); it.hasNext();) {
            Rsp<?> rsp=it.next();
            assertNotNull(rsp.getValue());
            assertEquals(Boolean.TRUE, rsp.getValue());
            assertTrue(rsp.wasReceived());
            assertFalse(rsp.wasSuspected());
        }

        rsps=disp.callRemoteMethods(null, new MethodCall(Target.C), new RequestOptions(ResponseMode.GET_ALL, 0));
        assertEquals(2, rsps.size());
        for(Iterator<? extends Rsp<?>> it=rsps.values().iterator(); it.hasNext();) {
            Rsp<?> rsp=it.next();
            assertNull(rsp.getValue());
            assertNotNull(rsp.getException());
            assertTrue(rsp.wasReceived());
            assertFalse(rsp.wasSuspected());
        }
    }


    protected static class Target {

        protected static final Method A, B, C;

        static {
            try {
                A=Target.class.getMethod("methodA", boolean.class, long.class);
                B=Target.class.getMethod("methodB");
                C=Target.class.getMethod("methodC");
            }
            catch(NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        }

        @SuppressWarnings("UnusedParameters")
        public static void methodA(boolean b, long l) {
            System.out.printf("b=%b, l=%d\n", b, l);
        }

        public static boolean methodB() {
            return true;
        }

        public static void methodC() {
            throw new IllegalArgumentException("dummy exception - for testing only");
        }
    }


    static class NonSerializable {
        @SuppressWarnings("unused") int i;
    }

}
