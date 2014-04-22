package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Buffer;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
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
        channel2.close();
        disp2.stop();

        disp.stop();
        channel.close();
    }

    public void testNonSerializableArgument() throws Exception {
        try {
            disp.callRemoteMethods(null, "foo", new Object[]{new NonSerializable()}, new Class[]{NonSerializable.class},
                                   new RequestOptions(ResponseMode.GET_ALL, 5000));
            throw new IllegalStateException("should throw NotSerializableException");
        }
        catch(Exception t) {
            assert t instanceof NotSerializableException : "exception is not of expected type: " + t;
        }
    }

    public void testTargetMethodNotFound() throws Exception {
        List<Address> members=channel.getView().getMembers();
        System.out.println("members are: " + members);
        RspList<Object> rsps=disp.callRemoteMethods(members, "foo", null, new Class[]{String.class, String.class},
                                            new RequestOptions(ResponseMode.GET_ALL, 8000));
        System.out.println("responses:\n" + rsps + ", channel.view: " + channel.getView() + ", channel2.view: " + channel2.getView());
        assert members.size() == rsps.size() : "expected " + members.size() + " responses, but got " + rsps + " (" + rsps.size() + ")";

        for(Rsp rsp: rsps.values()) {
            assert rsp.getException() instanceof InvocationTargetException;
            Throwable cause=rsp.getException().getCause();
            assert cause instanceof NoSuchMethodException;
        }
    }

    public void testMarshaller() throws Exception {
        RpcDispatcher.Marshaller m=new MyMarshaller();
        disp.setRequestMarshaller(m);
        disp.setResponseMarshaller(m);
        disp2.setRequestMarshaller(m);
        disp2.setResponseMarshaller(m);

        RspList rsps;
        rsps=disp.callRemoteMethods(null, "methodA", new Object[]{Boolean.TRUE, new Long(322649)},
                                    new Class[]{boolean.class, long.class},
                                    new RequestOptions(ResponseMode.GET_ALL, 0));
        assert rsps.size() == 2;
        for(Iterator<Rsp> it=rsps.values().iterator(); it.hasNext();) {
            Rsp rsp=it.next();
            assert rsp.getValue() == null;
            assertTrue(rsp.wasReceived());
            assertFalse(rsp.wasSuspected());
        }

        rsps=disp.callRemoteMethods(null, "methodB", null, null, new RequestOptions(ResponseMode.GET_ALL, 0));
        assertEquals(2, rsps.size());
        for(Iterator<Rsp> it=rsps.values().iterator(); it.hasNext();) {
            Rsp rsp=it.next();
            assertNotNull(rsp.getValue());
            assertEquals(Boolean.TRUE, rsp.getValue());
            assertTrue(rsp.wasReceived());
            assertFalse(rsp.wasSuspected());
        }


        rsps=disp.callRemoteMethods(null, "methodC", null, null, new RequestOptions(ResponseMode.GET_ALL, 0));
        assertEquals(2, rsps.size());
        for(Iterator<Rsp> it=rsps.values().iterator(); it.hasNext();) {
            Rsp rsp=it.next();
            assertNull(rsp.getValue());
            assertNotNull(rsp.getException());
            assertTrue(rsp.wasReceived());
            assertFalse(rsp.wasSuspected());
        }

        disp.setRequestMarshaller(null);
        disp.setResponseMarshaller(null);
        disp2.setRequestMarshaller(null);
        disp2.setResponseMarshaller(null);
    }



    static class MyMarshaller implements RpcDispatcher.Marshaller {
        static final byte NULL  = 0;
        static final byte BOOL  = 1;
        static final byte LONG  = 2;
        static final byte OBJ   = 3;

        public Buffer objectToBuffer(Object obj) throws Exception {
            ByteArrayOutputStream out=new ByteArrayOutputStream(24);
            ObjectOutputStream oos=new ObjectOutputStream(out);

            try {
                if(obj == null) {
                    oos.writeByte(NULL);
                }
                else if(obj instanceof Boolean) {
                    oos.writeByte(BOOL);
                    oos.writeBoolean(((Boolean)obj).booleanValue());
                }
                else if(obj instanceof Long) {
                    oos.writeByte(LONG);
                    oos.writeLong(((Long)obj).longValue());
                }
                else {
                    oos.writeByte(OBJ);
                    oos.writeObject(obj);
                }
                oos.flush();
                return new Buffer(out.toByteArray());
            }
            finally {
                Util.close(oos);
            }
        }

        public Object objectFromBuffer(byte[] buf, int offset, int length) throws Exception {
            ByteArrayInputStream inp=new ByteArrayInputStream(buf, offset, length);
            ObjectInputStream in=new ObjectInputStream(inp);

            try {
                int type=in.readByte();
                switch(type) {
                    case NULL:
                        return null;
                    case BOOL:
                        return Boolean.valueOf(in.readBoolean());
                    case LONG:
                        return new Long(in.readLong());
                    case OBJ:
                        return in.readObject();
                    default:
                        throw new IllegalArgumentException("incorrect type " + type);
                }
            }
            finally {
                Util.close(in);
            }
        }
    }

    static class Target {
        public static void methodA(boolean b, long l) {
            ;
        }

        public static boolean methodB() {
            return true;
        }

        public static void methodC() {
            throw new IllegalArgumentException("dummy exception - for testing only");
        }
    }


    static class NonSerializable {
        int i;
    }

}
