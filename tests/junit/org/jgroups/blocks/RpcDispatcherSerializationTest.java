package org.jgroups.blocks;

import org.jgroups.Address;
import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.JChannel;
import org.jgroups.tests.ChannelTestBase;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.*;
import java.util.Iterator;
import java.util.Vector;

@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class RpcDispatcherSerializationTest extends ChannelTestBase {
    private JChannel channel, channel2;
    private RpcDispatcher disp, disp2;
    private final Target target=new Target();



    @BeforeClass
    protected void setUp() throws Exception {
        channel=createChannel(true);
        disp=new RpcDispatcher(channel, null, null, target);
        channel.connect("RpcDispatcherSerializationTest");

        channel2=createChannel(channel);
        disp2=new RpcDispatcher(channel2, null, null, target);
        channel2.connect("RpcDispatcherSerializationTest");
    }


    @AfterClass
    protected void tearDown() throws Exception {
        channel2.close();
        disp2.stop();

        disp.stop();
        channel.close();
    }

    public void testNonSerializableArgument() throws Throwable {
        try {
            disp.callRemoteMethods(null, "foo", new Object[]{new NonSerializable()}, new Class[]{NonSerializable.class},
                                   GroupRequest.GET_ALL, 5000);
            throw new IllegalStateException("should throw NotSerializableException");
        }
        catch(Throwable t) {
            Throwable cause=t.getCause();
            if(cause != null && cause instanceof NotSerializableException) { // this needs to be changed once we change the signature
                System.out.println("received RuntimeException with NotSerializableException as cause - this is expected");
            }
            else
                throw t;
        }
    }

    public void testTargetMethodNotFound() {
        Vector<Address> members=channel.getView().getMembers();
        System.out.println("members are: " + members);
        RspList rsps=disp.callRemoteMethods(members, "foo", null, new Class[]{String.class, String.class},
                                            GroupRequest.GET_ALL, 8000);
        System.out.println("responses:\n" + rsps + ", channel.view: " + channel.getView() + ", channel2.view: " + channel2.getView());
        assert members.size() == rsps.size() : "expected " + members.size() + " responses, but got " + rsps + " (" + rsps.size() + ")";

        for(Rsp rsp: rsps.values()) {
            assert rsp.getValue() instanceof NoSuchMethodException : "response value is " + rsp.getValue();
        }
    }

    public void testMarshaller() {
        RpcDispatcher.Marshaller m=new MyMarshaller();
        disp.setRequestMarshaller(m);
        disp.setResponseMarshaller(m);
        disp2.setRequestMarshaller(m);
        disp2.setResponseMarshaller(m);

        RspList rsps;
        rsps=disp.callRemoteMethods(null, "methodA", new Object[]{Boolean.TRUE, new Long(322649)},
                                    new Class[]{boolean.class, long.class},
                                    GroupRequest.GET_ALL, 0);
        assert rsps.size() == 2;
        for(Iterator<Rsp> it=rsps.values().iterator(); it.hasNext();) {
            Rsp rsp=it.next();
            assert rsp.getValue() == null;
            assertTrue(rsp.wasReceived());
            assertFalse(rsp.wasSuspected());
        }

        rsps=disp.callRemoteMethods(null, "methodB", null, (Class[])null, GroupRequest.GET_ALL, 0);
        assertEquals(2, rsps.size());
        for(Iterator<Rsp> it=rsps.values().iterator(); it.hasNext();) {
            Rsp rsp=it.next();
            assertNotNull(rsp.getValue());
            assertEquals(Boolean.TRUE, rsp.getValue());
            assertTrue(rsp.wasReceived());
            assertFalse(rsp.wasSuspected());
        }


        rsps=disp.callRemoteMethods(null, "methodC", null, (Class[])null, GroupRequest.GET_ALL, 0);
        assertEquals(2, rsps.size());
        for(Iterator<Rsp> it=rsps.values().iterator(); it.hasNext();) {
            Rsp rsp=it.next();
            assertNotNull(rsp.getValue());
            assertTrue(rsp.getValue() instanceof Throwable);
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

        public byte[] objectToByteBuffer(Object obj) throws Exception {
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
                return out.toByteArray();
            }
            finally {
                Util.close(oos);
            }
        }

        public Object objectFromByteBuffer(byte[] buf) throws Exception {
            ByteArrayInputStream inp=new ByteArrayInputStream(buf);
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
