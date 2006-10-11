package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;
import org.jgroups.util.Util;

import java.io.*;
import java.util.Iterator;
import java.util.Vector;


public class RpcDispatcherSerializationTest extends TestCase {
    private JChannel channel, channel2;
    private RpcDispatcher disp, disp2;
    private String props=null;


    public RpcDispatcherSerializationTest(String testName) {
        super(testName);
    }


    public void methodA(boolean b, long l) {
        System.out.println("methodA(" + b + ", " + l + ") called");
    }


    public boolean methodB() {
        return true;
    }

    public void methodC() {
        throw new IllegalArgumentException("dummy exception - for testing only");
    }


    protected void setUp() throws Exception {
        super.setUp();
        channel=new JChannel(props);
        channel.setOpt(Channel.AUTO_RECONNECT, Boolean.TRUE);
        disp=new RpcDispatcher(channel, null, null, this);
        channel.connect("RpcDispatcherSerializationTestGroup");


        channel2=new JChannel(props);
        disp2=new RpcDispatcher(channel2, null, null, this);
        channel2.connect("RpcDispatcherSerializationTestGroup");
    }


    protected void tearDown() throws Exception {
        super.tearDown();
        channel2.close();
        disp2.stop();

        disp.stop();
        channel.close();
    }


    public void testNonSerializableArgument() {
        try {
            disp.callRemoteMethods(null, "foo", new Object[]{new NonSerializable()}, new Class[]{NonSerializable.class},
                                   GroupRequest.GET_ALL, 5000);
            fail("should throw NotSerializableException");
        }
        catch(Throwable t) {
            Throwable cause=t.getCause();
            if(cause != null && cause instanceof NotSerializableException) { // this needs to be changed once we change the signature
                System.out.println("received RuntimeException with NotSerializableException as cause - this is expected");
            }
            else
                fail("received " + t);
        }
    }

    public void testTargetMethodNotFound() {
        Vector members=channel.getView().getMembers();
        System.out.println("members are: " + members);
        RspList rsps=disp.callRemoteMethods(members, "foo", null, new Class[]{String.class, String.class},
                                            GroupRequest.GET_ALL, 8000);
        System.out.println("responses:\n" + rsps + ", channel.view: " + channel.getView() + ", channel2.view: " + channel2.getView());
        assertEquals(members.size(), rsps.size());
        for(int i=0; i < rsps.size(); i++) {
            Rsp rsp=(Rsp)rsps.elementAt(i);
            assertTrue(rsp.getValue() instanceof NoSuchMethodException);
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
        assertEquals(2, rsps.size());
        for(Iterator it=rsps.values().iterator(); it.hasNext();) {
            Rsp rsp=(Rsp)it.next();
            assertNull(rsp.getValue());
            assertTrue(rsp.wasReceived());
            assertFalse(rsp.wasSuspected());
        }

        rsps=disp.callRemoteMethods(null, "methodB", null, (Class[])null, GroupRequest.GET_ALL, 0);
        assertEquals(2, rsps.size());
        for(Iterator it=rsps.values().iterator(); it.hasNext();) {
            Rsp rsp=(Rsp)it.next();
            assertNotNull(rsp.getValue());
            assertEquals(Boolean.TRUE, rsp.getValue());
            assertTrue(rsp.wasReceived());
            assertFalse(rsp.wasSuspected());
        }


        rsps=disp.callRemoteMethods(null, "methodC", null, (Class[])null, GroupRequest.GET_ALL, 0);
        assertEquals(2, rsps.size());
        for(Iterator it=rsps.values().iterator(); it.hasNext();) {
            Rsp rsp=(Rsp)it.next();
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
                        return new Boolean(in.readBoolean());
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


    public static Test suite() {
        return new TestSuite(RpcDispatcherSerializationTest.class);
    }


    public static void main(String[] args) {
        junit.textui.TestRunner.run(RpcDispatcherSerializationTest.suite());
    }

    static class NonSerializable {
        int i;
    }

}
