package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.Channel;
import org.jgroups.JChannel;
import org.jgroups.util.Rsp;
import org.jgroups.util.RspList;

import java.io.NotSerializableException;
import java.util.Vector;


public class RpcDispatcherSerializationTest extends TestCase {
    private JChannel channel, channel2;
    private RpcDispatcher disp, disp2;
    private String props=null;


    public RpcDispatcherSerializationTest(String testName) {
        super(testName);
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
        RspList rsps=disp.callRemoteMethods(members, "foo", new Object[]{"one", "two"}, new Class[]{String.class, String.class},
                                            GroupRequest.GET_ALL, 800000);
        System.out.println("responses:\n" + rsps + ", channel.view: " + channel.getView() + ", channel2.view: " + channel2.getView());
        assertEquals(members.size(), rsps.size());
        for(int i=0; i < rsps.size(); i++) {
            Rsp rsp=(Rsp)rsps.elementAt(i);
            assertTrue(rsp.getValue() instanceof NoSuchMethodException);
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
