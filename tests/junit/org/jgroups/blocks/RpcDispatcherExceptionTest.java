package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Channel;
import org.jgroups.tests.ChannelTestBase;

import java.io.NotSerializableException;

/**
 * @author Bela Ban
 * @version $Id: RpcDispatcherExceptionTest.java,v 1.4 2008/04/08 07:19:10 belaban Exp $
 */
public class RpcDispatcherExceptionTest extends ChannelTestBase {
    RpcDispatcher disp;
    Channel channel;

    public void setUp() throws Exception {
        ;
        channel=createChannel("A");
        disp=new RpcDispatcher(channel, null, null, this);
        channel.connect("demo");
    }

    public void tearDown() throws Exception {
        ;
        disp.stop();
        channel.close();
    }

    public void foo(Pojo p) {
        System.out.println(p.toString());
    }


    public void testUnserializableValue() {
        try {
            disp.callRemoteMethods(null, "foo", new Object[]{new Pojo()}, new Class[]{Pojo.class}, GroupRequest.GET_ALL, 5000);
            fail("this should have thrown an exception");
        }
        catch(Throwable t) {
            System.out.println("received an exception as expected: " + t);
            assertTrue(t instanceof RuntimeException);
            Throwable cause=t.getCause();
            assertTrue(cause instanceof NotSerializableException);
        }
    }

    public void testUnserializableValue2() {
        try {
            disp.callRemoteMethod(channel.getLocalAddress(), "foo", new Object[]{new Pojo()}, new Class[]{Pojo.class},
                                  GroupRequest.GET_ALL, 5000);
            fail("this should have thrown an exception");
        }
        catch(Throwable t) {
            System.out.println("received an exception as expected: " + t);
            assertTrue(t instanceof NotSerializableException);
        }
    }

    static class Pojo { // doesn't implement Serializable !
        int age; String name;
    }


    public static Test suite() {
        return new TestSuite(RpcDispatcherExceptionTest.class);
    }


    public static void main(String[] args) {
        junit.textui.TestRunner.run(RpcDispatcherExceptionTest.suite());
    }
}
