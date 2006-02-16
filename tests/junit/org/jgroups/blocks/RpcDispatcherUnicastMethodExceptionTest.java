package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.jgroups.ChannelException;
import org.jgroups.JChannel;
import org.jgroups.TimeoutException;

/**
 * @author Bela Ban
 * @version $Id: RpcDispatcherUnicastMethodExceptionTest.java,v 1.1 2006/02/16 06:28:45 belaban Exp $
 */
public class RpcDispatcherUnicastMethodExceptionTest extends TestCase {
    RpcDispatcher disp;
    JChannel channel;

    protected void setUp() throws Exception {
        super.setUp();
        channel=new JChannel();
        disp=new RpcDispatcher(channel, null, null, this);
        channel.connect("demo");
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        disp.stop();
        channel.close();
    }


    public Object foo() {
        System.out.println("-- foo()");
        return "foo(): OK";
    }

    public Object bar() throws Exception {
        System.out.println("-- bar()");
        throw new TimeoutException("this is an exception");
    }

    public Object foobar() {
        System.out.println("-- foobar()");
        throw new IllegalArgumentException("bla bla bla from foobar");
    }


    public void testMethodWithoutException() throws Exception {
        Object retval=disp.callRemoteMethod(channel.getLocalAddress(), "foo", null, (Class[])null, GroupRequest.GET_ALL, 5000);
        assertNotNull(retval);
    }




    public void testMethodWithException() throws ChannelException {
        try {
            disp.callRemoteMethod(channel.getLocalAddress(), "bar", null, (Class[])null, GroupRequest.GET_ALL, 5000);
            fail("we should not get here; bar() should throw an exception");
        }
        catch(Exception e) {
            assertTrue(e instanceof TimeoutException);
        }
    }

    public void testMethods2() throws ChannelException {
        try {
            disp.callRemoteMethod(channel.getLocalAddress(), "foobar", null, (Class[])null, GroupRequest.GET_ALL, 5000);
            fail("we should not get here; foobar() should throw an exception");
        }
        catch(Exception e) {
            assertTrue(e instanceof IllegalArgumentException);
        }
    }


    public static Test suite() {
        return new TestSuite(RpcDispatcherUnicastMethodExceptionTest.class);
    }


    public static void main(String[] args) {
        junit.textui.TestRunner.run(RpcDispatcherUnicastMethodExceptionTest.suite());
    }
}
