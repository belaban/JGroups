package org.jgroups.blocks;

import junit.framework.Test;
import junit.framework.TestSuite;

import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.TimeoutException;
import org.jgroups.tests.ChannelTestBase;

/**
 * @author Bela Ban
 * @version $Id: RpcDispatcherUnicastMethodExceptionTest.java,v 1.5 2008/04/08 07:19:09 belaban Exp $
 */
public class RpcDispatcherUnicastMethodExceptionTest extends ChannelTestBase {
    RpcDispatcher disp;
    Channel channel;

    protected void setUp() throws Exception {
        ;
        channel=createChannel("A");
        disp=new RpcDispatcher(channel, null, null, this);
        channel.connect("demo");
    }

    protected void tearDown() throws Exception {
        ;
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

    public Object foofoobar() {
        System.out.println("-- foofoobar()");
        throw new AssertionError("bla bla bla from foofoobar");
    }

    public void fooWithThrowable() throws Throwable {
        System.out.println("-- fooWithThrowable()");
        throw new Throwable("this is an exception");
    }



    public void testMethodWithoutException() throws Throwable {
        Object retval=disp.callRemoteMethod(channel.getLocalAddress(), "foo", null, (Class[])null, GroupRequest.GET_ALL, 5000);
        System.out.println("retval: " + retval);
        assertNotNull(retval);
    }



    public void testMethodWithException() throws ChannelException {
        try {
            Object retval=disp.callRemoteMethod(channel.getLocalAddress(), "bar", null, (Class[])null, GroupRequest.GET_ALL, 5000);
            System.out.println("retval: " + retval);
            fail("we should not get here; bar() should throw an exception");
        }
        catch(Throwable e) {
            System.out.println("caught exception (" + e + ") - as expected");
            assertTrue(e instanceof TimeoutException);
        }
    }

    public void testMethodWithException2() throws ChannelException {
        try {
            Object retval=disp.callRemoteMethod(channel.getLocalAddress(), "foobar", null, (Class[])null, GroupRequest.GET_ALL, 5000);
            System.out.println("retval: " + retval);
            fail("we should not get here; foobar() should throw an exception");
        }
        catch(Throwable e) {
            System.out.println("caught exception (" + e + ") - as expected");
            assertTrue(e instanceof IllegalArgumentException);
        }
    }

    public void testMethodWithError() throws ChannelException {
        try {
            Object retval=disp.callRemoteMethod(channel.getLocalAddress(), "foofoobar", null, (Class[])null, GroupRequest.GET_ALL, 5000);
            System.out.println("retval: " + retval);
            fail("we should not get here; foofoobar() should throw an exception");
        }
        catch(Throwable e) {
            System.out.println("caught exception (" + e + ") - as expected");
            assertTrue(e instanceof AssertionError);
        }
    }

    public void testMethodWithThrowable() throws ChannelException {
        try {
            Object retval=disp.callRemoteMethod(channel.getLocalAddress(), "fooWithThrowable", null, (Class[])null, GroupRequest.GET_ALL, 5000);
            System.out.println("retval: " + retval);
            fail("we should not get here; foofoobar() should throw an exception");
        }
        catch(Throwable e) {
            System.out.println("caught exception (" + e + ") - as expected");
            assertTrue(e instanceof Throwable);
        }
    }


    public static Test suite() {
        return new TestSuite(RpcDispatcherUnicastMethodExceptionTest.class);
    }


    public static void main(String[] args) {
        junit.textui.TestRunner.run(RpcDispatcherUnicastMethodExceptionTest.suite());
    }
}
