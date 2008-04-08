package org.jgroups.blocks;

import org.jgroups.Channel;
import org.jgroups.ChannelException;
import org.jgroups.TimeoutException;
import org.jgroups.tests.ChannelTestBase;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @version $Id: RpcDispatcherUnicastMethodExceptionTest.java,v 1.6 2008/04/08 07:44:55 belaban Exp $
 */
public class RpcDispatcherUnicastMethodExceptionTest extends ChannelTestBase {
    RpcDispatcher disp;
    Channel channel;

    @BeforeTest
    protected void setUp() throws Exception {
        channel=createChannel("A");
        disp=new RpcDispatcher(channel, null, null, this);
        channel.connect("demo");
    }

    @AfterTest
    protected void tearDown() throws Exception {
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


    @Test
    public void testMethodWithoutException() throws Throwable {
        Object retval=disp.callRemoteMethod(channel.getLocalAddress(), "foo", null, (Class[])null, GroupRequest.GET_ALL, 5000);
        System.out.println("retval: " + retval);
        assertNotNull(retval);
    }


    @Test(expectedExceptions=TimeoutException.class)
    public void testMethodWithException() throws Throwable {
        Object retval=disp.callRemoteMethod(channel.getLocalAddress(), "bar", null, (Class[])null, GroupRequest.GET_ALL, 5000);
        System.out.println("retval: " + retval);
    }

    @Test(expectedExceptions=IllegalArgumentException.class)
    public void testMethodWithException2() throws Throwable {
        Object retval=disp.callRemoteMethod(channel.getLocalAddress(), "foobar", null, (Class[])null, GroupRequest.GET_ALL, 5000);
        System.out.println("retval: " + retval);
    }

    @Test(expectedExceptions=AssertionError.class)
    public void testMethodWithError() throws Throwable {
        Object retval=disp.callRemoteMethod(channel.getLocalAddress(), "foofoobar", null, (Class[])null, GroupRequest.GET_ALL, 5000);
        System.out.println("retval: " + retval);
    }

    @Test(expectedExceptions=TimeoutException.class)
    public void testMethodWithThrowable() throws Throwable {
        Object retval=disp.callRemoteMethod(channel.getLocalAddress(), "fooWithThrowable", null, (Class[])null, GroupRequest.GET_ALL, 5000);
        System.out.println("retval: " + retval);
    }



}
