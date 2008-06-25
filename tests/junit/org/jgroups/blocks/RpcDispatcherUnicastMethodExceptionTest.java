package org.jgroups.blocks;

import org.jgroups.Channel;
import org.jgroups.TimeoutException;
import org.jgroups.tests.ChannelTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 * @version $Id: RpcDispatcherUnicastMethodExceptionTest.java,v 1.10 2008/06/25 22:50:43 vlada Exp $
 */
@Test(groups="temp",sequential=true)
public class RpcDispatcherUnicastMethodExceptionTest extends ChannelTestBase {
    RpcDispatcher disp;
    Channel channel;

    @BeforeClass
    protected void setUp() throws Exception {
        channel=createChannel(true);
        disp=new RpcDispatcher(channel, null, null, this);
        channel.connect(getUniqueClusterName("RpcDispatcherUnicastMethodExceptionTest"));
    }

    @AfterClass
    protected void tearDown() throws Exception {
        disp.stop();
        channel.close();
    }

    @Test(enabled=false)
    public static Object foo() {
        return "foo(): OK";
    }

    @Test(enabled=false)
    public static Object bar() throws Exception {
        throw new TimeoutException("this is an exception");
    }

    @Test(enabled=false)
    public static Object foobar() {
        throw new IllegalArgumentException("bla bla bla from foobar");
    }

    @Test(enabled=false)
    public static Object foofoobar() {
        throw new AssertionError("bla bla bla from foofoobar");
    }

    @Test(enabled=false)
    public static void fooWithThrowable() throws Throwable {
        throw new Throwable("this is an exception");
    }


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

    @Test(expectedExceptions=Throwable.class)
    public void testMethodWithThrowable() throws Throwable {
        Object retval=disp.callRemoteMethod(channel.getLocalAddress(), "fooWithThrowable", null, (Class[])null, GroupRequest.GET_ALL, 5000);
        System.out.println("retval: " + retval);
    }



}
