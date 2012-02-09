package org.jgroups.blocks;

import org.jgroups.Channel;
import org.jgroups.Global;
import org.jgroups.TimeoutException;
import org.jgroups.tests.ChannelTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,sequential=true)
public class RpcDispatcherUnicastMethodExceptionTest extends ChannelTestBase {
    RpcDispatcher disp;
    Channel channel;

    @BeforeClass
    protected void setUp() throws Exception {
        channel=createChannel(true);
        disp=new RpcDispatcher(channel, this);
        channel.connect(getUniqueClusterName("RpcDispatcherUnicastMethodExceptionTest"));
    }

    @AfterClass
    protected void tearDown() throws Exception {
        disp.stop();
        channel.close();
    }

    static Object foo() {
        return "foo(): OK";
    }

    static Object bar() throws Exception {
        throw new TimeoutException("this is an exception");
    }

    static Object foobar() {
        throw new IllegalArgumentException("bla bla bla from foobar");
    }

    static Object foofoobar() {
        throw new AssertionError("bla bla bla from foofoobar");
    }

    static void fooWithThrowable() throws Throwable {
        throw new Throwable("this is an exception");
    }


    public void testMethodWithoutException() throws Exception {
        Object retval=disp.callRemoteMethod(channel.getAddress(), "foo", null, null,
                                            new RequestOptions(ResponseMode.GET_ALL, 5000));
        System.out.println("retval: " + retval);
        assertNotNull(retval);
    }


    @Test(expectedExceptions=TimeoutException.class)
    public void testMethodWithException() throws Exception {
        Object retval=disp.callRemoteMethod(channel.getAddress(), "bar", null, null,
                                            new RequestOptions(ResponseMode.GET_ALL, 5000));
        System.out.println("retval: " + retval);
    }

    @Test(expectedExceptions=IllegalArgumentException.class)
    public void testMethodWithException2() throws Exception {
        Object retval=disp.callRemoteMethod(channel.getAddress(), "foobar", null, null,
                                            new RequestOptions(ResponseMode.GET_ALL, 5000));
        System.out.println("retval: " + retval);
    }

    @Test(expectedExceptions=AssertionError.class)
    public void testMethodWithError() throws Exception {
        Object retval=disp.callRemoteMethod(channel.getAddress(), "foofoobar", null, null,
                                            new RequestOptions(ResponseMode.GET_ALL, 5000));
        System.out.println("retval: " + retval);
    }

    @Test(expectedExceptions=Throwable.class)
    public void testMethodWithThrowable() throws Exception {
        Object retval=disp.callRemoteMethod(channel.getAddress(), "fooWithThrowable", null, null,
                                            new RequestOptions(ResponseMode.GET_ALL, 5000));
        System.out.println("retval: " + retval);
    }



}
