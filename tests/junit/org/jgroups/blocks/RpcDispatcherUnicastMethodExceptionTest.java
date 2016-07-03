package org.jgroups.blocks;

import org.jgroups.JChannel;
import org.jgroups.Global;
import org.jgroups.tests.ChannelTestBase;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.TimeoutException;

/**
 * @author Bela Ban
 */
@Test(groups=Global.STACK_DEPENDENT,singleThreaded=true)
public class RpcDispatcherUnicastMethodExceptionTest extends ChannelTestBase {
    RpcDispatcher disp;
    JChannel channel;

    @BeforeClass
    protected void setUp() throws Exception {
        channel=createChannel(true);
        disp=new RpcDispatcher(channel, this);
        channel.connect("RpcDispatcherUnicastMethodExceptionTest");
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

    static Throwable returnException() {return new IllegalArgumentException("this is actually right");}


    public void testMethodWithoutException() throws Exception {
        Object retval=disp.callRemoteMethod(channel.getAddress(), "foo", null, null,
                                            new RequestOptions(ResponseMode.GET_ALL, 5000));
        System.out.println("retval: " + retval);
        assertNotNull(retval);
    }


    public void testMethodReturningException() throws Exception {
        Object retval=disp.callRemoteMethod(channel.getAddress(),"returnException",null,null,RequestOptions.SYNC());
        System.out.println("retval: " + retval);
        assertNotNull(retval);
        assert retval instanceof IllegalArgumentException;
    }


    public void testMethodWithException() throws Exception {
        try {
            disp.wrapExceptions(true);
            disp.callRemoteMethod(channel.getAddress(), "bar", null, null, RequestOptions.SYNC());
            assert false: "method should have thrown an exception";
        }
        catch(Exception ex) {
            assert ex instanceof InvocationTargetException;
            Throwable cause=ex.getCause();
            assert cause instanceof TimeoutException;
        }
        finally {
            disp.wrapExceptions(false);
        }
    }

    @Test(expectedExceptions=TimeoutException.class)
    public void testMethodWithExceptionWithoutWrapping() throws Exception {
        disp.callRemoteMethod(channel.getAddress(), "bar", null, null, RequestOptions.SYNC());
    }

    public void testMethodWithException2() throws Exception {
        try {
            disp.wrapExceptions(true);
            disp.callRemoteMethod(channel.getAddress(), "foobar", null, null, RequestOptions.SYNC());
        }
        catch(Throwable t) {
            System.out.println("t = " + t);
            assert t instanceof InvocationTargetException;
            assert t.getCause() instanceof IllegalArgumentException;
        }
        finally {
            disp.wrapExceptions(false);
        }
    }

    @Test(expectedExceptions=IllegalArgumentException.class)
    public void testMethodWithException2WithoutWrapping() throws Exception {
        disp.callRemoteMethod(channel.getAddress(), "foobar", null, null, RequestOptions.SYNC());
    }

    public void testMethodWithError() throws Exception {
        try {
            disp.callRemoteMethod(channel.getAddress(),"foofoobar",null,null,RequestOptions.SYNC());
        }
        catch(Throwable t) {
            System.out.println("t = " + t);
            assert t instanceof AssertionError;
        }
    }

    @Test(expectedExceptions=AssertionError.class)
    public void testMethodWithErrorWithoutWrapping() throws Exception {
        disp.callRemoteMethod(channel.getAddress(), "foofoobar", null, null, RequestOptions.SYNC());
    }

    public void testMethodWithThrowable() throws Exception {
        try {
            disp.wrapExceptions(true);
            disp.callRemoteMethod(channel.getAddress(),"fooWithThrowable",null,null,RequestOptions.SYNC());
        }
        catch(Throwable t) {
            System.out.println("t = " + t);
            assert t instanceof InvocationTargetException;
            assert t.getCause() instanceof Throwable;
        }
        finally {
            disp.wrapExceptions(false);
        }
    }


    @Test(expectedExceptions=Throwable.class)
    public void testMethodWithThrowableWithoutWrapping() throws Exception {
        disp.callRemoteMethod(channel.getAddress(), "fooWithThrowable", null, null, RequestOptions.SYNC());
    }
}
